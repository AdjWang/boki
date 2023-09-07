#include "log/engine.h"

#include "engine/engine.h"
#include "log/flags.h"
#include "utils/bits.h"
#include "utils/random.h"

namespace faas {
namespace log {

using protocol::Message;
using protocol::MessageHelper;
using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;
using protocol::SharedLogResultType;

Engine::Engine(engine::Engine* engine)
    : EngineBase(engine),
      log_header_(fmt::format("LogEngine[{}-N]: ", my_node_id())),
      current_view_(nullptr),
      current_view_active_(false) {
    // Setup cache
    if (absl::GetFlag(FLAGS_slog_engine_enable_cache)) {
        log_cache_enabled_ = true;
        log_cache_.reset(new ShardedLRUCache(absl::GetFlag(FLAGS_slog_engine_cache_cap_mb)));
    } else {
        log_cache_enabled_ = false;
    }
}

Engine::~Engine() {}

void Engine::OnViewCreated(const View* view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "New view {} created", view->id());
    bool contains_myself = view->contains_engine_node(my_node_id());
    if (!contains_myself) {
        HLOG_F(WARNING, "View {} does not include myself", view->id());
    }
    std::vector<SharedLogRequest> ready_requests;
    {
        absl::MutexLock view_lk(&view_mu_);
        if (contains_myself) {
            const View::Engine* engine_node = view->GetEngineNode(my_node_id());
            for (uint16_t sequencer_id : view->GetSequencerNodes()) {
                if (!view->is_active_phylog(sequencer_id)) {
                    continue;
                }
                producer_collection_.InstallLogSpace(std::make_unique<LogProducer>(
                    my_node_id(), view, sequencer_id));
                if (engine_node->HasIndexFor(sequencer_id)) {
                    index_collection_.InstallLogSpace(std::make_unique<Index>(
                        view, sequencer_id));
                }
            }
        }
        future_requests_.OnNewView(view, contains_myself ? &ready_requests : nullptr);
        current_view_ = view;
        if (contains_myself) {
            current_view_active_ = true;
        }
        views_.push_back(view);
        log_header_ = fmt::format("LogEngine[{}-{}]: ", my_node_id(), view->id());
    }
    if (!ready_requests.empty()) {
        HLOG_F(INFO, "{} requests for the new view", ready_requests.size());
        SomeIOWorker()->ScheduleFunction(
            nullptr, [this, requests = std::move(ready_requests)] () {
                ProcessRequests(requests);
            }
        );
    }
}

void Engine::OnViewFrozen(const View* view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "View {} frozen", view->id());
    absl::MutexLock view_lk(&view_mu_);
    DCHECK_EQ(view->id(), current_view_->id());
    if (view->contains_engine_node(my_node_id())) {
        DCHECK(current_view_active_);
        current_view_active_ = false;
    }
}

void Engine::OnViewFinalized(const FinalizedView* finalized_view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "View {} finalized", finalized_view->view()->id());
    LogProducer::AppendResultVec append_results;
    Index::QueryResultVec query_results;
    {
        absl::MutexLock view_lk(&view_mu_);
        DCHECK_EQ(finalized_view->view()->id(), current_view_->id());
        producer_collection_.ForEachActiveLogSpace(
            finalized_view->view(),
            [finalized_view, &append_results] (uint32_t logspace_id,
                                               LockablePtr<LogProducer> producer_ptr) {
                log_utils::FinalizedLogSpace<LogProducer>(
                    producer_ptr, finalized_view);
                auto locked_producer = producer_ptr.Lock();
                LogProducer::AppendResultVec tmp;
                locked_producer->PollAppendResults(&tmp);
                append_results.insert(append_results.end(), tmp.begin(), tmp.end());
            }
        );
        index_collection_.ForEachActiveLogSpace(
            finalized_view->view(),
            [finalized_view, &query_results] (uint32_t logspace_id,
                                              LockablePtr<Index> index_ptr) {
                log_utils::FinalizedLogSpace<Index>(
                    index_ptr, finalized_view);
                auto locked_index = index_ptr.Lock();
                locked_index->PollQueryResults(&query_results);
            }
        );
    }
    if (!append_results.empty()) {
        SomeIOWorker()->ScheduleFunction(
            nullptr, [this, results = std::move(append_results)] {
                ProcessAppendResults(results);
            }
        );
    }
    if (!query_results.empty()) {
        SomeIOWorker()->ScheduleFunction(
            nullptr, [this, results = std::move(query_results)] {
                ProcessIndexQueryResults(results);
            }
        );
    }
}

namespace {
static Message BuildLocalReadOKResponseWithData(uint64_t response_id,
                                                uint64_t localid, uint64_t seqnum,
                                                std::span<const uint64_t> user_tags,
                                                std::span<const char> log_data,
                                                uint16_t flags = protocol::kLogResponseEOFDataFlag) {
    Message response = MessageHelper::NewSharedLogOpSucceeded(
        SharedLogResultType::READ_OK, localid, seqnum, flags);
    response.response_id = response_id;
    if (user_tags.size() * sizeof(uint64_t) + log_data.size() > MESSAGE_INLINE_DATA_SIZE) {
        LOG_F(FATAL, "Log data too large: num_tags={}, size={}",
              user_tags.size(), log_data.size());
    }
    response.log_num_tags = gsl::narrow_cast<uint16_t>(user_tags.size());
    MessageHelper::AppendInlineData(&response, user_tags);
    MessageHelper::AppendInlineData(&response, log_data);
    return response;
}

static Message BuildLocalReadOKResponseWithData(uint64_t response_id,
                                                const LogEntry& log_entry,
                                                uint16_t flags = protocol::kLogResponseEOFDataFlag) {
    return BuildLocalReadOKResponseWithData(
        response_id,
        log_entry.metadata.localid,
        log_entry.metadata.seqnum,
        VECTOR_AS_SPAN(log_entry.user_tags),
        STRING_AS_SPAN(log_entry.data),
        flags);
}

static Message BuildLocalReadOKResponseWithoutData(uint64_t response_id) {
    Message response = MessageHelper::NewSharedLogOpWithoutData(SharedLogResultType::READ_OK);
    response.response_id = response_id;
    SET_LOG_RESP_FLAG(response.flags, protocol::kLogResponseEOFFlag);
    return response;
}

static Message BuildLocalAsyncReadOKResponseWithIndex(uint64_t response_id, uint64_t localid, uint64_t seqnum) {
    Message response = MessageHelper::NewSharedLogOpSucceeded(
        SharedLogResultType::ASYNC_READ_OK, localid, seqnum);
    response.response_id = response_id;
    SET_LOG_RESP_FLAG(response.flags, protocol::kLogResponseContinueFlag);
    return response;
}

static Message BuildLocalAsyncReadOKResponseWithData(uint64_t response_id,
                                                     uint64_t localid, uint64_t seqnum,
                                                     std::span<const uint64_t> user_tags,
                                                     std::span<const char> log_data) {
    Message response = BuildLocalAsyncReadOKResponseWithIndex(response_id, localid, seqnum);
    if (user_tags.size() * sizeof(uint64_t) + log_data.size() > MESSAGE_INLINE_DATA_SIZE) {
        LOG_F(FATAL, "Log data too large: num_tags={}, size={}",
              user_tags.size(), log_data.size());
    }
    // client would not wait for the second response if log is cached
    SET_LOG_RESP_FLAG(response.flags, protocol::kLogResponseEOFDataFlag);
    response.log_num_tags = gsl::narrow_cast<uint16_t>(user_tags.size());
    MessageHelper::AppendInlineData(&response, user_tags);
    MessageHelper::AppendInlineData(&response, log_data);
    return response;
}

static Message BuildLocalAsyncReadCachedOKResponseWithData(uint64_t response_id, const LogEntry& log_entry) {
    return BuildLocalAsyncReadOKResponseWithData(
        response_id,
        log_entry.metadata.localid,
        log_entry.metadata.seqnum,
        VECTOR_AS_SPAN(log_entry.user_tags),
        STRING_AS_SPAN(log_entry.data));
}
}  // namespace

// Start handlers for local requests (from functions)

#define ONHOLD_IF_SEEN_FUTURE_VIEW(LOCAL_OP_VAR)                          \
    do {                                                                  \
        uint16_t view_id = log_utils::GetViewId(                          \
            (LOCAL_OP_VAR)->metalog_progress);                            \
        if (current_view_ == nullptr || view_id > current_view_->id()) {  \
            future_requests_.OnHoldRequest(                               \
                view_id, SharedLogRequest(LOCAL_OP_VAR));                 \
            return;                                                       \
        }                                                                 \
    } while (0)

void Engine::HandleLocalAppend(LocalOp* op) {
    DCHECK(protocol::SharedLogOpTypeHelper::IsFuncAppend(op->type));
    HVLOG_F(1, "Handle local append: op_id={}, logspace={}, num_tags={}, size={}",
            op->id, op->user_logspace, op->user_tags.size(), op->data.length());
    // DEBUG
    // HVLOG_F(1, "Handle local append: op_id={}, logspace={}, tags={}, size={}",
    //         op->id, op->user_logspace, log_utils::TagsToString(op->user_tags), op->data.length());
    const View* view = nullptr;
    LogMetaData log_metadata = MetaDataFromAppendOp(op);
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        if (!current_view_active_) {
            HLOG(WARNING) << "Current view not active";
            Message response;
            if (op->type == protocol::SharedLogOpType::APPEND) {
                response = MessageHelper::NewSharedLogOpWithoutData(SharedLogResultType::DISCARDED);
            } else if (op->type == protocol::SharedLogOpType::ASYNC_APPEND) {
                response = MessageHelper::NewSharedLogOpWithoutData(SharedLogResultType::ASYNC_DISCARDED);
            } else {
                UNREACHABLE();
            }
            response.response_id = 0;
            SendLocalOpWithResponse(op, &response, /*megalog_progress*/0);
            return;
        }
        view = current_view_;
        uint32_t logspace_id = view->LogSpaceIdentifier(op->user_logspace);
        log_metadata.seqnum = bits::JoinTwo32(logspace_id, 0);
        auto producer_ptr = producer_collection_.GetLogSpaceChecked(logspace_id);
        {
            auto locked_producer = producer_ptr.Lock();
            locked_producer->LocalAppend(op, &log_metadata.localid);
        }
    }
    if (op->type == SharedLogOpType::ASYNC_APPEND) {
        // here's the first return of an async append, the second is identical to
        // sync append
        Message response = MessageHelper::NewSharedLogOpSucceeded(
            SharedLogResultType::ASYNC_APPEND_OK, log_metadata.localid);
        SET_LOG_RESP_FLAG(response.flags, protocol::kLogResponseContinueFlag);
        // Not update metalog_progress here because the generation of local op id
        // not affects the metalog_progress.
        response.response_id = 0;
        SendLocalOpWithResponse(op, &response, /*metalog_progress*/0);
    }

    ReplicateLogEntry(view, log_metadata, VECTOR_AS_SPAN(op->user_tags), op->data.to_span());
}

void Engine::HandleLocalTrim(LocalOp* op) {
    DCHECK(op->type == SharedLogOpType::TRIM);
    NOT_IMPLEMENTED();
}

void Engine::HandleLocalRead(LocalOp* op) {
    DCHECK(protocol::SharedLogOpTypeHelper::IsFuncRead(op->type));
    if ((op->flags & LocalOp::kReadLocalIdFlag) == 0) {
        HVLOG_F(1, "HandleLocalRead: op_type={} op_id={} logspace={} tag={} from={:016X} seqnum={:016X} flags={:04X}",
                op->type, op->id, op->user_logspace, op->query_tag, op->query_start_seqnum, op->seqnum, op->flags);
    } else {
        HVLOG_F(1, "HandleLocalRead: op_type={} op_id={} logspace={} tag={} from={:016X} localid={:016X} flags={:04X}",
                op->type, op->id, op->user_logspace, op->query_tag, op->query_start_seqnum, op->localid, op->flags);
    }
    const View::Sequencer* sequencer_node = nullptr;
    LockablePtr<Index> index_ptr;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_SEEN_FUTURE_VIEW(op);
        uint32_t logspace_id = current_view_->LogSpaceIdentifier(op->user_logspace);
        sequencer_node = current_view_->GetSequencerNode(bits::LowHalf32(logspace_id));
        if (sequencer_node->IsIndexEngineNode(my_node_id())) {
            index_ptr = index_collection_.GetLogSpaceChecked(logspace_id);
        }
    }
    ongoing_local_reads_.PutChecked(op->id, op);

    bool use_local_index = true;
    if (absl::GetFlag(FLAGS_slog_engine_force_remote_index)) {
        use_local_index = false;
    }
    if (absl::GetFlag(FLAGS_slog_engine_prob_remote_index) > 0.0f) {
        float coin = utils::GetRandomFloat(0.0f, 1.0f);
        if (coin < absl::GetFlag(FLAGS_slog_engine_prob_remote_index)) {
            use_local_index = false;
        }
    }
    if (index_ptr != nullptr && use_local_index) {
        // Use local index
        IndexQuery query = BuildIndexQuery(op);
        Index::QueryResultVec query_results;
        {
            auto locked_index = index_ptr.Lock();
            locked_index->MakeQuery(query);
            locked_index->PollQueryResults(&query_results);
        }
        ProcessIndexQueryResults(query_results);
    } else {
        HVLOG_F(1, "There is no local index for sequencer {}, "
                   "will send request to remote engine node",
                DCHECK_NOTNULL(sequencer_node)->node_id());
        SharedLogMessage request = BuildReadRequestMessage(op);
        bool send_success = SendIndexReadRequest(DCHECK_NOTNULL(sequencer_node), &request);
        if (!send_success) {
            ongoing_local_reads_.RemoveChecked(op->id);
            Message response = MessageHelper::NewSharedLogOpWithoutData(SharedLogResultType::DATA_LOST);
            response.response_id = 0;
            SendLocalOpWithResponse(op, &response, /*metalog_progress*/0);
        }
    }
}

void Engine::HandleLocalSetAuxData(LocalOp* op) {
    uint64_t seqnum = op->seqnum;
    uint64_t localid = op->localid;
    uint64_t tag = op->query_tag;
    HVLOG_F(1, "HandleLocalSetAuxData op_id={} seqnum={:016X} localid={:016X} tag={}",
                op->id, seqnum, localid, tag);
    LogCachePutAuxData(seqnum, localid, tag, op->data.to_span());
    // localid and seqnum are useless here, set any value
    Message response = MessageHelper::NewSharedLogOpSucceeded(
        SharedLogResultType::AUXDATA_OK, localid, seqnum);
    SET_LOG_RESP_FLAG(response.flags, protocol::kLogResponseEOFDataFlag);
    response.response_id = 0;
    SendLocalOpWithResponse(op, &response, /*metalog_progress*/0);
    if (!absl::GetFlag(FLAGS_slog_engine_propagate_auxdata)) {
        return;
    }
    if (auto log_entry = LogCacheGet(seqnum); log_entry.has_value()) {
        if (auto aux_entry = LogCacheGetAuxData(seqnum); aux_entry.has_value()) {
            DCHECK_EQ(log_entry->metadata.seqnum, aux_entry->metadata.seqnum);
            if (aux_entry->metadata.localid != protocol::kInvalidLogLocalId) {
                DCHECK_EQ(log_entry->metadata.localid, aux_entry->metadata.localid);
            }
            uint16_t view_id = log_utils::GetViewId(seqnum);
            absl::ReaderMutexLock view_lk(&view_mu_);
            if (view_id < views_.size()) {
                const View* view = views_.at(view_id);
                std::string aux_entry_data = log_utils::EncodeAuxEntry(aux_entry.value());
                PropagateAuxData(view, log_entry->metadata, STRING_AS_SPAN(aux_entry_data));
            }
        }
    }
}

#undef ONHOLD_IF_SEEN_FUTURE_VIEW

// Start handlers for remote messages

#define ONHOLD_IF_FROM_FUTURE_VIEW(MESSAGE_VAR, PAYLOAD_VAR)        \
    do {                                                            \
        if (current_view_ == nullptr                                \
                || (MESSAGE_VAR).view_id > current_view_->id()) {   \
            future_requests_.OnHoldRequest(                         \
                (MESSAGE_VAR).view_id,                              \
                SharedLogRequest(MESSAGE_VAR, PAYLOAD_VAR));        \
            return;                                                 \
        }                                                           \
    } while (0)

#define IGNORE_IF_FROM_PAST_VIEW(MESSAGE_VAR)                       \
    do {                                                            \
        if (current_view_ != nullptr                                \
                && (MESSAGE_VAR).view_id < current_view_->id()) {   \
            HLOG(WARNING) << "Receive outdate request from view "   \
                          << (MESSAGE_VAR).view_id;                 \
            return;                                                 \
        }                                                           \
    } while (0)

void Engine::HandleRemoteRead(const SharedLogMessage& request) {
    SharedLogOpType op_type = SharedLogMessageHelper::GetOpType(request);
    DCHECK(protocol::SharedLogOpTypeHelper::IsFuncRead(op_type));
    LockablePtr<Index> index_ptr;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(request, EMPTY_CHAR_SPAN);
        index_ptr = index_collection_.GetLogSpaceChecked(request.logspace_id);
    }
    IndexQuery query = BuildIndexQuery(request);
    Index::QueryResultVec query_results;
    {
        auto locked_index = index_ptr.Lock();
        locked_index->MakeQuery(query);
        locked_index->PollQueryResults(&query_results);
    }
    ProcessIndexQueryResults(query_results);
}

void Engine::OnRecvNewMetaLogs(const SharedLogMessage& message,
                               std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::METALOGS);
    MetaLogsProto metalogs_proto = log_utils::MetaLogsFromPayload(payload);
    DCHECK_EQ(metalogs_proto.logspace_id(), message.logspace_id);
    LogProducer::AppendResultVec append_results;
    Index::QueryResultVec query_results;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        IGNORE_IF_FROM_PAST_VIEW(message);
        auto producer_ptr = producer_collection_.GetLogSpaceChecked(message.logspace_id);
        {
            auto locked_producer = producer_ptr.Lock();
            for (const MetaLogProto& metalog_proto : metalogs_proto.metalogs()) {
                locked_producer->ProvideMetaLog(metalog_proto);
            }
            locked_producer->PollAppendResults(&append_results);
        }
        if (current_view_->GetEngineNode(my_node_id())->HasIndexFor(message.sequencer_id)) {
            auto index_ptr = index_collection_.GetLogSpaceChecked(message.logspace_id);
            {
                auto locked_index = index_ptr.Lock();
                for (const MetaLogProto& metalog_proto : metalogs_proto.metalogs()) {
                    locked_index->ProvideMetaLog(metalog_proto);
                }
                locked_index->PollQueryResults(&query_results);
            }
        }
    }
    ProcessAppendResults(append_results);
    ProcessIndexQueryResults(query_results);
}

void Engine::OnRecvNewIndexData(const SharedLogMessage& message,
                                std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::INDEX_DATA);
    IndexDataProto index_data_proto;
    if (!index_data_proto.ParseFromArray(payload.data(),
                                         static_cast<int>(payload.size()))) {
        LOG(FATAL) << "Failed to parse IndexDataProto";
    }
    Index::QueryResultVec query_results;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        DCHECK(message.view_id < views_.size());
        const View* view = views_.at(message.view_id);
        if (!view->contains_engine_node(my_node_id())) {
            HLOG_F(FATAL, "View {} does not contain myself", view->id());
        }
        const View::Engine* engine_node = view->GetEngineNode(my_node_id());
        if (!engine_node->HasIndexFor(message.sequencer_id)) {
            HLOG_F(FATAL, "This node is not index node for log space {}",
                   bits::HexStr0x(message.logspace_id));
        }
        auto index_ptr = index_collection_.GetLogSpaceChecked(message.logspace_id);
        {
            auto locked_index = index_ptr.Lock();
            locked_index->ProvideIndexData(index_data_proto);
            locked_index->PollQueryResults(&query_results);
        }
    }
    ProcessIndexQueryResults(query_results);
}

#undef ONHOLD_IF_FROM_FUTURE_VIEW
#undef IGNORE_IF_FROM_PAST_VIEW

static std::string GetAuxDataToReturn(const log::AuxEntry& aux_entry,
                                      std::optional<UserTagVec> query_tag_set=std::nullopt) {
    json aux_data = aux_entry.data;
    if (query_tag_set.has_value()) {
        aux_data = log_utils::GetByTags(aux_entry, query_tag_set.value());
    }
    if (aux_data.empty()) {
        return "";
    }
    const std::string& cached_aux_data(aux_data.dump());
    std::string compressed_aux_data;
    snappy::Compress(cached_aux_data.data(), cached_aux_data.size(), &compressed_aux_data);
    return compressed_aux_data;
    // return cached_aux_data;
}

void Engine::OnRecvResponse(const SharedLogMessage& message,
                            std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::RESPONSE);
    SharedLogResultType result = SharedLogMessageHelper::GetResultType(message);
    uint64_t seqnum = bits::JoinTwo32(message.logspace_id, message.seqnum_lowhalf);
    uint64_t localid = message.localid;
    HVLOG_F(1, "Receive remote read response result={} seqnum={:016X} localid={:016X} payload_size={}",
                result, seqnum, localid, message.payload_size);

    auto ResponseAndCache = [this, result, localid, seqnum](
                                LocalOp* op, const SharedLogMessage& message,
                                std::span<const char> payload) {
        Message response;
        if (message.payload_size > 0) {
            std::span<const uint64_t> user_tags;
            std::span<const char> log_data;
            std::span<const char> aux_entry_data;
            // decode log entry
            log_utils::SplitPayloadForMessage(message, payload, &user_tags, &log_data, &aux_entry_data);
            // decode aux entry
            AuxEntry aux_entry;
            if (aux_entry_data.size() > 0) {
                uint64_t aux_seqnum = log_utils::DecodeAuxEntry(aux_entry_data, localid, &aux_entry);
                DCHECK_EQ(aux_seqnum, seqnum);
            }
            // make response message
            if (result == SharedLogResultType::ASYNC_READ_OK) {
                response = BuildLocalAsyncReadOKResponseWithData(/*response_id*/ 0, localid, seqnum, user_tags, log_data);
            } else if (result == SharedLogResultType::READ_OK) {
                if (op->type == protocol::SharedLogOpType::READ_SYNCTO) {
                    uint64_t response_id = op->response_counter.GetBufferedRequestId(seqnum);
                    response = BuildLocalReadOKResponseWithData(response_id, localid, seqnum, user_tags, log_data);
                } else {
                    if (protocol::SharedLogOpTypeHelper::IsAsyncSharedLogOp(op->type)) {
                        response = BuildLocalReadOKResponseWithData(/*response_id*/ 1, localid, seqnum, user_tags, log_data);
                    } else {
                        response = BuildLocalReadOKResponseWithData(/*response_id*/ 0, localid, seqnum, user_tags, log_data);
                    }
                }
            } else {
                UNREACHABLE();
            }
            if (!aux_entry.data.empty()) {
                const std::string& cached_aux_data = GetAuxDataToReturn(aux_entry, op->user_tags);
                size_t full_size = log_data.size()
                                    + user_tags.size() * sizeof(uint64_t)
                                    + cached_aux_data.size();
                if (full_size <= MESSAGE_INLINE_DATA_SIZE) {
                    auto aux_data = STRING_AS_SPAN(cached_aux_data);
                    response.log_aux_data_size = gsl::narrow_cast<uint16_t>(cached_aux_data.size());
                    MessageHelper::AppendInlineData(&response, aux_data);
                } else {
                    HLOG_F(WARNING, "Inline buffer of message not large enough "
                                    "for auxiliary data of log (seqnum {}): "
                                    "log_size={} num_tags={} cached_aux_data_size={}",
                            bits::HexStr0x(seqnum), log_data.size(),
                            user_tags.size(), cached_aux_data.size());
                    if (op->type == protocol::SharedLogOpType::READ_SYNCTO) {
                        HLOG(FATAL) << "Unable to return aux_data for syncto";
                    }
                }
            }
            SendLocalOpWithResponse(
                op, &response, message.user_metalog_progress,
                [this, op] /*on_finished*/ () {
                    ongoing_local_reads_.RemoveChecked(op->id);
                });
            // Put the received log entry into log cache
            LogMetaData log_metadata = log_utils::GetMetaDataFromMessage(message);
            LogCachePut(log_metadata, user_tags, log_data);
            if (aux_entry_data.size() > 0) {
                // would merge to local rather than overwrite to null
                LogCachePutAuxData(aux_entry);
            }
        } else {
            // EOF
            DCHECK(op->type == protocol::SharedLogOpType::READ_SYNCTO);
            uint64_t response_id = op->response_counter.GetBufferedRequestId(seqnum);
            Message response = BuildLocalReadOKResponseWithoutData(response_id);
            SendLocalOpWithResponse(
                op, &response, message.user_metalog_progress,
                [this, op] /*on_finished*/ () {
                    ongoing_local_reads_.RemoveChecked(op->id);
                });
        }
    };

    uint64_t op_id = message.client_data;
    LocalOp* op;
    if (!ongoing_local_reads_.Peek(op_id, &op)) {
        HLOG_F(WARNING, "Cannot find read op with id {}", op_id);
        return;
    }
    if (result == SharedLogResultType::ASYNC_READ_OK || 
        result == SharedLogResultType::ASYNC_EMPTY) {
        if (result == SharedLogResultType::ASYNC_READ_OK) {
            // remote index response
            if (message.payload_size == 0) {
                Message response = BuildLocalAsyncReadOKResponseWithIndex(/*response_id*/ 0, localid, seqnum);
                SendLocalOpWithResponse(op, &response, message.user_metalog_progress);
            } else {
                ResponseAndCache(op, message, payload);
            }
        } else if (result == SharedLogResultType::ASYNC_EMPTY) {
            Message response = MessageHelper::NewSharedLogOpWithoutData(result);
            response.response_id = 0;
            SendLocalOpWithResponse(
                op, &response, message.user_metalog_progress,
                [this, op] /*on_finished*/ () {
                    ongoing_local_reads_.RemoveChecked(op->id);
                });
        } else {
            UNREACHABLE();
        }
    } else if (    result == SharedLogResultType::READ_OK
                || result == SharedLogResultType::EMPTY
                || result == SharedLogResultType::DATA_LOST) {
        if (result == SharedLogResultType::READ_OK) {
            ResponseAndCache(op, message, payload);
        } else if (result == SharedLogResultType::EMPTY || 
                   result == SharedLogResultType::DATA_LOST) {
            if (result == SharedLogResultType::DATA_LOST) {
                HLOG_F(WARNING, "Receive DATA_LOST response for read request: seqnum={}, tag={}",
                    bits::HexStr0x(op->seqnum), op->query_tag);
            }
            uint64_t response_id;
            if (op->type == protocol::SharedLogOpType::READ_SYNCTO) {
                response_id = op->response_counter.GetBufferedRequestId(seqnum);
            } else {
                if (protocol::SharedLogOpTypeHelper::IsAsyncSharedLogOp(op->type)) {
                    response_id = 1;
                } else {
                    response_id = 0;
                }
            }
            Message response = MessageHelper::NewSharedLogOpWithoutData(result);
            response.response_id = response_id;
            SendLocalOpWithResponse(
                op, &response, message.user_metalog_progress,
                [this, op] /*on_finished*/ () {
                    ongoing_local_reads_.RemoveChecked(op->id);
                });
        } else {
            UNREACHABLE();
        }
    } else {
        HLOG(FATAL) << "Unknown result type: " << message.op_result;
    }
}

void Engine::ProcessAppendResults(const LogProducer::AppendResultVec& results) {
    for (const LogProducer::AppendResult& result : results) {
        LocalOp* op = reinterpret_cast<LocalOp*>(result.caller_data);
        uint64_t response_id;
        if (op->type == protocol::SharedLogOpType::ASYNC_APPEND) {
            response_id = 1;
        } else if (op->type == protocol::SharedLogOpType::APPEND) {
            response_id = 0;
        } else {
            UNREACHABLE();
        }
        if (result.seqnum != kInvalidLogSeqNum) {
            LogMetaData log_metadata = MetaDataFromAppendOp(op);
            log_metadata.seqnum = result.seqnum;
            log_metadata.localid = result.localid;
            LogCachePut(log_metadata, VECTOR_AS_SPAN(op->user_tags), op->data.to_span());
            Message response = MessageHelper::NewSharedLogOpSucceeded(
                SharedLogResultType::APPEND_OK, result.localid, result.seqnum);
            SET_LOG_RESP_FLAG(response.flags, protocol::kLogResponseEOFDataFlag);
            response.response_id = response_id;
            SendLocalOpWithResponse(op, &response, result.metalog_progress);
        } else {
            HVLOG_F(1, "Discard append result due to seqnum={:016X} invalid", result.seqnum);
            Message response = MessageHelper::NewSharedLogOpWithoutData(protocol::SharedLogResultType::DISCARDED);
            response.response_id = response_id;
            SendLocalOpWithResponse(op, &response, /*metalog_progress*/0);
        }
    }
}

void Engine::ProcessIndexFoundResult(const IndexQueryResult& query_result) {
    DCHECK(query_result.state == IndexQueryResult::kFound ||
           query_result.state == IndexQueryResult::kFoundRange);
    const IndexQuery& query = query_result.original_query;
    bool local_request = (query.origin_node_id == my_node_id());
    uint64_t seqnum = query_result.found_result.seqnum;
    uint64_t localid = query_result.found_result.localid;
    // aux data
    std::optional<AuxEntry> cached_aux_entry = std::nullopt;
    if (query_result.state == IndexQueryResult::kFoundRange) {
        cached_aux_entry = query_result.promised_auxdata;
        if (cached_aux_entry.has_value()) {
            DCHECK_EQ(cached_aux_entry->metadata.seqnum, seqnum);
            DCHECK_EQ(cached_aux_entry->metadata.localid, localid);
        }
    } else {
        if (query.promised_auxdata.has_value() && seqnum == query.promised_auxdata->metadata.seqnum) {
            cached_aux_entry = query.promised_auxdata;
        } else {
            cached_aux_entry = LogCacheGetAuxData(seqnum);
        }
    }
    // async read response when found && sync read response
    // For async requests:
    // if the log is cached: return ONCE as an async response with kLogDataCachedFlag set.
    // if the log is not cached: return twice as usual.
    if (auto cached_log_entry = LogCacheGet(seqnum); cached_log_entry.has_value()) {
        // Cache hits
        HVLOG_F(1, "Cache hits for log entry (seqnum={:016X})", seqnum);
        const LogEntry& log_entry = cached_log_entry.value();
        DCHECK_EQ(log_entry.metadata.seqnum, seqnum);
        if (localid != protocol::kInvalidLogLocalId) {
            DCHECK_EQ(log_entry.metadata.localid, localid);
        }
        if (local_request) {
            Message response;
            if (query.type == IndexQuery::kAsync) {
                HVLOG_F(1, "Send local async read cached response for log (seqnum={:016X})", seqnum);
                response = BuildLocalAsyncReadCachedOKResponseWithData(query_result.id, log_entry);
            } else {
                HVLOG_F(1, "Send local read response for log (seqnum={:016X})", seqnum);
                response = BuildLocalReadOKResponseWithData(query_result.id, log_entry);
            }
            LocalOp* op = ongoing_local_reads_.PeekChecked(query.client_data);
            std::string cached_aux_data;
            std::span<const char> aux_data;
            if (cached_aux_entry.has_value()) {
                cached_aux_data = GetAuxDataToReturn(cached_aux_entry.value(), op->user_tags);
                size_t full_size = log_entry.data.size()
                                    + log_entry.user_tags.size() * sizeof(uint64_t)
                                    + cached_aux_data.size();
                if (full_size <= MESSAGE_INLINE_DATA_SIZE) {
                    aux_data = STRING_AS_SPAN(cached_aux_data);
                } else {
                    HLOG_F(WARNING, "Inline buffer of message not large enough "
                                    "for auxiliary data of log (seqnum={:016X}): "
                                    "log_size={} num_tags={} cached_aux_data_size={}",
                            seqnum, log_entry.data.size(),
                            log_entry.user_tags.size(), cached_aux_data.size());
                    if (op->type == protocol::SharedLogOpType::READ_SYNCTO) {
                        HLOG(FATAL) << "Unable to return aux_data for syncto";
                    }
                }
            }
            response.log_aux_data_size = gsl::narrow_cast<uint16_t>(aux_data.size());
            MessageHelper::AppendInlineData(&response, aux_data);
            SendLocalOpWithResponse(
                op, &response, query_result.metalog_progress,
                [this, query] /*on_finished*/ () {
                    ongoing_local_reads_.RemoveChecked(query.client_data);
                });
        } else {
            SharedLogMessage response;
            if (query.type == IndexQuery::kAsync) {
                HVLOG_F(1, "Send remote async read cached response for log (seqnum={:016X})", seqnum);
                // shared log message not using kLogDataCachedFlag now, use payload_size to
                // check if the log is cached in OnRecvResponse(...)
                response = SharedLogMessageHelper::NewAsyncReadOkResponse();
            } else {
                HVLOG_F(1, "Send remote read response for log (seqnum={:016X})", seqnum);
                response = SharedLogMessageHelper::NewReadOkResponse();
            }
            log_utils::PopulateMetaDataToMessage(log_entry.metadata, &response);
            response.user_metalog_progress = query_result.metalog_progress;
            std::string cached_aux_data;
            std::span<const char> aux_data;
            if (cached_aux_entry.has_value()) {
                cached_aux_data = GetAuxDataToReturn(cached_aux_entry.value());
                aux_data = STRING_AS_SPAN(cached_aux_data);
            }
            response.aux_data_size = gsl::narrow_cast<uint16_t>(aux_data.size());
            SendReadResponse(query, &response,
                             VECTOR_AS_CHAR_SPAN(log_entry.user_tags),
                             STRING_AS_SPAN(log_entry.data), aux_data);
        }
    } else {
        // Cache miss
        HVLOG_F(1, "Cache miss for log entry (seqnum={:016X})", seqnum);
        // async read first response
        if (query.type == IndexQuery::kAsync) {
            if(local_request) {
                HVLOG_F(1, "Send local async read response for log (seqnum={:016X})", seqnum);
                LocalOp* op = ongoing_local_reads_.PeekChecked(query.client_data);
                Message response = BuildLocalAsyncReadOKResponseWithIndex(/*response_id*/ 0, localid, seqnum);
                SendLocalOpWithResponse(op, &response, query_result.metalog_progress);
            } else {
                HVLOG_F(1, "Send remote async read response for log (seqnum={:016X})", seqnum);
                SharedLogMessage response = SharedLogMessageHelper::NewAsyncReadOkResponse();
                response.logspace_id = bits::HighHalf64(seqnum);
                response.seqnum_lowhalf = bits::LowHalf64(seqnum);
                response.user_metalog_progress = query_result.metalog_progress;
                response.localid = localid;
                SendReadResponse(query, &response);
            }
        }
        const View::Engine* engine_node = nullptr;
        {
            absl::ReaderMutexLock view_lk(&view_mu_);
            uint16_t view_id = query_result.found_result.view_id;
            if (view_id < views_.size()) {
                const View* view = views_.at(view_id);
                engine_node = view->GetEngineNode(query_result.found_result.engine_id);
            } else {
                HLOG_F(FATAL, "Cannot find view {}", view_id);
            }
        }
        LocalOp* op = ongoing_local_reads_.PeekChecked(query.client_data);
        if (op->type == protocol::SharedLogOpType::READ_SYNCTO) {
            op->response_counter.BufferRequestId(seqnum, query_result.id);
        }
        // aux_data on storage node maybe stale, so protagate aux_data and
        // loopback to read response
        std::string aux_entry_data;
        std::span<const char> aux_data;
        if (cached_aux_entry.has_value()) {
            aux_entry_data = log_utils::EncodeAuxEntry(cached_aux_entry.value());
            aux_data = STRING_AS_SPAN(aux_entry_data);
        }
        bool success = SendStorageReadRequest(query_result, engine_node, aux_data);
        if (!success) {
            HLOG_F(WARNING, "Failed to send read request for seqnum={:016X} ", seqnum);
            if (local_request) {
                Message response = MessageHelper::NewSharedLogOpWithoutData(SharedLogResultType::DATA_LOST);
                if (protocol::SharedLogOpTypeHelper::IsAsyncSharedLogOp(op->type)) {
                    response.response_id = 1;
                } else {
                    response.response_id = query_result.id;
                }
                SendLocalOpWithResponse(
                    op, &response, query_result.metalog_progress,
                    [this, query] /*on_finished*/ () {
                        ongoing_local_reads_.RemoveChecked(query.client_data);
                    });
            } else {
                SendReadResponseWithoutData(query, SharedLogResultType::DATA_LOST);
            }
        }
    }
}

void Engine::ProcessIndexContinueResult(const IndexQueryResult& query_result,
                                        Index::QueryResultVec* more_results) {
    DCHECK(query_result.state == IndexQueryResult::kViewContinue ||
           query_result.state == IndexQueryResult::kAuxContinue ||
           query_result.state == IndexQueryResult::kFoundRangeContinue);
    HVLOG_F(1, "Process IndexContinueResult: next_view_id={}",
            query_result.next_view_id);
    const IndexQuery& query = query_result.original_query;
    const View::Sequencer* sequencer_node = nullptr;
    LockablePtr<Index> index_ptr;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        uint16_t view_id = query_result.next_view_id;
        if (view_id >= views_.size()) {
            HLOG_F(FATAL, "Cannot find view {}", view_id);
        }
        const View* view = views_.at(view_id);
        uint32_t logspace_id = view->LogSpaceIdentifier(query.user_logspace);
        sequencer_node = view->GetSequencerNode(bits::LowHalf32(logspace_id));
        if (sequencer_node->IsIndexEngineNode(my_node_id())) {
            index_ptr = index_collection_.GetLogSpaceChecked(logspace_id);
        }
    }
    if (index_ptr != nullptr) {
        HVLOG(1) << "Use local index";
        IndexQuery query = BuildIndexQuery(query_result);
        auto locked_index = index_ptr.Lock();
        locked_index->MakeQuery(query);
        locked_index->PollQueryResults(more_results);
    } else {
        HVLOG(1) << "Send to remote index";
        // drop readed query.promised_auxdata here, use remote node's aux data
        SharedLogMessage request = BuildReadRequestMessage(query_result);
        // FIXME: handle query request id for remote index read
        // LocalOp* op = ongoing_local_reads_.PeekChecked(query.client_data);
        // if (op->type == protocol::SharedLogOpType::READ_SYNCTO) {
        //     op->response_counter.BufferRequestId(result., query_result.id);
        // }
        bool send_success = SendIndexReadRequest(DCHECK_NOTNULL(sequencer_node), &request);
        if (!send_success) {
            uint32_t logspace_id = bits::JoinTwo16(sequencer_node->view()->id(),
                                                   sequencer_node->node_id());
            HLOG_F(ERROR, "Failed to send read index request for logspace {}",
                   bits::HexStr0x(logspace_id));
        }
    }
}

IndexQueryResult Engine::UpdateQueryResultWithAux(const IndexQueryResult& result) {
    DCHECK(result.state == IndexQueryResult::kFoundRange);

    uint64_t start_seqnum = result.found_result.seqnum;
    uint64_t end_seqnum = result.found_result.end_seqnum;
    uint64_t future_localid = result.found_result.future_localid;
    HVLOG_F(1, "UpdateQueryResultWithAux start_seqnum={:016X} localid={:016X} end_seqnum={:016X} future_localid={:016X}",
                start_seqnum, result.found_result.localid, end_seqnum, future_localid);

    DCHECK_NE(start_seqnum, 0u);
    DCHECK_NE(start_seqnum, protocol::kInvalidLogSeqNum);
    DCHECK_NE(end_seqnum, 0u);
    DCHECK_NE(end_seqnum, protocol::kInvalidLogSeqNum);

    if (future_localid == protocol::kInvalidLogLocalId) {
        if (start_seqnum >= end_seqnum) {
            IndexQueryResult new_result = result;
            new_result.state = IndexQueryResult::kEOF;
            HVLOG_F(1, "UpdateQueryResultWithAux produce kEOF id={}", new_result.id);
            return new_result;
        } else if ((start_seqnum + 1) == end_seqnum) {
            IndexQueryResult new_result = result;
            new_result.promised_auxdata = LogCacheGetAuxData(start_seqnum);
            HVLOG_F(1, "UpdateQueryResultWithAux produce kFoundRange id={}", new_result.id);
            return new_result;
        }
    }
    // make new updated result
    IndexQueryResult new_result = result;
    const IndexQuery& query = result.original_query;
    std::optional<AuxEntry> aux_entry = std::nullopt;
    if (query.initial && ((query.flags & IndexQuery::kReadFromCachedFlag) != 0)) {
        // produce new result by aux position
        aux_entry = LogCacheGetAuxDataPrev(query.user_tag, end_seqnum - 1);
    } else {
        aux_entry = LogCacheGetAuxData(new_result.found_result.seqnum);
    }
    if (aux_entry.has_value() && aux_entry->metadata.seqnum >= start_seqnum) {
        new_result.promised_auxdata = aux_entry;
        // TODO: new_result.end_seqnum and new_result.future_localid are inherited
        // from result, but what about view_id and engine_id?
        // They are not used until now but may become diaster in the future.
        new_result.found_result.seqnum = aux_entry->metadata.seqnum;
        new_result.found_result.localid = aux_entry->metadata.localid;
        HVLOG_F(1, "UpdateQueryResultWithAux adjust seqnum={:016X}->{:016X} localid={:016X}->{:016X}",
                    result.found_result.seqnum, new_result.found_result.seqnum,
                    result.found_result.localid, new_result.found_result.localid);
    } else {
        new_result.promised_auxdata = std::nullopt;
    }
    // DEBUG
    {
        uint64_t start_seqnum = new_result.found_result.seqnum;
        uint64_t localid = new_result.found_result.localid;
        uint64_t end_seqnum = new_result.found_result.end_seqnum;
        uint64_t future_localid = new_result.found_result.future_localid;
        HVLOG_F(1, "UpdateQueryResultWithAux produce kFoundRange id={} seqnum={:016X} localid={:016X} "
                    "end_seqnum={:016X} future_localid={:016X}",
                    new_result.id, start_seqnum, localid, end_seqnum, future_localid);
    }
    return new_result;
}

void Engine::ProcessIndexQueryResults(const Index::QueryResultVec& results) {
    Index::QueryResultVec query_results = results;
    while (!query_results.empty()) {
        // avoid recursive on the stack
        query_results = DoProcessIndexQueryResults(query_results);
    }
}
Index::QueryResultVec Engine::DoProcessIndexQueryResults(const Index::QueryResultVec& results) {
    Index::QueryResultVec more_results;
    for (const IndexQueryResult& original_result : results) {
        IndexQueryResult result = original_result;
        if (original_result.state == IndexQueryResult::kFoundRange) {
            result = UpdateQueryResultWithAux(original_result);
        }
        const IndexQuery& query = result.original_query;
        switch (result.state) {
        case IndexQueryResult::kFound:
        case IndexQueryResult::kFoundRange:
            ProcessIndexFoundResult(result);
            break;
        case IndexQueryResult::kEmpty:
            if (query.origin_node_id == my_node_id()) {
                LocalOp* op = ongoing_local_reads_.PeekChecked(query.client_data);
                Message response;
                if (query.type == IndexQuery::kSync) {
                    response = MessageHelper::NewSharedLogOpWithoutData(SharedLogResultType::EMPTY);
                } else if (query.type == IndexQuery::kAsync) {
                    response = MessageHelper::NewSharedLogOpWithoutData(SharedLogResultType::ASYNC_EMPTY);
                } else {
                    UNREACHABLE();
                }
                response.response_id = result.id;
                SendLocalOpWithResponse(
                    op, &response, result.metalog_progress,
                    [this, query] /*on_finished*/ () {
                        ongoing_local_reads_.RemoveChecked(query.client_data);
                    });
            } else {
                SharedLogResultType result_type;
                if (query.type == IndexQuery::kSync) {
                    result_type = SharedLogResultType::EMPTY;
                } else if (query.type == IndexQuery::kAsync) {
                    result_type = SharedLogResultType::ASYNC_EMPTY;
                } else {
                    UNREACHABLE();
                }
                SendReadResponseWithoutData(query, result_type, result.metalog_progress);
            }
            break;
        case IndexQueryResult::kViewContinue:
        case IndexQueryResult::kAuxContinue:
        case IndexQueryResult::kFoundRangeContinue:
            ProcessIndexContinueResult(result, &more_results);
            break;
        case IndexQueryResult::kEOF:
            DCHECK(query.direction == IndexQuery::ReadDirection::kReadNextU);
            if (query.origin_node_id == my_node_id()) {
                Message response = BuildLocalReadOKResponseWithoutData(result.id);
                LocalOp* op = ongoing_local_reads_.PeekChecked(query.client_data);
                SendLocalOpWithResponse(
                    op, &response, result.metalog_progress,
                    [this, query] /*on_finished*/ () {
                        ongoing_local_reads_.RemoveChecked(query.client_data);
                    });
            } else {
                // perceive empty read ok as EOF
                SendReadResponseWithoutData(query, SharedLogResultType::READ_OK, result.metalog_progress);
            }
            break;
        default:
            UNREACHABLE();
        }
    }
    // if (!more_results.empty()) {
    //     ProcessIndexQueryResults(more_results);
    // }
    return more_results;
}

void Engine::ProcessRequests(const std::vector<SharedLogRequest>& requests) {
    for (const SharedLogRequest& request : requests) {
        if (request.local_op == nullptr) {
            MessageHandler(request.message, STRING_AS_SPAN(request.payload));
        } else {
            LocalOpHandler(reinterpret_cast<LocalOp*>(request.local_op));
        }
    }
}

void Engine::LogCachePut(const LogMetaData& log_metadata,
                         std::span<const uint64_t> user_tags,
                         std::span<const char> log_data) {
    if (!log_cache_enabled_) {
        return;
    }
    HVLOG_F(1, "Store cache for log entry seqnum={:016X}", log_metadata.seqnum);
    log_cache_->PutLogData(log_metadata, user_tags, log_data);
}

std::optional<LogEntry> Engine::LogCacheGet(uint64_t seqnum) {
    if (!log_cache_enabled_) {
        return std::nullopt;
    }
    return log_cache_->GetLogData(seqnum);
}

void Engine::LogCachePutAuxData(const AuxEntry& aux_entry) {
    if (!log_cache_enabled_) {
        return;
    }
    log_cache_->PutAuxData(aux_entry);
}

void Engine::LogCachePutAuxData(uint64_t seqnum, uint64_t localid, uint64_t tag,
                                std::span<const char> aux_data) {
    if (!log_cache_enabled_) {
        return;
    }
    log_cache_->PutAuxData(seqnum, localid, tag, aux_data);
}

std::optional<AuxEntry> Engine::LogCacheGetAuxData(uint64_t seqnum) {
    if (!log_cache_enabled_) {
        return std::nullopt;
    }
    return log_cache_->GetAuxData(seqnum);
}

std::optional<AuxEntry> Engine::LogCacheGetAuxDataPrev(uint64_t tag, uint64_t seqnum) {
    if (!log_cache_enabled_) {
        return std::nullopt;
    }
    return log_cache_->GetAuxDataPrev(tag, seqnum);
}

std::optional<AuxEntry> Engine::LogCacheGetAuxDataNext(uint64_t tag, uint64_t seqnum) {
    if (!log_cache_enabled_) {
        return std::nullopt;
    }
    return log_cache_->GetAuxDataNext(tag, seqnum);
}

SharedLogMessage Engine::BuildReadRequestMessage(LocalOp* op) {
    DCHECK(protocol::SharedLogOpTypeHelper::IsFuncRead(op->type));
    SharedLogMessage request = SharedLogMessageHelper::NewReadMessage(op->type);
    request.origin_node_id = my_node_id();
    request.hop_times = 1;
    request.client_data = op->id;
    request.user_logspace = op->user_logspace;
    request.query_tag = op->query_tag;
    request.query_seqnum = op->seqnum;
    request.query_start_seqnum = op->query_start_seqnum;
    request.user_metalog_progress = op->metalog_progress;
    request.flags |= protocol::kReadInitialFlag;
    if ((op->flags & LocalOp::kReadLocalIdFlag) != 0) {
        request.flags |= protocol::kReadLocalIdFlag;
    }
    request.prev_view_id = 0;
    request.prev_engine_id = 0;
    request.flags &= ~protocol::kReadPrevFoundFlag;
    return request;
}

SharedLogMessage Engine::BuildReadRequestMessage(const IndexQueryResult& result) {
    DCHECK(result.state == IndexQueryResult::kViewContinue ||
           result.state == IndexQueryResult::kAuxContinue ||
           result.state == IndexQueryResult::kFoundRangeContinue);
    if (result.state == IndexQueryResult::kViewContinue) {
        IndexQuery query = result.original_query;
        SharedLogMessage request =
            SharedLogMessageHelper::NewReadMessage(query.DirectionToOpType());
        request.origin_node_id = query.origin_node_id;
        request.hop_times = query.hop_times + 1;
        request.client_data = query.client_data;
        // drop readed query.promised_auxdata here, use remote node's aux data
        request.user_logspace = query.user_logspace;
        request.query_tag = query.user_tag;
        request.query_seqnum = query.query_seqnum;
        request.user_metalog_progress = result.metalog_progress;
        if ((query.flags & IndexQuery::kReadLocalIdFlag) != 0) {
            request.flags |= protocol::kReadLocalIdFlag;
        }
        if (result.found_result.seqnum != protocol::kInvalidLogSeqNum) {
            request.prev_view_id = result.found_result.view_id;
            request.prev_engine_id = result.found_result.engine_id;
            request.prev_found_seqnum = result.found_result.seqnum;
            request.flags |= protocol::kReadPrevFoundFlag;
        } else {
            request.query_start_seqnum = query.query_start_seqnum;
            request.flags &= ~protocol::kReadPrevFoundFlag;
        }
        return request;
    } else if (result.state == IndexQueryResult::kAuxContinue) {
        IndexQuery query = result.original_query;
        SharedLogMessage request =
            SharedLogMessageHelper::NewReadMessage(query.DirectionToOpType());
        request.origin_node_id = query.origin_node_id;
        request.hop_times = query.hop_times;
        request.client_data = query.client_data;
        // drop readed query.promised_auxdata here, use remote node's aux data
        request.user_logspace = query.user_logspace;
        request.query_tag = query.user_tag;
        request.query_seqnum = result.found_result.seqnum;
        request.query_start_seqnum = query.query_start_seqnum;
        request.user_metalog_progress = result.metalog_progress;
        DCHECK((query.flags & IndexQuery::kReadLocalIdFlag) != 0);
        request.flags &= ~protocol::kReadLocalIdFlag;
        if ((query.flags & IndexQuery::kReadFromCachedFlag) != 0) {
            request.flags |= protocol::kLogQueryFromCachedFlag;
        }
        request.prev_view_id = 0;
        request.prev_engine_id = 0;
        request.flags &= ~protocol::kReadPrevFoundFlag;
        return request;
    } else if (result.state == IndexQueryResult::kFoundRangeContinue) {
        IndexQuery query = result.original_query;
        SharedLogMessage request =
            SharedLogMessageHelper::NewReadMessage(query.DirectionToOpType());
        request.origin_node_id = query.origin_node_id;
        request.hop_times = query.hop_times;
        request.client_data = query.client_data;
        // drop readed query.promised_auxdata here, use remote node's aux data
        request.user_logspace = query.user_logspace;
        request.query_tag = query.user_tag;
        uint64_t future_localid = result.found_result.future_localid;
        if (future_localid == protocol::kInvalidLogLocalId) {
            request.flags &= ~protocol::kReadLocalIdFlag;
            request.query_seqnum = result.found_result.end_seqnum;
        } else {
            DCHECK((query.flags & IndexQuery::kReadLocalIdFlag) != 0);
            request.flags |= protocol::kReadLocalIdFlag;
            request.localid = future_localid;
        }
        request.query_start_seqnum = result.found_result.seqnum + 1;
        request.user_metalog_progress = result.metalog_progress;
        if ((query.flags & IndexQuery::kReadFromCachedFlag) != 0) {
            request.flags |= protocol::kLogQueryFromCachedFlag;
        }
        request.prev_view_id = 0;
        request.prev_engine_id = 0;
        request.flags &= ~protocol::kReadPrevFoundFlag;
        return request;
    } else {
        UNREACHABLE();
    }
}

IndexQuery Engine::UpdateQueryWithAux(IndexQuery& query) {
    if (query.direction == IndexQuery::ReadDirection::kReadPrevAux ||
        query.direction == IndexQuery::ReadDirection::kReadNextU) {
        if (query.direction == IndexQuery::ReadDirection::kReadNextU) {
            DCHECK(query.initial);
            if ((query.flags & IndexQuery::kReadFromCachedFlag) == 0) {
                // not skipping cached entries
                return query;
            }
            uint64_t target_seqnum;
            if ((query.flags & IndexQuery::kReadLocalIdFlag) != 0) {
                // use localid as target
                target_seqnum = kMaxLogSeqNum;
            } else {
                // use seqnum as target
                if (query.query_seqnum == 0) {
                    return query;
                }
                // syncto not including target seqnum
                target_seqnum = query.query_seqnum - 1;
            }
            std::optional<AuxEntry> aux_entry =
                LogCacheGetAuxDataPrev(query.user_tag, target_seqnum);
            if (aux_entry.has_value() && aux_entry->metadata.seqnum >= query.query_start_seqnum) {
                query.promised_auxdata = aux_entry;
            } else {
                query.promised_auxdata = std::nullopt;
            }
            // HVLOG_F(1, "query kReadNextU flags={:04X} tag={} seqfrom={:016X} seqnum={:016X} found={}",
            //             query.flags, query.user_tag, query.query_start_seqnum, query.query_seqnum, query.promised_auxdata.has_value());
        } else {
            HVLOG_F(1, "query kReadPrevAux flags={:04X}", query.flags);
            std::optional<AuxEntry> aux_entry =
                LogCacheGetAuxDataPrev(query.user_tag, query.query_seqnum);
            query.promised_auxdata = aux_entry;
            if (aux_entry.has_value()) {
                // adjust query seqnum to the cached position for aux reading
                query.query_seqnum = aux_entry->metadata.seqnum;
            }
        }
    }
    // FUTURE: kReadNextAux
    return query;
}

IndexQuery Engine::BuildIndexQuery(LocalOp* op) {
    auto query = IndexQuery {
        .type = protocol::SharedLogOpTypeHelper::IsAsyncSharedLogOp(op->type) ? \
                    IndexQuery::QueryType::kAsync : IndexQuery::QueryType::kSync,
        .direction = IndexQuery::DirectionFromOpType(op->type),
        .origin_node_id = my_node_id(),
        .hop_times = 0,
        .initial = true,
        .client_data = op->id,
        .promised_auxdata = std::nullopt,
        .user_logspace = op->user_logspace,
        .user_tag = op->query_tag,
        .query_seqnum = op->seqnum,
        .query_start_seqnum = op->query_start_seqnum,
        .metalog_progress = op->metalog_progress,
        .flags = 0,
        .prev_found_result = {
            .view_id = 0,
            .engine_id = 0,
            .seqnum = kInvalidLogSeqNum,
            .localid = kInvalidLogId,
        },
        .next_result_id = 0
    };
    if ((op->flags & LocalOp::kReadLocalIdFlag) != 0) {
        query.flags |= IndexQuery::kReadLocalIdFlag;
        query.query_localid = op->localid;
    }
    if ((op->flags & LocalOp::kReadFromCachedFlag) != 0) {
        query.flags |= IndexQuery::kReadFromCachedFlag;
    }
    return UpdateQueryWithAux(query);
}

IndexQuery Engine::BuildIndexQuery(const SharedLogMessage& message) {
    SharedLogOpType op_type = SharedLogMessageHelper::GetOpType(message);
    auto query = IndexQuery {
        .type = protocol::SharedLogOpTypeHelper::IsAsyncSharedLogOp(op_type) ? \
                    IndexQuery::QueryType::kAsync : IndexQuery::QueryType::kSync,
        .direction = IndexQuery::DirectionFromOpType(op_type),
        .origin_node_id = message.origin_node_id,
        .hop_times = message.hop_times,
        .initial = (message.flags | protocol::kReadInitialFlag) != 0,
        .client_data = message.client_data,
        .promised_auxdata = std::nullopt,
        .user_logspace = message.user_logspace,
        .user_tag = message.query_tag,
        .query_seqnum = message.query_seqnum,
        .metalog_progress = message.user_metalog_progress,
        .flags = 0,
        .next_result_id = 0
    };
    if ((message.flags & protocol::kReadPrevFoundFlag) != 0) {
        query.query_start_seqnum = 0;
        query.prev_found_result = IndexFoundResult {
            .view_id = message.prev_view_id,
            .engine_id = message.prev_engine_id,
            .seqnum = message.prev_found_seqnum,
            .localid = protocol::kInvalidLogLocalId
        };
    } else {
        query.query_start_seqnum = message.query_start_seqnum;
        query.prev_found_result = IndexFoundResult {
            .view_id = 0,
            .engine_id = 0,
            .seqnum = protocol::kInvalidLogSeqNum,
            .localid = protocol::kInvalidLogLocalId
        };
    }
    if ((message.flags & protocol::kReadLocalIdFlag) != 0) {
        query.flags |= IndexQuery::kReadLocalIdFlag;
        query.query_localid = message.localid;
    }
    if ((message.flags & protocol::kLogQueryFromCachedFlag) != 0) {
        query.flags |= IndexQuery::kReadFromCachedFlag;
    }
    return UpdateQueryWithAux(query);
}

IndexQuery Engine::BuildIndexQuery(const IndexQueryResult& result) {
    DCHECK(result.state == IndexQueryResult::kViewContinue ||
           result.state == IndexQueryResult::kAuxContinue ||
           result.state == IndexQueryResult::kFoundRangeContinue);
    if (result.state == IndexQueryResult::kViewContinue) {
        IndexQuery query = result.original_query;
        query.initial = false;
        query.metalog_progress = result.metalog_progress;
        query.prev_found_result = result.found_result;
        return UpdateQueryWithAux(query);
    } else if (result.state == IndexQueryResult::kAuxContinue) {
        IndexQuery query = result.original_query;
        query.metalog_progress = result.metalog_progress;
        query.flags &= ~IndexQuery::kReadLocalIdFlag;
        query.promised_auxdata = std::nullopt;
        // new target seqnum
        query.query_seqnum = result.found_result.seqnum;
        return UpdateQueryWithAux(query);
    } else if (result.state == IndexQueryResult::kFoundRangeContinue) {
        IndexQuery query = result.original_query;
        uint64_t future_localid = result.found_result.future_localid;
        if (future_localid == protocol::kInvalidLogLocalId) {
            query.flags &= ~IndexQuery::kReadLocalIdFlag;
            query.query_seqnum = result.found_result.end_seqnum;
        } else {
            DCHECK((query.flags & IndexQuery::kReadLocalIdFlag) != 0);
            query.query_localid = future_localid;
        }
        // only the first one needs to resolve cached
        query.flags &= ~IndexQuery::kReadFromCachedFlag;

        query.query_start_seqnum = result.found_result.seqnum + 1;
        query.metalog_progress = result.metalog_progress;
        query.promised_auxdata = std::nullopt;
        query.next_result_id = result.id;
        // DEBUG: set initial to false would put next continue query to pending,
        // which is not SyncTo intents.
        // Originally SyncTo use initial to assert InitRead and ContinueRead, only
        // for debug use.
        // query.initial = false;
        return query;
    } else {
        UNREACHABLE();
    }
}

}  // namespace log
}  // namespace faas
