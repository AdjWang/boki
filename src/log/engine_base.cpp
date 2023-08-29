#include "log/engine_base.h"

#include "common/time.h"
#include "log/flags.h"
#include "log/utils.h"
#include "server/constants.h"
#include "engine/engine.h"
#include "utils/bits.h"

#define log_header_ "LogEngineBase: "

namespace faas {
namespace log {

using protocol::FuncCall;
using protocol::FuncCallHelper;
using protocol::Message;
using protocol::MessageHelper;
using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;
using protocol::SharedLogResultType;

EngineBase::EngineBase(engine::Engine* engine)
    : node_id_(engine->node_id_),
      engine_(engine),
      next_local_op_id_(0) {}

EngineBase::~EngineBase() {}

zk::ZKSession* EngineBase::zk_session() {
    return engine_->zk_session();
}

void EngineBase::Start() {
    SetupZKWatchers();
    SetupTimers();
}

void EngineBase::Stop() {}

void EngineBase::SetupZKWatchers() {
    view_watcher_.SetViewCreatedCallback(
        [this] (const View* view) {
            this->OnViewCreated(view);
        }
    );
    view_watcher_.SetViewFrozenCallback(
        [this] (const View* view) {
            this->OnViewFrozen(view);
        }
    );
    view_watcher_.SetViewFinalizedCallback(
        [this] (const FinalizedView* finalized_view) {
            this->OnViewFinalized(finalized_view);
        }
    );
    view_watcher_.StartWatching(zk_session());
}

void EngineBase::SetupTimers() {
}

void EngineBase::OnNewExternalFuncCall(const FuncCall& func_call, uint32_t log_space) {
    absl::MutexLock fn_ctx_lk(&fn_ctx_mu_);
    if (fn_call_ctx_.contains(func_call.full_call_id)) {
        HLOG(FATAL) << "FuncCall already exists: "
                    << FuncCallHelper::DebugString(func_call);
    }
    fn_call_ctx_[func_call.full_call_id] = FnCallContext {
        .user_logspace = log_space,
        .metalog_progress = 0,
        .parent_call_id = protocol::kInvalidFuncCallId
    };
    // HLOG(INFO) << "Applying new external function call: "
    //            << FuncCallHelper::DebugString(func_call)
    //            << DebugListExistingFnCall(fn_call_ctx_);
}

void EngineBase::OnNewInternalFuncCall(const FuncCall& func_call,
                                       const FuncCall& parent_func_call) {
    absl::MutexLock fn_ctx_lk(&fn_ctx_mu_);
    if (fn_call_ctx_.contains(func_call.full_call_id)) {
        HLOG(FATAL) << "FuncCall already exists: "
                    << FuncCallHelper::DebugString(func_call);
    }
    if (!fn_call_ctx_.contains(parent_func_call.full_call_id)) {
        HLOG(FATAL) << "Cannot find parent FuncCall: "
                    << FuncCallHelper::DebugString(parent_func_call);
    }
    FnCallContext ctx = fn_call_ctx_.at(parent_func_call.full_call_id);
    ctx.parent_call_id = parent_func_call.full_call_id;
    fn_call_ctx_[func_call.full_call_id] = std::move(ctx);
    // HLOG(INFO) << "Applying new internal function call: "
    //            << FuncCallHelper::DebugString(func_call)
    //            << DebugListExistingFnCall(fn_call_ctx_);
}

void EngineBase::OnFuncCallCompleted(const FuncCall& func_call) {
    absl::MutexLock fn_ctx_lk(&fn_ctx_mu_);
    if (!fn_call_ctx_.contains(func_call.full_call_id)) {
        HLOG(FATAL) << "Cannot find FuncCall: "
                    << FuncCallHelper::DebugString(func_call);
    }
    fn_call_ctx_.erase(func_call.full_call_id);
}

void EngineBase::LocalOpHandler(LocalOp* op) {
    switch (op->type) {
    case SharedLogOpType::APPEND:
    case SharedLogOpType::ASYNC_APPEND:
        HandleLocalAppend(op);
        break;
    case SharedLogOpType::READ_NEXT:
    case SharedLogOpType::READ_PREV:
    case SharedLogOpType::READ_NEXT_B:
    case SharedLogOpType::READ_SYNCTO:
    case SharedLogOpType::ASYNC_READ_NEXT:
    case SharedLogOpType::ASYNC_READ_PREV:
    case SharedLogOpType::ASYNC_READ_PREV_AUX:
    case SharedLogOpType::ASYNC_READ_NEXT_B:
    case SharedLogOpType::ASYNC_READ_LOCALID:
        HandleLocalRead(op);
        break;
    case SharedLogOpType::TRIM:
        HandleLocalTrim(op);
        break;
    case SharedLogOpType::SET_AUXDATA:
        HandleLocalSetAuxData(op);
        break;
    default:
        UNREACHABLE();
    }
}

void EngineBase::MessageHandler(const SharedLogMessage& message,
                                std::span<const char> payload) {
    switch (SharedLogMessageHelper::GetOpType(message)) {
    case SharedLogOpType::READ_NEXT:
    case SharedLogOpType::READ_PREV:
    case SharedLogOpType::READ_NEXT_B:
    case SharedLogOpType::READ_SYNCTO:
    case SharedLogOpType::ASYNC_READ_NEXT:
    case SharedLogOpType::ASYNC_READ_PREV:
    case SharedLogOpType::ASYNC_READ_PREV_AUX:
    case SharedLogOpType::ASYNC_READ_NEXT_B:
    case SharedLogOpType::ASYNC_READ_LOCALID:
        HandleRemoteRead(message);
        break;
    case SharedLogOpType::INDEX_DATA:
        OnRecvNewIndexData(message, payload);
        break;
    case SharedLogOpType::METALOGS:
        OnRecvNewMetaLogs(message, payload);
        break;
    case SharedLogOpType::RESPONSE:
        OnRecvResponse(message, payload);
        break;
    default:
        UNREACHABLE();
    }
}

void EngineBase::PopulateLogTagsAndData(const Message& message, LocalOp* op) {
    DCHECK(protocol::SharedLogOpTypeHelper::IsFuncAppend(op->type) ||
           op->type == SharedLogOpType::READ_SYNCTO ||
           op->type == SharedLogOpType::SET_AUXDATA);
    DCHECK_EQ(message.log_aux_data_size, 0U);
    std::span<const char> data = MessageHelper::GetInlineData(message);
    size_t num_tags = message.log_num_tags;
    if (num_tags > 0) {
        op->user_tags.resize(num_tags);
        memcpy(op->user_tags.data(), data.data(), num_tags * sizeof(uint64_t));
    }
    op->data.AppendData(data.subspan(num_tags * sizeof(uint64_t)));
}

std::string EngineBase::DebugListExistingFnCall(const absl::flat_hash_map</* full_call_id */ uint64_t, FnCallContext>& fn_call_ctx) {
    std::string output(" Existing function call: ");
    for (auto it = fn_call_ctx.begin(); it != fn_call_ctx.end(); ++it) {
        output += fmt::format("{} ", it->first);
    }
    output += "\n";
    return output;
}
// DEBUG
static std::string DebugPrintMessage(const Message& message) {
    return fmt::format("msg_type={} client_id={} client_data={} log_op_type={} flags={:08X}",
        message.message_type, message.log_client_id, message.log_client_data, message.log_op, message.flags);
}
void EngineBase::OnMessageFromFuncWorker(const Message& message) {
    protocol::FuncCall func_call = MessageHelper::GetFuncCall(message);
    FnCallContext ctx;
    {
        absl::ReaderMutexLock fn_ctx_lk(&fn_ctx_mu_);
        if (!fn_call_ctx_.contains(func_call.full_call_id)) {
            HLOG(ERROR) << "Cannot find FuncCall: "
                        << FuncCallHelper::DebugString(func_call)
                        << DebugListExistingFnCall(fn_call_ctx_)
                        << DebugPrintMessage(message);
            return;
        }
        // HLOG(INFO) << "Found FuncCall: "
        //            << FuncCallHelper::DebugString(func_call)
        //            << DebugListExistingFnCall(fn_call_ctx_);
        ctx = fn_call_ctx_.at(func_call.full_call_id);
    }

    LocalOp* op = log_op_pool_.Get();
    op->id = next_local_op_id_.fetch_add(1, std::memory_order_acq_rel);
    op->start_timestamp = GetMonotonicMicroTimestamp();
    op->client_id = message.log_client_id;
    op->client_data = message.log_client_data;
    op->func_call_id = func_call.full_call_id;
    op->user_logspace = ctx.user_logspace;
    op->metalog_progress = ctx.metalog_progress;
    op->type = MessageHelper::GetSharedLogOpType(message);
    op->seqnum = kInvalidLogSeqNum;
    op->query_start_seqnum = 0;
    op->query_tag = kInvalidLogTag;
    op->user_tags.clear();
    op->data.Reset();
    op->flags = 0;
    op->response_counter.Reset();

    // TODO: modified format
    // DCHECK(!(protocol::SharedLogOpTypeHelper::IsFuncRead(op->type) &&
    //          message.log_seqnum == protocol::kInvalidLogSeqNum &&
    //          message.log_localid == protocol::kInvalidLogLocalId));

    switch (op->type) {
    case SharedLogOpType::APPEND:
    case SharedLogOpType::ASYNC_APPEND:
        PopulateLogTagsAndData(message, op);
        break;
    case SharedLogOpType::READ_NEXT:
    case SharedLogOpType::READ_PREV:
    case SharedLogOpType::READ_NEXT_B:
    case SharedLogOpType::ASYNC_READ_NEXT:
    case SharedLogOpType::ASYNC_READ_PREV:
    case SharedLogOpType::ASYNC_READ_PREV_AUX:
    case SharedLogOpType::ASYNC_READ_NEXT_B:
        op->query_tag = message.log_tag;
        DCHECK(message.log_query_seqnum != protocol::kInvalidLogSeqNum);
        op->seqnum = message.log_query_seqnum;
        break;
    case SharedLogOpType::READ_SYNCTO:
        // get aux tags to grab
        PopulateLogTagsAndData(message, op);
        // get query infos
        op->query_tag = message.log_tag;
        if (message.log_query_start_seqnum != protocol::kInvalidLogSeqNum) {
            op->query_start_seqnum = message.log_query_start_seqnum;
        }
        if ((message.flags & protocol::kLogQueryLocalIdFlag) == 0) {
            DCHECK(message.log_query_seqnum != protocol::kInvalidLogSeqNum);
            op->seqnum = message.log_query_seqnum;
            DCHECK(op->query_start_seqnum <= op->seqnum)
                << fmt::format("start_seqnum={:016X} target_seqnum={:016X}",
                               op->query_start_seqnum, op->seqnum);
        } else {
            op->flags |= LocalOp::kReadLocalIdFlag;
            DCHECK(message.log_query_localid != protocol::kInvalidLogLocalId);
            op->localid = message.log_query_localid;
        }
        if ((message.flags & protocol::kLogQueryFromCachedFlag) != 0) {
            op->flags |= LocalOp::kReadFromCachedFlag;
        }
        break;
    case SharedLogOpType::ASYNC_READ_LOCALID:
        DCHECK((message.flags & protocol::kLogQueryLocalIdFlag) != 0);
        op->flags |= LocalOp::kReadLocalIdFlag;
        DCHECK(message.log_query_localid != protocol::kInvalidLogLocalId);
        op->localid = message.log_query_localid;
        break;
    case SharedLogOpType::TRIM:
        op->seqnum = message.log_seqnum;
        break;
    case SharedLogOpType::SET_AUXDATA:
        op->query_tag = message.log_aux_key;
        DCHECK(message.log_num_tags == 0);
        PopulateLogTagsAndData(message, op);
        op->seqnum = message.log_seqnum;
        break;
    default:
        HLOG(FATAL) << "Unknown shared log op type: " << message.log_op;
    }

    LocalOpHandler(op);
}

void EngineBase::OnRecvSharedLogMessage(int conn_type, uint16_t src_node_id,
                                        const SharedLogMessage& message,
                                        std::span<const char> payload) {
    SharedLogOpType op_type = SharedLogMessageHelper::GetOpType(message);
    DCHECK(
        (conn_type == kSequencerIngressTypeId && op_type == SharedLogOpType::METALOGS)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::READ_NEXT)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::READ_PREV)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::READ_NEXT_B)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::READ_SYNCTO)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::ASYNC_READ_LOCALID)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::ASYNC_READ_NEXT)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::ASYNC_READ_NEXT_B)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::ASYNC_READ_PREV)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::ASYNC_READ_PREV_AUX)
     || (conn_type == kStorageIngressTypeId && op_type == SharedLogOpType::INDEX_DATA)
     || op_type == SharedLogOpType::RESPONSE
    ) << fmt::format("Invalid combination: conn_type={:#x}, op_type={:#x}",
                     conn_type, message.op_type);
    MessageHandler(message, payload);
}

void EngineBase::ReplicateLogEntry(const View* view, const LogMetaData& log_metadata,
                                   std::span<const uint64_t> user_tags,
                                   std::span<const char> log_data) {
    SharedLogMessage message = SharedLogMessageHelper::NewReplicateMessage();
    log_utils::PopulateMetaDataToMessage(log_metadata, &message);
    message.origin_node_id = node_id_;
    message.payload_size = gsl::narrow_cast<uint32_t>(
        user_tags.size() * sizeof(uint64_t) + log_data.size());
    const View::Engine* engine_node = view->GetEngineNode(node_id_);
    for (uint16_t storage_id : engine_node->GetStorageNodes()) {
        engine_->SendSharedLogMessage(protocol::ConnType::ENGINE_TO_STORAGE,
                                      storage_id, message,
                                      VECTOR_AS_CHAR_SPAN(user_tags), log_data);
    }
}

void EngineBase::PropagateAuxData(const View* view, const LogMetaData& log_metadata, 
                                  std::span<const char> aux_data) {
    uint16_t engine_id = gsl::narrow_cast<uint16_t>(
        bits::HighHalf64(log_metadata.localid));
    DCHECK(view->contains_engine_node(engine_id));
    const View::Engine* engine_node = view->GetEngineNode(engine_id);
    SharedLogMessage message = SharedLogMessageHelper::NewSetAuxDataMessage(
        log_metadata.seqnum);
    message.origin_node_id = node_id_;
    message.payload_size = gsl::narrow_cast<uint32_t>(aux_data.size());
    for (uint16_t storage_id : engine_node->GetStorageNodes()) {
        engine_->SendSharedLogMessage(protocol::ConnType::ENGINE_TO_STORAGE,
                                      storage_id, message, aux_data);
    }
}

void EngineBase::SendLocalOpWithResponse(LocalOp* op, Message* response,
                                         uint64_t metalog_progress,
                                         std::function<void()> on_finished) {
    if (metalog_progress > 0) {
        absl::MutexLock fn_ctx_lk(&fn_ctx_mu_);
        if (fn_call_ctx_.contains(op->func_call_id)) {
            FnCallContext& ctx = fn_call_ctx_[op->func_call_id];
            if (metalog_progress > ctx.metalog_progress) {
                ctx.metalog_progress = metalog_progress;
            }
        }
    }
    response->log_client_data = op->client_data;
    HVLOG_F(1, "EngineBase send response op_id={} response_id={} cid={} resp_flags={:08X}",
        op->id, response->response_id, op->client_data, response->flags);
    engine_->SendFuncWorkerMessage(op->client_id, response);

    bool finished;
    if ((response->flags & protocol::kLogResponseContinueFlag) != 0) {
        // Continue
        finished = op->response_counter.AddCountAndCheck(1);
    } else {
        // EOFData or EOF
        finished = op->response_counter.SetTargetAndCheck(response->response_id);
    }
    if (finished) {
        if (on_finished != nullptr) {
            // Resource reclaiming operations from engine must be performed
            // before returning op
            on_finished();
        }
        DCHECK(op->response_counter.IsResolved()) << op->InspectRead();
        log_op_pool_.Return(op);
    }
}

bool EngineBase::SendIndexReadRequest(const View::Sequencer* sequencer_node,
                                      SharedLogMessage* request) {
    static constexpr int kMaxRetries = 3;

    request->sequencer_id = sequencer_node->node_id();
    request->view_id = sequencer_node->view()->id();
    for (int i = 0; i < kMaxRetries; i++) {
        uint16_t engine_id = sequencer_node->PickIndexEngineNode();
        if (engine_id == node_id_) {
            continue;
        }
        bool success = engine_->SendSharedLogMessage(
            protocol::ConnType::SLOG_ENGINE_TO_ENGINE, engine_id, *request);
        if (success) {
            return true;
        }
    }
    return false;
}

bool EngineBase::SendStorageReadRequest(const IndexQueryResult& result,
                                        const View::Engine* engine_node) {
    static constexpr int kMaxRetries = 3;
    DCHECK(result.state == IndexQueryResult::kFound);

    uint64_t seqnum = result.found_result.seqnum;
    SharedLogMessage request = SharedLogMessageHelper::NewReadAtMessage(
        bits::HighHalf64(seqnum), bits::LowHalf64(seqnum));
    request.user_metalog_progress = result.metalog_progress;
    request.origin_node_id = result.original_query.origin_node_id;
    request.hop_times = result.original_query.hop_times + 1;
    request.client_data = result.original_query.client_data;
    for (int i = 0; i < kMaxRetries; i++) {
        uint16_t storage_id = engine_node->PickStorageNode();
        bool success = engine_->SendSharedLogMessage(
            protocol::ConnType::ENGINE_TO_STORAGE, storage_id, request);
        if (success) {
            return true;
        }
    }
    return false;
}

void EngineBase::SendReadResponse(const IndexQuery& query,
                                  protocol::SharedLogMessage* response,
                                  std::span<const char> user_tags_payload,
                                  std::span<const char> data_payload,
                                  std::span<const char> aux_data_payload) {
    response->origin_node_id = node_id_;
    response->hop_times = query.hop_times + 1;
    response->client_data = query.client_data;
    response->payload_size = gsl::narrow_cast<uint32_t>(
        user_tags_payload.size() + data_payload.size() + aux_data_payload.size());
    uint16_t engine_id = query.origin_node_id;
    bool success = engine_->SendSharedLogMessage(
        protocol::ConnType::SLOG_ENGINE_TO_ENGINE,
        engine_id, *response, user_tags_payload, data_payload, aux_data_payload);
    if (!success) {
        HLOG_F(WARNING, "Failed to send read response to engine {}", engine_id);
        HLOG_F(WARNING, "[DEBUG] StackTrace:\n{}", utils::DumpStackTrace());
    }
}

void EngineBase::SendReadResponseWithoutData(const IndexQuery& query,
                                             protocol::SharedLogResultType result_type,
                                             uint64_t metalog_progress) {
    SharedLogMessage response = SharedLogMessageHelper::NewResponse(result_type);
    response.user_metalog_progress = metalog_progress;
    SendReadResponse(query, &response);
}

bool EngineBase::SendSequencerMessage(uint16_t sequencer_id,
                                      SharedLogMessage* message,
                                      std::span<const char> payload) {
    message->origin_node_id = node_id_;
    message->payload_size = gsl::narrow_cast<uint32_t>(payload.size());
    return engine_->SendSharedLogMessage(
        protocol::ConnType::ENGINE_TO_SEQUENCER,
        sequencer_id, *message, payload);
}

server::IOWorker* EngineBase::SomeIOWorker() {
    return engine_->SomeIOWorker();
}

}  // namespace log
}  // namespace faas
