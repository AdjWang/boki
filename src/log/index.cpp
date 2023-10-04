#include "log/index.h"

#include "log/utils.h"

namespace faas {
namespace log {

IndexQuery::ReadDirection IndexQuery::DirectionFromOpType(protocol::SharedLogOpType op_type) {
    switch (op_type) {
    case protocol::SharedLogOpType::READ_NEXT:
    case protocol::SharedLogOpType::ASYNC_READ_NEXT:
        return IndexQuery::kReadNext;
    case protocol::SharedLogOpType::READ_PREV:
    case protocol::SharedLogOpType::ASYNC_READ_PREV:
        return IndexQuery::kReadPrev;
    case protocol::SharedLogOpType::READ_NEXT_B:
    case protocol::SharedLogOpType::ASYNC_READ_NEXT_B:
        return IndexQuery::kReadNextB;
    case protocol::SharedLogOpType::ASYNC_READ_LOCALID:
        return IndexQuery::kReadLocalId;
    default:
        UNREACHABLE();
    }
}

protocol::SharedLogOpType IndexQuery::DirectionToOpType() const {
    if (type == IndexQuery::kAsync) {
        switch (direction) {
        case IndexQuery::kReadNext:
            return protocol::SharedLogOpType::ASYNC_READ_NEXT;
        case IndexQuery::kReadPrev:
            return protocol::SharedLogOpType::ASYNC_READ_PREV;
        case IndexQuery::kReadNextB:
            return protocol::SharedLogOpType::ASYNC_READ_NEXT_B;
        case IndexQuery::kReadLocalId:
            return protocol::SharedLogOpType::ASYNC_READ_LOCALID;
        default:
            UNREACHABLE();
        }
    } else {
        switch (direction) {
        case IndexQuery::kReadNext:
            return protocol::SharedLogOpType::READ_NEXT;
        case IndexQuery::kReadPrev:
            return protocol::SharedLogOpType::READ_PREV;
        case IndexQuery::kReadNextB:
            return protocol::SharedLogOpType::READ_NEXT_B;
        default:
            UNREACHABLE();
        }
    }
}

Index::Index(const View* view, uint16_t sequencer_id)
    : LogSpaceBase(LogSpaceBase::kFullMode, view, sequencer_id),
      index_data_(identifier()),
      data_received_seqnum_position_(0) {
    log_header_ = fmt::format("LogIndex[{}-{}]: ", view->id(), sequencer_id);
    state_ = kNormal;
}

Index::~Index() {}

void Index::ProvideIndexData(const IndexDataProto& index_data) {
    DCHECK_EQ(identifier(), index_data.logspace_id());
    int n = index_data.seqnum_halves_size();
    DCHECK_EQ(n, index_data.local_ids_size());
    DCHECK_EQ(n, index_data.user_logspaces_size());
    DCHECK_EQ(n, index_data.user_tag_sizes_size());
    uint32_t total_tags = absl::c_accumulate(index_data.user_tag_sizes(), 0U);
    DCHECK_EQ(static_cast<int>(total_tags), index_data.user_tags_size());
    auto tag_iter = index_data.user_tags().begin();
    for (int i = 0; i < n; i++) {
        size_t num_tags = index_data.user_tag_sizes(i);
        uint32_t seqnum = index_data.seqnum_halves(i);
        if (seqnum < index_data_.indexed_seqnum_position()) {
            tag_iter += num_tags;
            continue;
        }
        if (received_data_.count(seqnum) == 0) {
            received_data_[seqnum] = RecvIndexData {
                .local_id      = index_data.local_ids(i),
                .user_logspace = index_data.user_logspaces(i),
                .user_tags     = UserTagVec(tag_iter, tag_iter + num_tags)
            };
        } else {
#if DCHECK_IS_ON()
            const RecvIndexData& data = received_data_[seqnum];
            DCHECK_EQ(data.local_id, index_data.local_ids(i));
            DCHECK_EQ(data.user_logspace, index_data.user_logspaces(i));
            DCHECK_EQ(data.user_tags.size(),
                      gsl::narrow_cast<size_t>(index_data.user_tag_sizes(i)));
#endif
        }
        tag_iter += num_tags;
    }
    while (received_data_.count(data_received_seqnum_position_) > 0) {
        data_received_seqnum_position_++;
    }
    AdvanceIndexProgress();
}

void Index::MakeQuery(const IndexQuery& query) {
    IndexDataManager::QueryConsistencyType consistency_type = index_data_.CheckConsistency(query);
    switch (consistency_type) {
        case IndexDataManager::QueryConsistencyType::kInitFutureViewBail:
            HLOG_F(FATAL, "Cannot process query with metalog_progress from the future: "
                          "metalog_progress={}, my_view_id={}",
                   bits::HexStr0x(query.metalog_progress), bits::HexStr0x(view_->id()));
            break;
        case IndexDataManager::QueryConsistencyType::kInitCurrentViewPending: {
            // DEBUG: stat temporary
            IndexQuery& temp_query = const_cast<IndexQuery&>(query);
            temp_query.metalog_inside = false;

            uint32_t position = bits::LowHalf64(query.metalog_progress);
            pending_queries_.insert(std::make_pair(position, temp_query));
            break;
        }
        case IndexDataManager::QueryConsistencyType::kInitCurrentViewOK:
        case IndexDataManager::QueryConsistencyType::kInitPastViewOK: {
            IndexQueryResult result = ProcessQuery(query);
            ProcessQueryResult(result);
            break;
        }
        case IndexDataManager::QueryConsistencyType::kContOK:
            if (finalized()) {
                HVLOG_F(1, "MakeQuery Process query type=0x{:02X} seqnum=0x{:016X} since finalized",
                        uint16_t(query.type), query.query_seqnum);
                IndexQueryResult result = ProcessQuery(query);
                ProcessQueryResult(result);
            } else {
                pending_queries_.insert(std::make_pair(kMaxMetalogPosition, query));
            }
            break;
        default:
            UNREACHABLE();
    }
}

void Index::PollQueryResults(QueryResultVec* results) {
    if (pending_query_results_.empty()) {
        return;
    }
    if (results->empty()) {
        *results = std::move(pending_query_results_);
    } else {
        results->insert(results->end(),
                        pending_query_results_.begin(),
                        pending_query_results_.end());
    }
    pending_query_results_.clear();
}

void Index::OnMetaLogApplied(const MetaLogProto& meta_log_proto) {
    if (meta_log_proto.type() == MetaLogProto::NEW_LOGS) {
        const auto& new_logs_proto = meta_log_proto.new_logs_proto();
        uint32_t seqnum = new_logs_proto.start_seqnum();
        for (uint32_t delta : new_logs_proto.shard_deltas()) {
            seqnum += delta;
        }
        cuts_.push_back(std::make_pair(meta_log_proto.metalog_seqnum(), seqnum));
    }
    AdvanceIndexProgress();
}

void Index::OnFinalized(uint32_t metalog_position) {
    auto iter = pending_queries_.begin();
    while (iter != pending_queries_.end()) {
        DCHECK_EQ(iter->first, kMaxMetalogPosition);
        const IndexQuery& query = iter->second;
        ProcessQuery(query);
        iter = pending_queries_.erase(iter);
    }
}

void Index::AdvanceIndexProgress() {
    while (!cuts_.empty()) {
        uint32_t end_seqnum = cuts_.front().second;
        if (data_received_seqnum_position_ < end_seqnum) {
            break;
        }
        HVLOG_F(1, "Apply RecvIndexData until seqnum {}", bits::HexStr0x(end_seqnum));
        auto iter = received_data_.begin();
        while (iter != received_data_.end()) {
            uint32_t seqnum = iter->first;
            if (seqnum >= end_seqnum) {
                break;
            }
            const RecvIndexData& index_data = iter->second;
            // update index (globab view)
            uint16_t engine_id = gsl::narrow_cast<uint16_t>(
                bits::HighHalf64(index_data.local_id));
            index_data_.AddIndexData(index_data.user_logspace, seqnum,
                                     engine_id, index_data.user_tags);
            // update index map, to serve async log query
            index_data_.AddAsyncIndexData(index_data.local_id, seqnum);

            iter = received_data_.erase(iter);
        }
        DCHECK_GT(end_seqnum, index_data_.indexed_seqnum_position());
        index_data_.set_indexed_seqnum_position(end_seqnum);
        uint32_t metalog_seqnum = cuts_.front().first;
        index_data_.set_indexed_metalog_position(metalog_seqnum + 1);
        cuts_.pop_front();
    }
    if (!blocking_reads_.empty()) {
        int64_t current_timestamp = GetMonotonicMicroTimestamp();
        std::vector<std::pair<int64_t, IndexQuery>> unfinished;
        for (const auto& [start_timestamp, query] : blocking_reads_) {
            IndexQueryResult query_result;
            if (query.direction == IndexQuery::kReadLocalId) {
                query_result = index_data_.ProcessLocalIdQuery(query);
            } else {
                query_result = index_data_.ProcessBlockingQuery(query);
            }
            if (query_result.state == IndexQueryResult::State::kPending) {
                if (current_timestamp - start_timestamp
                        < absl::ToInt64Microseconds(kBlockingQueryTimeout)) {
                    unfinished.push_back(std::make_pair(start_timestamp, query));
                } else {
                    pending_query_results_.push_back(index_data_.BuildNotFoundResult(query));
                }
            } else {
                pending_query_results_.push_back(query_result);
            }
        }
        blocking_reads_ = std::move(unfinished);
    }
    auto iter = pending_queries_.begin();
    while (iter != pending_queries_.end()) {
        if (iter->first > index_data_.indexed_metalog_position()) {
            break;
        }
        const IndexQuery& query = iter->second;
        IndexQueryResult result = ProcessQuery(query);
        ProcessQueryResult(result);
        iter = pending_queries_.erase(iter);
    }
}

// Read local index
// FIXME: note that boki currently propagate log indices to all the engine
// nodes, but it can be confitured to partial propagation, in which case
// local query must support remote engine read!! (Skip just for now)
IndexQueryResult Index::ProcessQuery(const IndexQuery& query) {
    HVLOG_F(1, "ProcessQuery: direction={}", query.direction);
    if (query.direction == IndexQuery::kReadLocalId) {
        return index_data_.ProcessLocalIdQuery(query);
    } else if (query.direction == IndexQuery::kReadNextB) {
        return index_data_.ProcessBlockingQuery(query);
    } else if (query.direction == IndexQuery::kReadNext) {
        return index_data_.ProcessReadNext(query);
    } else if (query.direction == IndexQuery::kReadPrev) {
        return index_data_.ProcessReadPrev(query);
    } else {
        UNREACHABLE();
    }
}

void Index::ProcessQueryResult(const IndexQueryResult& result) {
    if (result.state == IndexQueryResult::State::kPending) {
        blocking_reads_.push_back(std::make_pair(GetMonotonicMicroTimestamp(), result.original_query));
    } else {
        pending_query_results_.push_back(result);
    }
}

}  // namespace log
}  // namespace faas
