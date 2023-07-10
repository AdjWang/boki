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
    case protocol::SharedLogOpType::ASYNC_READ_PREV_AUX:
        return IndexQuery::kReadPrevAux;
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
        case IndexQuery::kReadPrevAux:
            return protocol::SharedLogOpType::ASYNC_READ_PREV_AUX;
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

Index::Index(const View* view, uint16_t sequencer_id, std::optional<LRUCache>& cache)
    : LogSpaceBase(LogSpaceBase::kFullMode, view, sequencer_id),
      indexed_metalog_position_(0),
      data_received_seqnum_position_(0),
      indexed_seqnum_position_(0),
      cache_(cache) {
    log_header_ = fmt::format("LogIndex[{}-{}]: ", view->id(), sequencer_id);
    state_ = kNormal;
}

Index::~Index() {}

class Index::PerSpaceIndex {
public:
    PerSpaceIndex(uint32_t logspace_id, uint32_t user_logspace,
                  std::optional<LRUCache>& cache);
    ~PerSpaceIndex() {}

    void Add(uint32_t seqnum_lowhalf, uint16_t engine_id, const UserTagVec& user_tags);

    bool FindPrev(uint64_t query_seqnum, uint64_t user_tag, uint64_t* seqnum,
                  uint16_t* engine_id, size_t* index = nullptr) const;
    bool FindNext(uint64_t query_seqnum, uint64_t user_tag, uint64_t* seqnum,
                  uint16_t* engine_id, size_t* index = nullptr) const;
    bool FindNextAuxData(size_t seqnum_index, uint64_t user_tag,
                         uint64_t* seqnum,
                         /*out*/std::string& aux_data) const;
    bool FindPrevAuxData(size_t seqnum_index, uint64_t user_tag,
                         uint64_t* seqnum,
                         /*out*/std::string& aux_data) const;

    // DEBUG
    std::string Inspect() const;

   private:
    std::string log_header_;
    uint32_t logspace_id_;
    uint32_t user_logspace_;
    std::optional<LRUCache>& cache_;

    absl::flat_hash_map</* seqnum */ uint32_t, uint16_t> engine_ids_;
    std::vector<uint32_t> seqnums_;
    absl::flat_hash_map</* tag */ uint64_t, std::vector<uint32_t>> seqnums_by_tag_;

    bool FindPrev(const std::vector<uint32_t>& seqnums, uint64_t query_seqnum,
                  uint32_t* result_seqnum, size_t* index) const;
    bool FindNext(const std::vector<uint32_t>& seqnums, uint64_t query_seqnum,
                  uint32_t* result_seqnum, size_t* index) const;

    DISALLOW_COPY_AND_ASSIGN(PerSpaceIndex);
};

std::string Index::PerSpaceIndex::Inspect() const {
    std::string output("PerSpaceIndex::Inspact:\n");
    for (const auto& [tag, seqnums] : seqnums_by_tag_) {
        std::stringstream ss;
        for (const uint32_t seqnum : seqnums) {
            ss << fmt::format("0x{:016X}, ", bits::JoinTwo32(logspace_id_, seqnum));
        }
        ss << "\b\b  \b\b";  // remove tailing 2 characters
        if (seqnums.empty()) {
            output.append(fmt::format("tag={}, seqnums=[none]\n", tag));
        } else {
            output.append(fmt::format("tag={}, seqnums=[{}]\n", tag, ss.str()));
        }
    }
    return output;
}

Index::PerSpaceIndex::PerSpaceIndex(uint32_t logspace_id,
                                    uint32_t user_logspace,
                                    std::optional<LRUCache>& cache)
    : log_header_(fmt::format("PerSpaceIndex[{}-{}]: ", logspace_id, user_logspace)),
      logspace_id_(logspace_id),
      user_logspace_(user_logspace),
      cache_(cache) {}

void Index::PerSpaceIndex::Add(uint32_t seqnum_lowhalf, uint16_t engine_id,
                               const UserTagVec& user_tags) {
    DCHECK(!engine_ids_.contains(seqnum_lowhalf));
    engine_ids_[seqnum_lowhalf] = engine_id;
    DCHECK(seqnums_.empty() || seqnum_lowhalf > seqnums_.back());
    seqnums_.push_back(seqnum_lowhalf);
    for (uint64_t user_tag : user_tags) {
        DCHECK_NE(user_tag, kEmptyLogTag);
        seqnums_by_tag_[user_tag].push_back(seqnum_lowhalf);
    }
}

bool Index::PerSpaceIndex::FindPrev(uint64_t query_seqnum, uint64_t user_tag,
                                    uint64_t* seqnum, uint16_t* engine_id,
                                    size_t* index) const {
    uint32_t seqnum_lowhalf;
    if (user_tag == kEmptyLogTag) {
        if (!FindPrev(seqnums_, query_seqnum, &seqnum_lowhalf, index)) {
            return false;
        }
    } else {
        if (!seqnums_by_tag_.contains(user_tag)) {
            return false;
        }
        if (!FindPrev(seqnums_by_tag_.at(user_tag), query_seqnum, &seqnum_lowhalf, index)) {
            return false;
        }
    }
    DCHECK(engine_ids_.contains(seqnum_lowhalf));
    *seqnum = bits::JoinTwo32(logspace_id_, seqnum_lowhalf);
    DCHECK_LE(*seqnum, query_seqnum);
    *engine_id = engine_ids_.at(seqnum_lowhalf);
    return true;
}

bool Index::PerSpaceIndex::FindNext(uint64_t query_seqnum, uint64_t user_tag,
                                    uint64_t* seqnum, uint16_t* engine_id,
                                    size_t* index) const {
    uint32_t seqnum_lowhalf;
    if (user_tag == kEmptyLogTag) {
        if (!FindNext(seqnums_, query_seqnum, &seqnum_lowhalf, index)) {
            return false;
        }
    } else {
        if (!seqnums_by_tag_.contains(user_tag)) {
            return false;
        }
        if (!FindNext(seqnums_by_tag_.at(user_tag), query_seqnum, &seqnum_lowhalf, index)) {
            return false;
        }
    }
    DCHECK(engine_ids_.contains(seqnum_lowhalf));
    *seqnum = bits::JoinTwo32(logspace_id_, seqnum_lowhalf);
    DCHECK_GE(*seqnum, query_seqnum);
    *engine_id = engine_ids_.at(seqnum_lowhalf);
    return true;
}

bool Index::PerSpaceIndex::FindPrevAuxData(size_t seqnum_index, uint64_t user_tag,
                                           uint64_t* seqnum,
                                           /*out*/std::string& aux_data) const {
    if (!cache_.has_value()) {
        return false;
    }
    DCHECK(seqnum != nullptr);
    DCHECK(seqnums_by_tag_.contains(user_tag));
    const std::vector<uint32_t>& seqnums(seqnums_by_tag_.at(user_tag));
    DCHECK(seqnums.size() > 0);
    if (seqnum_index == 0) {
        uint32_t seqnum_lowhalf = seqnums[seqnum_index];
        uint64_t temp_seqnum = bits::JoinTwo32(logspace_id_, seqnum_lowhalf);
        if(auto data = cache_->GetAuxData(temp_seqnum); data.has_value()) {
            HVLOG(1) << "FindPrevWithAuxData iter_count=0";
            *seqnum = temp_seqnum;
            aux_data = data.value();
            return true;
        }
    } else {
        for (size_t i = seqnum_index - 1; i > 0; --i) {
            DCHECK(i < seqnums.size()) << fmt::format("i={}, seqnums.size()={}", i, seqnums.size());
            uint32_t seqnum_lowhalf = seqnums[i];
            uint64_t temp_seqnum = bits::JoinTwo32(logspace_id_, seqnum_lowhalf);
            if(auto data = cache_->GetAuxData(temp_seqnum); data.has_value()) {
                HVLOG_F(1, "FindPrevWithAuxData iter_count={}", seqnum_index - i);
                *seqnum = temp_seqnum;
                aux_data = data.value();
                return true;
            }
        }
    }
    return false;
}

bool Index::PerSpaceIndex::FindNextAuxData(size_t seqnum_index, uint64_t user_tag,
                                           uint64_t* seqnum,
                                           /*out*/std::string& aux_data) const {
    if (!cache_.has_value()) {
        return false;
    }
    const std::vector<uint32_t>& seqnums(seqnums_by_tag_.at(user_tag));
    for (size_t i = seqnum_index; i < seqnums.size(); ++i) {
        uint32_t seqnum_lowhalf = seqnums[i];
        uint64_t temp_seqnum = bits::JoinTwo32(logspace_id_, seqnum_lowhalf);
        if(auto data = cache_->GetAuxData(temp_seqnum); data.has_value()) {
            HVLOG_F(1, "FindNextWithAuxData iter_count={}", i - seqnum_index + 1);
            *seqnum = temp_seqnum;
            aux_data = data.value();
            return true;
        }
    }
    return false;
}

bool Index::PerSpaceIndex::FindPrev(const std::vector<uint32_t>& seqnums,
                                    uint64_t query_seqnum, uint32_t* result_seqnum,
                                    size_t* index) const {
    if (seqnums.empty() || bits::JoinTwo32(logspace_id_, seqnums.front()) > query_seqnum) {
        return false;
    }
    if (query_seqnum == kMaxLogSeqNum) {
        *result_seqnum = seqnums.back();
        if (index != nullptr) {
            *index = seqnums.size() - 1;
        }
        return true;
    }
    auto iter = absl::c_upper_bound(
        seqnums, query_seqnum,
        [logspace_id = logspace_id_] (uint64_t lhs, uint32_t rhs) {
            return lhs < bits::JoinTwo32(logspace_id, rhs);
        }
    );
    if (iter == seqnums.begin()) {
        return false;
    } else {
        --iter;
        *result_seqnum = *iter;
        if (index != nullptr) {
            *index = static_cast<size_t>(iter - seqnums.begin());
        }
        return true;
    }
}

bool Index::PerSpaceIndex::FindNext(const std::vector<uint32_t>& seqnums,
                                    uint64_t query_seqnum, uint32_t* result_seqnum,
                                    size_t* index) const {
    if (seqnums.empty() || bits::JoinTwo32(logspace_id_, seqnums.back()) < query_seqnum) {
        return false;
    }
    auto iter = absl::c_lower_bound(
        seqnums, query_seqnum,
        [logspace_id = logspace_id_] (uint32_t lhs, uint64_t rhs) {
            return bits::JoinTwo32(logspace_id, lhs) < rhs;
        }
    );
    if (iter == seqnums.end()) {
        return false;
    } else {
        *result_seqnum = *iter;
        if (index != nullptr) {
            *index = static_cast<size_t>(iter - seqnums.begin());
        }
        return true;
    }
}

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
        if (seqnum < indexed_seqnum_position_) {
            tag_iter += num_tags;
            continue;
        }
        if (received_data_.count(seqnum) == 0) {
            received_data_[seqnum] = IndexData {
                .local_id      = index_data.local_ids(i),
                .user_logspace = index_data.user_logspaces(i),
                .user_tags     = UserTagVec(tag_iter, tag_iter + num_tags)
            };
        } else {
#if DCHECK_IS_ON()
            const IndexData& data = received_data_[seqnum];
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
    if (query.initial) {
        HVLOG(1) << "Receive initial query";
        uint16_t view_id = log_utils::GetViewId(query.metalog_progress);
        if (view_id > view_->id()) {
            HLOG_F(FATAL, "Cannot process query with metalog_progress from the future: "
                          "metalog_progress={}, my_view_id={}",
                   bits::HexStr0x(query.metalog_progress), bits::HexStr0x(view_->id()));
        } else if (view_id < view_->id()) {
            HVLOG_F(1, "MakeQuery Process query type=0x{:02X} seqnum=0x{:016X} "
                       "since pending_query viewid={} smaller than current viewid={}",
                    uint16_t(query.type), query.query_seqnum, view_id, view_->id());
            ProcessQuery(query);
        } else {
            DCHECK_EQ(view_id, view_->id());
            uint32_t position = bits::LowHalf64(query.metalog_progress);
            if (position <= indexed_metalog_position_) {
                HVLOG_F(1, "MakeQuery Process query type=0x{:02X} seqnum=0x{:016X} "
                           "since pending_query metalog_position={} not larger than indexed_metalog_position={}",
                        uint16_t(query.type), query.query_seqnum, position, indexed_metalog_position_);
                ProcessQuery(query);
            } else {
                pending_queries_.insert(std::make_pair(position, query));
            }
        }
    } else {
        HVLOG(1) << "Receive continue query";
        if (finalized()) {
            HVLOG_F(1, "MakeQuery Process query type=0x{:02X} seqnum=0x{:016X} since finalized",
                    uint16_t(query.type), query.query_seqnum);
            ProcessQuery(query);
        } else {
            pending_queries_.insert(std::make_pair(kMaxMetalogPosition, query));
        }
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
        HVLOG_F(1, "Apply IndexData until seqnum {}", bits::HexStr0x(end_seqnum));
        auto iter = received_data_.begin();
        while (iter != received_data_.end()) {
            uint32_t seqnum = iter->first;
            if (seqnum >= end_seqnum) {
                break;
            }
            const IndexData& index_data = iter->second;
            // update index (globab view)
            uint16_t engine_id = gsl::narrow_cast<uint16_t>(
                bits::HighHalf64(index_data.local_id));
            GetOrCreateIndex(index_data.user_logspace)->Add(
                seqnum, engine_id, index_data.user_tags);
            // update index map, to serve async log query
            DCHECK(log_index_map_.find(index_data.local_id) == log_index_map_.end())
                << "Duplicate index_data.local_id for log_index_map_";
            log_index_map_[index_data.local_id] = AsyncIndexData{
                .seqnum = bits::JoinTwo32(identifier(), seqnum),
                .user_tags = index_data.user_tags,
            };

            iter = received_data_.erase(iter);
        }
        DCHECK_GT(end_seqnum, indexed_seqnum_position_);
        indexed_seqnum_position_ = end_seqnum;
        uint32_t metalog_seqnum = cuts_.front().first;
        indexed_metalog_position_ = metalog_seqnum + 1;
        cuts_.pop_front();
    }
    if (!blocking_reads_.empty()) {
        int64_t current_timestamp = GetMonotonicMicroTimestamp();
        std::vector<std::pair<int64_t, IndexQuery>> unfinished;
        for (const auto& [start_timestamp, query] : blocking_reads_) {
            bool query_result;
            if (query.direction == IndexQuery::kReadLocalId) {
                query_result = ProcessLocalIdQuery(query);
            } else {
                query_result = ProcessBlockingQuery(query);
            }
            if (!query_result) {
                if (current_timestamp - start_timestamp
                        < absl::ToInt64Microseconds(kBlockingQueryTimeout)) {
                    unfinished.push_back(std::make_pair(start_timestamp, query));
                } else {
                    pending_query_results_.push_back(BuildNotFoundResult(query));
                }
            }
        }
        blocking_reads_ = std::move(unfinished);
    }
    auto iter = pending_queries_.begin();
    while (iter != pending_queries_.end()) {
        if (iter->first > indexed_metalog_position_) {
            break;
        }
        const IndexQuery& query = iter->second;
        HVLOG_F(1, "AdvanceIndexProgress Process query type=0x{:02X} seqnum=0x{:016X} \
since pending_query metalog_position={} not larger than indexed_metalog_position={}",
                uint16_t(query.type), query.query_seqnum, iter->first, indexed_metalog_position_);
        ProcessQuery(query);
        iter = pending_queries_.erase(iter);
    }
}

Index::PerSpaceIndex* Index::GetOrCreateIndex(uint32_t user_logspace) {
    if (index_.contains(user_logspace)) {
        return index_.at(user_logspace).get();
    }
    HVLOG_F(1, "Create index of user logspace {}", user_logspace);
    PerSpaceIndex* index = new PerSpaceIndex(identifier(), user_logspace, cache_);
    index_[user_logspace].reset(index);
    return index;
}

// Read local index
// FIXME: note that boki currently propagate log indices to all the engine
// nodes, but it can be confitured to partial propagation, in which case
// local query must support remote engine read!! (Skip just for now)
bool Index::ProcessLocalIdQuery(const IndexQuery& query) {
    DCHECK(query.direction == IndexQuery::kReadLocalId);
    // Local id is propagated by user atop function arguments, it may faster
    // than metalog_position_ propagation. In one user session, previously
    // appended async log's local id must be always perceived as readable to
    // gaurantee read-your-write. So local query is promised to be found, even
    // metalog_position_ is old.

    // replace seqnum if querying by localid
    uint64_t local_id = query.query_seqnum;
    if (log_index_map_.find(local_id) == log_index_map_.end()) {
        // not found
        // HVLOG_F(1, "pending ProcessQuery: NotFoundResult due to log_index_map_ not indexed local_id: 0x{:016X}",
        //         local_id);
        return false;
    } else {
        // found
        AsyncIndexData index_data = log_index_map_[local_id];
        uint64_t seqnum = index_data.seqnum;
        std::optional<std::string> aux_data;
        if (cache_.has_value()) {
            aux_data = cache_->GetAuxData(seqnum);
        } else {
            aux_data = std::nullopt;
        }
        // HVLOG_F(1, "ProcessQuery: found async map from local_id=0x{:016X} to seqnum=0x{:016X}",
        //         local_id, seqnum);
        // BUG: WOW! A reaaaaaaaaly strange bug! Triggered so many times with the magic result 28524,
        // causing functions failed to send back the query result to the target engine node.
        DCHECK(query.origin_node_id != 28524) << debug::DumpStackTrace();

        uint16_t engine_id = gsl::narrow_cast<uint16_t>(bits::HighHalf64(local_id));
        auto result = BuildFoundResult(query, view_->id(), seqnum, engine_id, aux_data);
        pending_query_results_.push_back(result);
        return true;
    }
}

void Index::ProcessQuery(const IndexQuery& query) {
    HVLOG_F(1, "ProcessQuery: direction={}", query.direction);
    if (query.direction == IndexQuery::kReadLocalId) {
        bool success = ProcessLocalIdQuery(query);
        if (!success) {
            blocking_reads_.push_back(std::make_pair(GetMonotonicMicroTimestamp(), query));
        }
    } else if (query.direction == IndexQuery::kReadNextB) {
        bool success = ProcessBlockingQuery(query);
        if (!success) {
            blocking_reads_.push_back(std::make_pair(GetMonotonicMicroTimestamp(), query));
        }
    } else if (query.direction == IndexQuery::kReadNext) {
        ProcessReadNext(query);
    } else if (query.direction == IndexQuery::kReadPrev || query.direction == IndexQuery::kReadPrevAux) {
        ProcessReadPrev(query);
    } else {
        UNREACHABLE();
    }
}

void Index::ProcessReadNext(const IndexQuery& query) {
    DCHECK(query.direction == IndexQuery::kReadNext);
    HVLOG_F(1, "ProcessReadNext: seqnum={}, logspace={}, tag={}",
            bits::HexStr0x(query.query_seqnum), query.user_logspace, query.user_tag);
    uint16_t query_view_id = log_utils::GetViewId(query.query_seqnum);
    if (query_view_id > view_->id()) {
        pending_query_results_.push_back(BuildNotFoundResult(query));
        HVLOG(1) << "ProcessReadNext: NotFoundResult";
        return;
    }
    uint64_t seqnum;
    uint16_t engine_id;
    std::optional<std::string> aux_data;
    bool found = IndexFindNext(query, &seqnum, &engine_id, /*out*/aux_data);
    if (query_view_id == view_->id()) {
        if (found) {
            pending_query_results_.push_back(
                BuildFoundResult(query, view_->id(), seqnum, engine_id, aux_data));
            HVLOG_F(1, "ProcessReadNext: FoundResult: seqnum=0x{:016X}", seqnum);
        } else {
            if (query.prev_found_result.seqnum != kInvalidLogSeqNum) {
                const IndexFoundResult& found_result = query.prev_found_result;
                pending_query_results_.push_back(
                    BuildFoundResult(query, found_result.view_id,
                                     found_result.seqnum, found_result.engine_id, found_result.aux_data));
                HVLOG_F(1, "ProcessReadNext: FoundResult (from prev_result): seqnum=0x{:016X}",
                        found_result.seqnum);
            } else {
                pending_query_results_.push_back(BuildNotFoundResult(query));
                HVLOG(1) << "ProcessReadNext: NotFoundResult" << GetOrCreateIndex(query.user_logspace)->Inspect();
            }
        }
    } else {
        pending_query_results_.push_back(
            BuildContinueResult(query, found, seqnum, engine_id, aux_data));
        HVLOG(1) << "ProcessReadNext: ContinueResult";
    }
}

void Index::ProcessReadPrev(const IndexQuery& query) {
    DCHECK(query.direction == IndexQuery::kReadPrev
            || query.direction == IndexQuery::kReadPrevAux);
    HVLOG_F(1, "ProcessReadPrev: seqnum={}, logspace={}, tag={}",
            bits::HexStr0x(query.query_seqnum), query.user_logspace, query.user_tag);
    uint16_t query_view_id = log_utils::GetViewId(query.query_seqnum);
    if (query_view_id < view_->id()) {
        pending_query_results_.push_back(BuildContinueResult(query, false, 0, 0, std::nullopt));
        HVLOG(1) << "ProcessReadPrev: ContinueResult";
        return;
    }
    uint64_t seqnum;
    uint16_t engine_id;
    std::optional<std::string> aux_data;
    bool found = IndexFindPrev(query, &seqnum, &engine_id, /*out*/aux_data);
    if (found) {
        pending_query_results_.push_back(
            BuildFoundResult(query, view_->id(), seqnum, engine_id, aux_data));
        HVLOG_F(1, "ProcessReadPrev: FoundResult: seqnum={:016X}", seqnum);
    } else if (view_->id() > 0) {
        pending_query_results_.push_back(BuildContinueResult(query, false, 0, 0, std::nullopt));
        HVLOG(1) << "ProcessReadPrev: ContinueResult";
    } else {
        pending_query_results_.push_back(BuildNotFoundResult(query));
        HVLOG(1) << "ProcessReadPrev: NotFoundResult";
    }
}

bool Index::ProcessBlockingQuery(const IndexQuery& query) {
    DCHECK(query.direction == IndexQuery::kReadNextB && query.initial);
    uint16_t query_view_id = log_utils::GetViewId(query.query_seqnum);
    if (query_view_id > view_->id()) {
        pending_query_results_.push_back(BuildNotFoundResult(query));
        return true;
    }
    uint64_t seqnum;
    uint16_t engine_id;
    std::optional<std::string> aux_data;
    bool found = IndexFindNext(query, &seqnum, &engine_id, /*out*/aux_data);
    if (query_view_id == view_->id()) {
        if (found) {
            pending_query_results_.push_back(
                BuildFoundResult(query, view_->id(), seqnum, engine_id, aux_data));
        }
        return found;
    } else {
        pending_query_results_.push_back(
            BuildContinueResult(query, found, seqnum, engine_id, aux_data));
        return true;
    }
}

bool Index::IndexFindNext(const IndexQuery& query, uint64_t* seqnum,
                          uint16_t* engine_id,
                          /*out*/ std::optional<std::string>& aux_data) {
    DCHECK(query.direction == IndexQuery::kReadNext
            || query.direction == IndexQuery::kReadNextB);
    if (!index_.contains(query.user_logspace)) {
        return false;
    }
    if (GetOrCreateIndex(query.user_logspace)->FindNext(
        query.query_seqnum, query.user_tag, seqnum, engine_id)) {
        if (cache_.has_value()) {
            aux_data = cache_->GetAuxData(query.query_seqnum);
        }
        return true;
    } else {
        return false;
    }
}

bool Index::IndexFindPrev(const IndexQuery& query, uint64_t* seqnum,
                          uint16_t* engine_id,
                          /*out*/ std::optional<std::string>& aux_data) {
    DCHECK(query.direction == IndexQuery::kReadPrev
            || query.direction == IndexQuery::kReadPrevAux);
    if (!index_.contains(query.user_logspace)) {
        return false;
    }
    Index::PerSpaceIndex* per_space_index = GetOrCreateIndex(query.user_logspace);
    DCHECK(per_space_index != nullptr);
    size_t index;
    if (query.direction == IndexQuery::kReadPrev) {
        if (per_space_index->FindPrev(query.query_seqnum, query.user_tag,
                                       seqnum, engine_id, &index)) {
            if (cache_.has_value()) {
                aux_data = cache_->GetAuxData(*seqnum);
                HVLOG_F(1, "IndexFindPrev GetAuxData seqnum=0x{:016X} got={}",
                           query.query_seqnum, aux_data.has_value());
            }
            return true;
        } else {
            return false;
        }
    } else {
        if (per_space_index->FindPrev(query.query_seqnum, query.user_tag,
                                       seqnum, engine_id, &index)) {
            std::string temp_aux_data;
            if (per_space_index->FindPrevAuxData(index, query.user_tag,
                                                 seqnum, /*out*/ temp_aux_data)) {
                aux_data = temp_aux_data;
                HVLOG_F(1, "IndexFindPrev FindPrevAuxData seqnum=0x{:016X} got={}",
                           *seqnum, aux_data.has_value());
                return true;
            } else {
                aux_data = std::nullopt;
                return false;
            }
        } else {
            return false;
        }
    }
}

IndexQueryResult Index::BuildNotFoundResult(const IndexQuery& query) {
    return IndexQueryResult {
        .state = IndexQueryResult::kEmpty,
        .metalog_progress = query.initial ? index_metalog_progress()
                                          : query.metalog_progress,
        .next_view_id = 0,
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = 0,
            .engine_id = 0,
            .seqnum = kInvalidLogSeqNum,
            .aux_data = std::nullopt
        },
    };
}

IndexQueryResult Index::BuildFoundResult(
    const IndexQuery& query, uint16_t view_id, uint64_t seqnum,
    uint16_t engine_id, const std::optional<std::string>& aux_data) {
    return IndexQueryResult {
        .state = IndexQueryResult::kFound,
        .metalog_progress = query.initial ? index_metalog_progress()
                                          : query.metalog_progress,
        .next_view_id = 0,
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = view_id,
            .engine_id = engine_id,
            .seqnum = seqnum,
            .aux_data = aux_data
        },
    };
}
IndexQueryResult Index::BuildFoundResult(
    const IndexQuery& query, uint16_t view_id, uint64_t seqnum,
    uint16_t engine_id, const std::optional<std::string>&& aux_data) {
    return BuildFoundResult(query, view_id, seqnum, engine_id, aux_data);
}

IndexQueryResult Index::BuildContinueResult(
    const IndexQuery& query, bool found, uint64_t seqnum, uint16_t engine_id,
    const std::optional<std::string>& aux_data) {
    DCHECK(view_->id() > 0);
    IndexQueryResult result = {
        .state = IndexQueryResult::kContinue,
        .metalog_progress = query.initial ? index_metalog_progress()
                                          : query.metalog_progress,
        .next_view_id = gsl::narrow_cast<uint16_t>(view_->id() - 1),
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = 0,
            .engine_id = 0,
            .seqnum = kInvalidLogSeqNum,
            .aux_data = std::nullopt
        },
    };
    if (query.direction == IndexQuery::kReadNextB) {
        result.original_query.direction = IndexQuery::kReadNext;
    }
    if (!query.initial) {
        result.found_result = query.prev_found_result;
    }
    if (found) {
        result.found_result = IndexFoundResult {
            .view_id = view_->id(),
            .engine_id = engine_id,
            .seqnum = seqnum,
            .aux_data = aux_data
        };
    } else if (!query.initial && query.prev_found_result.seqnum != kInvalidLogSeqNum) {
        result.found_result = query.prev_found_result;
    }
    return result;
}
IndexQueryResult Index::BuildContinueResult(
    const IndexQuery& query, bool found, uint64_t seqnum, uint16_t engine_id,
    const std::optional<std::string>&& aux_data) {
    return BuildContinueResult(query, found, seqnum, engine_id, aux_data);
}

}  // namespace log
}  // namespace faas
