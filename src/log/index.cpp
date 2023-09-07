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
    case protocol::SharedLogOpType::READ_SYNCTO:
        return IndexQuery::kReadNextU;
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
        case IndexQuery::kReadNextU:
            return protocol::SharedLogOpType::READ_SYNCTO;
        default:
            UNREACHABLE();
        }
    }
}

Index::Index(const View* view, uint16_t sequencer_id)
    : LogSpaceBase(LogSpaceBase::kFullMode, view, sequencer_id),
      indexed_metalog_position_(0),
      data_received_seqnum_position_(0),
      indexed_seqnum_position_(0) {
    log_header_ = fmt::format("LogIndex[{}-{}]: ", view->id(), sequencer_id);
    state_ = kNormal;
}

Index::~Index() {}

class Index::PerSpaceIndex {
public:
    PerSpaceIndex(uint32_t logspace_id, uint32_t user_logspace);
    ~PerSpaceIndex() {}

    void Add(uint32_t seqnum_lowhalf, uint16_t engine_id,
             uint32_t localid_lowhalf, const UserTagVec& user_tags);

    bool ContainsTag(uint64_t user_tag) const;
    bool FindPrev(uint64_t query_seqnum, uint64_t user_tag, uint64_t* seqnum,
                  uint16_t* engine_id, uint64_t* localid,
                  size_t* index = nullptr) const;
    bool FindNext(uint64_t query_seqnum, uint64_t user_tag, uint64_t* seqnum,
                  uint16_t* engine_id, uint64_t* localid,
                  size_t* index = nullptr) const;
    // both side of the range is closed: [start_index, end_index]
    // if end_index exceeds max_limit, auto set it to the end.
    size_t GetRange(uint64_t start_seqnum, uint64_t end_seqnum, uint64_t user_tag,
            std::function<void(uint64_t /*seqnum*/, uint16_t /*engine_id*/, uint64_t /*localid*/)> cb);
    size_t GetRangeByIndex(size_t start_index, size_t end_index, uint64_t user_tag,
            std::function<bool(size_t index, uint64_t /*seqnum*/,
                               uint16_t /*engine_id*/, uint64_t /*localid*/)> cb);

   private:
    uint32_t logspace_id_;
    uint32_t user_logspace_;

    absl::flat_hash_map</* seqnum */ uint32_t, std::pair<uint16_t, uint32_t>> log_ids_;
    std::vector<uint32_t> seqnums_;
    absl::flat_hash_map</* tag */ uint64_t, std::vector<uint32_t>> seqnums_by_tag_;

    bool FindPrev(const std::vector<uint32_t>& seqnums, uint64_t query_seqnum,
                  uint32_t* result_seqnum, size_t* index=nullptr) const;
    bool FindNext(const std::vector<uint32_t>& seqnums, uint64_t query_seqnum,
                  uint32_t* result_seqnum, size_t* index=nullptr) const;

    DISALLOW_COPY_AND_ASSIGN(PerSpaceIndex);
};

Index::PerSpaceIndex::PerSpaceIndex(uint32_t logspace_id, uint32_t user_logspace)
    : logspace_id_(logspace_id),
      user_logspace_(user_logspace) {}

void Index::PerSpaceIndex::Add(uint32_t seqnum_lowhalf, uint16_t engine_id,
                               uint32_t localid_lowhalf, const UserTagVec& user_tags) {
    DCHECK(!log_ids_.contains(seqnum_lowhalf));
    log_ids_[seqnum_lowhalf] = std::make_pair(engine_id, localid_lowhalf);
    DCHECK(seqnums_.empty() || seqnum_lowhalf > seqnums_.back());
    seqnums_.push_back(seqnum_lowhalf);
    for (uint64_t user_tag : user_tags) {
        DCHECK_NE(user_tag, kEmptyLogTag);
        seqnums_by_tag_[user_tag].push_back(seqnum_lowhalf);
    }
}

bool Index::PerSpaceIndex::ContainsTag(uint64_t user_tag) const {
    return user_tag == kEmptyLogTag || seqnums_by_tag_.contains(user_tag);
}

bool Index::PerSpaceIndex::FindPrev(uint64_t query_seqnum, uint64_t user_tag,
                                    uint64_t* seqnum, uint16_t* engine_id,
                                    uint64_t* localid, size_t* index) const {
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
    DCHECK(log_ids_.contains(seqnum_lowhalf));
    *seqnum = bits::JoinTwo32(logspace_id_, seqnum_lowhalf);
    DCHECK_LE(*seqnum, query_seqnum);
    *engine_id = log_ids_.at(seqnum_lowhalf).first;
    // TODO: make high 16bit as view id
    *localid = bits::JoinTwo32(bits::JoinTwo16(0, *engine_id),
                               log_ids_.at(seqnum_lowhalf).second);
    return true;
}

bool Index::PerSpaceIndex::FindNext(uint64_t query_seqnum, uint64_t user_tag,
                                    uint64_t* seqnum, uint16_t* engine_id,
                                    uint64_t* localid, size_t* index) const {
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
    DCHECK(log_ids_.contains(seqnum_lowhalf));
    *seqnum = bits::JoinTwo32(logspace_id_, seqnum_lowhalf);
    DCHECK_GE(*seqnum, query_seqnum);
    *engine_id = log_ids_.at(seqnum_lowhalf).first;
    // TODO: make high 16bit as view id
    *localid = bits::JoinTwo32(bits::JoinTwo16(0, *engine_id),
                               log_ids_.at(seqnum_lowhalf).second);
    return true;
}

size_t Index::PerSpaceIndex::GetRange(uint64_t start_seqnum, uint64_t end_seqnum, uint64_t user_tag,
        std::function<void(uint64_t /*seqnum*/, uint16_t /*engine_id*/, uint64_t /*localid*/)> cb) {
    if (start_seqnum > end_seqnum) {
        return 0;
    }
    std::vector<uint32_t>* searching_seqnums;
    if (user_tag == kEmptyLogTag) {
        searching_seqnums = &seqnums_;
    } else {
        if (!seqnums_by_tag_.contains(user_tag)) {
            return 0;
        }
        searching_seqnums = &seqnums_by_tag_.at(user_tag);
    }
    // find left endpoint
    uint32_t l_seqnum_lowhalf;
    size_t l_index;
    if (!FindNext(*searching_seqnums, start_seqnum, &l_seqnum_lowhalf, &l_index)) {
        return 0;
    }
    DCHECK(log_ids_.contains(l_seqnum_lowhalf));
    DCHECK_GE(bits::JoinTwo32(logspace_id_, l_seqnum_lowhalf), start_seqnum);
    // find right endpoint
    uint32_t r_seqnum_lowhalf;
    size_t r_index;
    if (!FindPrev(*searching_seqnums, end_seqnum, &r_seqnum_lowhalf, &r_index)) {
        return 0;
    }
    DCHECK(log_ids_.contains(r_seqnum_lowhalf));
    DCHECK_LE(bits::JoinTwo32(logspace_id_, r_seqnum_lowhalf), end_seqnum);
    // make results
    if (l_seqnum_lowhalf > r_seqnum_lowhalf) {
        return 0;
    } else {
        DCHECK(r_index != std::numeric_limits<size_t>::max());
        for (size_t i = l_index; i <= r_index; i++) {
            uint32_t seqnum_lowhalf = (*searching_seqnums)[i];
            uint64_t seqnum = bits::JoinTwo32(logspace_id_, seqnum_lowhalf);
            uint16_t engine_id = log_ids_.at(seqnum_lowhalf).first;
            // TODO: make high 16bit as view id
            uint64_t localid = bits::JoinTwo32(bits::JoinTwo16(0, engine_id),
                                    log_ids_.at(seqnum_lowhalf).second);
            cb(seqnum, engine_id, localid);
        }
        return r_index - l_index + 1;
    }
}

size_t Index::PerSpaceIndex::GetRangeByIndex(size_t start_index, size_t end_index, uint64_t user_tag,
        std::function<bool(size_t index, uint64_t /*seqnum*/, uint16_t /*engine_id*/, uint64_t /*localid*/)> cb) {
    if (start_index > end_index) {
        return 0;
    }
    std::vector<uint32_t>* searching_seqnums;
    if (user_tag == kEmptyLogTag) {
        searching_seqnums = &seqnums_;
    } else {
        if (!seqnums_by_tag_.contains(user_tag)) {
            return 0;
        }
        searching_seqnums = &seqnums_by_tag_.at(user_tag);
    }
    if (start_index >= (*searching_seqnums).size()) {
        return 0;
    }
    if (end_index >= (*searching_seqnums).size()) {
        end_index = (*searching_seqnums).size() - 1;
    }
    for (size_t i = start_index; i <= end_index; i++) {
        uint32_t seqnum_lowhalf = (*searching_seqnums)[i];
        uint64_t seqnum = bits::JoinTwo32(logspace_id_, seqnum_lowhalf);
        uint16_t engine_id = log_ids_.at(seqnum_lowhalf).first;
        // TODO: make high 16bit as view id
        uint64_t localid = bits::JoinTwo32(bits::JoinTwo16(0, engine_id),
                                log_ids_.at(seqnum_lowhalf).second);
        bool stop = cb(i, seqnum, engine_id, localid);
        if (stop) {
            return i - start_index + 1;
        }
    }
    return end_index - start_index + 1;
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
                .localid       = index_data.local_ids(i),
                .user_logspace = index_data.user_logspaces(i),
                .user_tags     = UserTagVec(tag_iter, tag_iter + num_tags)
            };
        } else {
#if DCHECK_IS_ON()
            const IndexData& data = received_data_[seqnum];
            DCHECK_EQ(data.localid, index_data.local_ids(i));
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
            HVLOG_F(1, "MakeQuery Process query type={:02X} seqnum={:016X} "
                        "since pending_query viewid={} smaller than current viewid={}",
                    uint16_t(query.type), query.query_seqnum, view_id, view_->id());
            ProcessQuery(query);
        } else {
            DCHECK_EQ(view_id, view_->id());
            uint32_t position = bits::LowHalf64(query.metalog_progress);
            if (position <= indexed_metalog_position_) {
                HVLOG_F(1, "MakeQuery Process query type={:02X} seqnum={:016X} "
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
            HVLOG_F(1, "MakeQuery Process query type={:02X} seqnum={:016X} since finalized",
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
    bool index_updated = false;
    while (!cuts_.empty()) {
        uint32_t end_seqnum = cuts_.front().second;
        if (data_received_seqnum_position_ < end_seqnum) {
            break;
        }
        HVLOG_F(1, "Apply IndexData until seqnum={:08X}", end_seqnum);
        auto iter = received_data_.begin();
        while (iter != received_data_.end()) {
            uint32_t seqnum = iter->first;
            if (seqnum >= end_seqnum) {
                break;
            }
            const IndexData& index_data = iter->second;
            // update index (globab view)
            uint16_t engine_id = gsl::narrow_cast<uint16_t>(
                bits::HighHalf64(index_data.localid));
            uint32_t localid_lowhalf = bits::LowHalf64(index_data.localid);
            GetOrCreateIndex(index_data.user_logspace)->Add(
                seqnum, engine_id, localid_lowhalf, index_data.user_tags);
            // update index map, to serve async log query
            DCHECK(!log_index_map_.contains(index_data.localid))
                << "Duplicate index_data.localid for log_index_map_";
            uint64_t full_seqnum = bits::JoinTwo32(identifier(), seqnum);
            log_index_map_[index_data.localid] = full_seqnum;
            // DEBUG
            HVLOG_F(1, "AdvanceIndexProgress apply localid={:016X} seqnum={:016X} tags={}",
                        index_data.localid, full_seqnum, log_utils::TagsToString(index_data.user_tags));
            index_updated = true;
            iter = received_data_.erase(iter);
        }
        DCHECK_GT(end_seqnum, indexed_seqnum_position_);
        indexed_seqnum_position_ = end_seqnum;
        uint32_t metalog_seqnum = cuts_.front().first;
        indexed_metalog_position_ = metalog_seqnum + 1;
        cuts_.pop_front();
    }
    // handle blocking reads
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
                    HVLOG_F(1, "AdvanceIndexProgress query timeout dir={} target={:016X}",
                                query.direction, query.query_seqnum);
                }
            }
        }
        blocking_reads_ = std::move(unfinished);
    }
    // handle consistent read by metalog
    auto iter = pending_queries_.begin();
    while (iter != pending_queries_.end()) {
        if (iter->first > indexed_metalog_position_) {
            break;
        }
        const IndexQuery& query = iter->second;
        HVLOG_F(1, "AdvanceIndexProgress Process query type={:02X} seqnum={:016X} "
                    "since pending_query metalog_position={} not larger than indexed_metalog_position={}",
                uint16_t(query.type), query.query_seqnum, iter->first, indexed_metalog_position_);
        ProcessQuery(query);
        iter = pending_queries_.erase(iter);
    }
    // handle pending syncto queries
    if (index_updated && !pending_syncto_queries_.empty()) {
        // pending_syncto_queries_ is modified in ProcessQuery, so use another box
        std::multimap</*localid*/ uint64_t, IndexQuery> temp(std::move(pending_syncto_queries_));
        pending_syncto_queries_.clear();
        auto iter = temp.begin();
        while (iter != temp.end()) {
            if (log_index_map_.contains(iter->first)) {
                ProcessQuery(iter->second);
                iter = temp.erase(iter);
            } else {
                ++iter;
            }
        }
        // push back unfinished
        pending_syncto_queries_.merge(temp);
    }
}

Index::PerSpaceIndex* Index::GetOrCreateIndex(uint32_t user_logspace) {
    if (index_.contains(user_logspace)) {
        return index_.at(user_logspace).get();
    }
    HVLOG_F(1, "Create index of user logspace {}", user_logspace);
    PerSpaceIndex* index = new PerSpaceIndex(identifier(), user_logspace);
    index_[user_logspace].reset(index);
    return index;
}

// Read local index
// FIXME: note that boki currently propagate log indices to all the engine
// nodes, but it can be confitured to partial propagation, in which case
// local query must support remote engine read!! (Skip just for now)
bool Index::ProcessLocalIdQuery(const IndexQuery& query) {
    DCHECK(query.direction == IndexQuery::kReadLocalId);
    DCHECK((query.flags & IndexQuery::kReadLocalIdFlag) != 0);
    // Local id is propagated by user atop function arguments, it may faster
    // than metalog_position_ propagation. In one user session, previously
    // appended async log's local id must be always perceived as readable to
    // gaurantee read-your-write. So local query is promised to be found, even
    // metalog_position_ is old.

    uint64_t localid = query.query_localid;
    DCHECK_NE(localid, protocol::kInvalidLogLocalId);
    const auto& it = log_index_map_.find(localid);
    if (it == log_index_map_.end()) {
        // not found
        // HVLOG_F(1, "pending ProcessQuery: NotFoundResult due to log_index_map_ not indexed localid={:016X}",
        //         localid);
        return false;
    } else {
        // found
        uint64_t seqnum = it->second;
        // HVLOG_F(1, "ProcessQuery: found async map from localid={:016X} to seqnum={:016X}",
        //         localid, seqnum);
        // BUG: WOW! A reaaaaaaaaly strange bug! Triggered so many times with the magic result 28524,
        // causing functions failed to send back the query result to the target engine node.
        DCHECK(query.origin_node_id != 28524) << utils::DumpStackTrace();

        DCHECK_NE(seqnum, protocol::kInvalidLogSeqNum);
        uint16_t engine_id = gsl::narrow_cast<uint16_t>(bits::HighHalf64(localid));
        auto result = BuildFoundResult(query, view_->id(), seqnum, engine_id, localid);
        pending_query_results_.push_back(result);
        return true;
    }
}

void Index::ProcessQuery(const IndexQuery& query) {
    HVLOG_F(1, "ProcessQuery: direction={}, flags={:02X}", query.direction, query.flags);
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
    } else if (query.direction == IndexQuery::kReadNextU) {
        ProcessReadNextUntil(query);
    } else if (query.direction == IndexQuery::kReadPrev ||
               query.direction == IndexQuery::kReadPrevAux) {
        ProcessReadPrev(query);
    } else {
        UNREACHABLE();
    }
}

void Index::ProcessReadNext(const IndexQuery& query) {
    DCHECK(query.direction == IndexQuery::kReadNext);
    DCHECK((query.flags & IndexQuery::kReadLocalIdFlag) == 0);
    HVLOG_F(1, "ProcessReadNext: seqnum={:016X}, logspace={}, tag={}",
            query.query_seqnum, query.user_logspace, query.user_tag);
    uint16_t query_view_id = log_utils::GetViewId(query.query_seqnum);
    if (query_view_id > view_->id()) {
        pending_query_results_.push_back(BuildNotFoundResult(query));
        HVLOG(1) << "ProcessReadNext: NotFoundResult";
        return;
    }
    uint64_t seqnum;
    uint16_t engine_id;
    uint64_t localid;
    bool found = IndexFindNext(query, &seqnum, &engine_id, &localid);
    if (query_view_id == view_->id()) {
        if (found) {
            pending_query_results_.push_back(
                BuildFoundResult(query, view_->id(), seqnum, engine_id, localid));
            HVLOG_F(1, "ProcessReadNext: FoundResult: seqnum={:016X}", seqnum);
        } else {
            if (query.prev_found_result.seqnum != kInvalidLogSeqNum) {
                const IndexFoundResult& found_result = query.prev_found_result;
                pending_query_results_.push_back(BuildFoundResult(
                    query, found_result.view_id, found_result.seqnum,
                    found_result.engine_id, found_result.localid));
                HVLOG_F(1, "ProcessReadNext: FoundResult (from prev_result): seqnum={:016X}",
                        found_result.seqnum);
            } else {
                pending_query_results_.push_back(BuildNotFoundResult(query));
                HVLOG(1) << "ProcessReadNext: NotFoundResult";
            }
        }
    } else {
        pending_query_results_.push_back(
            BuildViewContinueResult(query, found, seqnum, engine_id, localid));
        HVLOG(1) << "ProcessReadNext: ContinueResult";
    }
}

void Index::ProcessReadNextUntilInitial(const IndexQuery& query) {
    DCHECK(query.direction == IndexQuery::kReadNextU);
    DCHECK(query.initial);
    uint64_t result_id = query.next_result_id;
    // TODO: remove this if check after integrating view to localid
    if ((query.flags & IndexQuery::kReadLocalIdFlag) == 0) {
        uint16_t query_view_id = log_utils::GetViewId(query.query_seqnum);
        if (query_view_id < view_->id()) {
            pending_query_results_.push_back(BuildViewContinueResult(query, false, 0, 0, 0, result_id));
            HVLOG(1) << "ProcessReadNextU: ViewContinueResult";
            return;
        }
    }
    uint64_t tag = query.user_tag;
    uint64_t syncto_seqnum; // syncto not including self
    bool sync_continue;
    if ((query.flags & IndexQuery::kReadLocalIdFlag) == 0) {
        // query use seqnum
        syncto_seqnum = query.query_seqnum;
        if (syncto_seqnum == 0) {
            pending_query_results_.push_back(BuildResolvedResult(query, result_id));
            HVLOG(1) << "ProcessReadNextU: ResolvedResult no preceding logs need to be synced";
            return;
        }
        if ((query.flags & IndexQuery::kReadFromCachedFlag) == 0) {
            DCHECK(!query.promised_auxdata.has_value());
        }
        if (query.promised_auxdata.has_value() && query.promised_auxdata->metadata.seqnum >= syncto_seqnum) {
            // when log append finished before starting to query
            DCHECK(result_id == 0);
            pending_query_results_.push_back(BuildAuxContinueResult(query, syncto_seqnum));
            HVLOG(1) << "ProcessReadNextU: AuxContinueResult";
            return;
        }
        sync_continue = false;
        HVLOG_F(1, "ProcessReadNextUntil: hop_times={} seqnum={:016X} logspace={} tag={} syncto_seqnum={:016X} continue={}",
                query.hop_times, query.query_seqnum, query.user_logspace, tag, syncto_seqnum, sync_continue);
    } else {
        // query use localid
        const auto& it = log_index_map_.find(query.query_localid);
        if (it != log_index_map_.end()) {
            // syncto future
            syncto_seqnum = it->second;
            if (syncto_seqnum == 0) {
                pending_query_results_.push_back(BuildResolvedResult(query, result_id));
                HVLOG(1) << "ProcessReadNextU: ResolvedResult no preceding logs need to be synced";
                return;
            }
            if ((query.flags & IndexQuery::kReadFromCachedFlag) != 0) {
                // Disable auxdata would not causing loop here: the IndexQuery::kReadLocalIdFlag
                // would be clear in Engine::BuildIndexQuery, so the next query would never reach to here again.
                if (!query.promised_auxdata.has_value() || query.promised_auxdata->metadata.seqnum >= syncto_seqnum) {
                    // when log append finished before starting to query
                    DCHECK(result_id == 0);
                    pending_query_results_.push_back(BuildAuxContinueResult(query, syncto_seqnum));
                    HVLOG(1) << "ProcessReadNextU: AuxContinueResult";
                    return;
                }
            }
            sync_continue = false;
        } else {
            // No need to check promised_auxdata here since querying a future seqnum,
            // any auxdata(if got) must be previous than the future.
            syncto_seqnum = kMaxLogSeqNum;
            sync_continue = true;
        }
        HVLOG_F(1, "ProcessReadNextUntil: hop_times={} localid={:016X} logspace={} tag={} syncto_seqnum={:016X} continue={}",
                query.hop_times, query.query_localid, query.user_logspace, tag, syncto_seqnum, sync_continue);
    }
    // get index before target
    size_t end_index;
    uint64_t end_seqnum;
    uint16_t end_engine_id;
    uint64_t end_localid;
    PerSpaceIndex* perspace_index = nullptr;
    bool end_index_found;
    if (!index_.contains(query.user_logspace)) {
        end_index_found = false;
    } else {
        perspace_index = GetOrCreateIndex(query.user_logspace);
        uint64_t query_end_seqnum =
            syncto_seqnum == kMaxLogSeqNum ? syncto_seqnum : syncto_seqnum - 1;
        end_index_found = perspace_index->FindPrev(query_end_seqnum, query.user_tag,
            &end_seqnum, &end_engine_id, &end_localid, &end_index);
        HVLOG_F(1, "ProcessReadNextUntil find end: found={} query_end_seqnum={:016X} seqnum={:016X} logspace={} tag={}",
                end_index_found, query_end_seqnum, end_seqnum, query.user_logspace, query.user_tag);
    }
    if (end_index_found) {
        size_t start_index = 0;
        uint64_t start_seqnum = query.query_start_seqnum;
        DCHECK(result_id == 0);
        // get view before target
        if (query.promised_auxdata.has_value()) {
            start_seqnum = query.promised_auxdata->metadata.seqnum;
        }
        if (start_seqnum != 0) {
            uint16_t start_engine_id;
            uint64_t start_localid;
            uint64_t temp = start_seqnum;   // DEBUG
            perspace_index->FindNext(start_seqnum, query.user_tag,
                &start_seqnum, &start_engine_id, &start_localid, &start_index);
            HVLOG_F(1, "ProcessReadNextUntil find start: query_start_seqnum={:016X} seqnum={:016X} logspace={} tag={}",
                    temp, start_seqnum, query.user_logspace, query.user_tag);
        }
        if (start_seqnum < end_seqnum) {
            // proceed multiple steps
            size_t n = perspace_index->GetRangeByIndex(start_index, end_index, tag,
                [this, query, &result_id](size_t index, uint64_t seqnum, uint16_t engine_id, uint64_t localid) {
                    pending_query_results_.push_back(
                        BuildFoundResult(query, view_->id(), seqnum, engine_id, localid, result_id++));
                    HVLOG_F(1, "ProcessReadNextU: FoundResult: seqnum={:016X}", seqnum);
                    return false;   // stop
                });
            DCHECK(n > 0);
        } else if (start_seqnum == end_seqnum) {
            // proceed only one step
            pending_query_results_.push_back(
                BuildFoundResult(query, view_->id(), end_seqnum, end_engine_id, end_localid, result_id++));
            HVLOG_F(1, "ProcessReadNextU: FoundResult: seqnum={:016X}", end_seqnum);
        } else /* if (start_seqnum > end_seqnum) */ {
            // this may happen when got a query_start_seqnum belongs to
            // (last_seqnum, target_seqnum) of the tag, where end_seqnum = last_seqnum
            HVLOG_F(1, "ProcessReadNextU: start_seqnum={:016X} out of range [{:016X}, {:016X}]",
                        start_seqnum, query.query_start_seqnum, end_seqnum);
        }
    } else if (view_->id() > 0) {
        pending_query_results_.push_back(BuildViewContinueResult(query, false, 0, 0, 0, result_id));
        HVLOG(1) << "ProcessReadNextU: ContinueResult";
        return;
    } else {
        if (!sync_continue) {
            pending_query_results_.push_back(BuildNotFoundResult(query, result_id));
            HVLOG(1) << "ProcessReadNextU: NotFoundResult";
            return;
        }
        // For sync_continue, searching MaxLogSeqNum may get empty due to the
        // first async append is on the way.
    }

    if (sync_continue) {
        pending_syncto_queries_.insert(std::make_pair(
            query.query_localid,
            BuildContinueQuery(query, end_index_found, end_index, end_seqnum,
                               end_engine_id, end_localid, result_id)));
        if (end_index_found) {
            HVLOG_F(1, "ProcessReadNextU: ContinueQuery last localid={:016X} seqnum={:016X} id={}",
                end_localid, end_seqnum, result_id);
        } else {
            // no entry in PerSpaceLogIndex
            HVLOG(1) << "ProcessReadNextU: ContinueQuery nothing found yet";
        }
    } else {
        pending_query_results_.push_back(BuildResolvedResult(query, result_id));
        HVLOG_F(1, "ProcessReadNextU: ResolvedResult all preceding logs are synced id={}", result_id);
    }
}
void Index::ProcessReadNextUntilContinue(const IndexQuery& query) {
    DCHECK(query.direction == IndexQuery::kReadNextU);
    DCHECK(!query.initial);
    DCHECK(query.hop_times > 0);
    DCHECK((query.flags & IndexQuery::kReadLocalIdFlag) != 0);
    DCHECK(query.prev_found_result.localid != kInvalidLogId);

    uint64_t result_id = query.next_result_id;
    uint64_t tag = query.user_tag;
    uint64_t target_localid = query.query_localid;
    // When target_localid had been propagated, GetRange may also return none
    // if the target_localid is added to a different tag, but the view is new
    // and able to return EOF.
    bool sync_continue = !log_index_map_.contains(target_localid);
    size_t end_index = std::numeric_limits<size_t>::max();
    uint64_t end_seqnum;
    uint16_t end_engine_id;
    uint64_t end_localid;
    size_t n = GetOrCreateIndex(query.user_logspace)->GetRangeByIndex(
        query.prev_found_result.log_index + 1, end_index, tag,
        [this, query, target_localid, &result_id, &end_index, &end_seqnum, &end_engine_id, &end_localid, &sync_continue]
        (size_t index, uint64_t seqnum, uint16_t engine_id, uint64_t localid) {
            if (localid == target_localid) {
                sync_continue = false;
                HVLOG_F(1, "ProcessReadNextU Continue: FoundResult: localid={:016X}", localid);
                return true;    // stop early
            }
            pending_query_results_.push_back(
                BuildFoundResult(query, view_->id(), seqnum, engine_id, localid, result_id++));
            end_index = index;
            end_seqnum = seqnum;
            end_engine_id = engine_id;
            end_localid = localid;
            HVLOG_F(1, "ProcessReadNextU Continue: FoundResult: localid={:016X} seqnum={:016X}", localid, seqnum);
            return false;   // continue
        });
    if (sync_continue) {
        pending_syncto_queries_.insert(std::make_pair(
            target_localid,
            BuildContinueQuery(query, /*end_index_found*/ n > 0, end_index,
                               end_seqnum, end_engine_id, end_localid,
                               result_id)));
        if (n > 0) {
            HVLOG_F(1, "ProcessReadNextU Continue: ContinueQuery last localid={:016X} seqnum={:016X} id={}",
                end_localid, end_seqnum, result_id);
        } else {
            // no entry in PerSpaceLogIndex
            HVLOG(1) << "ProcessReadNextU Continue: ContinueQuery nothing found yet";
        }
    } else {
        pending_query_results_.push_back(BuildResolvedResult(query, result_id));
        HVLOG_F(1, "ProcessReadNextU Continue: ResolvedResult all preceding logs are synced id={}", result_id);
    }
}

void Index::ProcessReadNextUntil(const IndexQuery& query) {
    // if (query.initial) {
    //     ProcessReadNextUntilInitial(query);
    // } else {
    //     ProcessReadNextUntilContinue(query);
    // }

    DCHECK(query.direction == IndexQuery::kReadNextU);
    uint64_t result_id = query.next_result_id;
    HVLOG_F(1, "ProcessReadNextUntil query_start_seqnum={:016X} "
               "target=f{}:{:016X} logspace={} tag={} initial={} hop_times={} id={}",
               query.query_start_seqnum, query.flags, query.query_seqnum,
               query.user_logspace, query.user_tag, query.initial,
               query.hop_times, result_id);
    // TODO: remove this if check after integrating view to localid
    if ((query.flags & IndexQuery::kReadLocalIdFlag) == 0) {
        uint16_t query_view_id = log_utils::GetViewId(query.query_seqnum);
        if (query_view_id < view_->id()) {
            pending_query_results_.push_back(BuildViewContinueResult(query, false, 0, 0, 0, result_id));
            HVLOG(1) << "ProcessReadNextU: ViewContinueResult";
            return;
        }
    }
    uint64_t start_seqnum = query.query_start_seqnum;
    if ((query.flags & IndexQuery::kReadLocalIdFlag) == 0) {
        DCHECK_LT(start_seqnum, query.query_seqnum);
    } else {
        DCHECK_NE(query.query_localid, protocol::kInvalidLogLocalId);
    }
    uint64_t found_start_seqnum;
    uint16_t start_engine_id;
    uint64_t start_localid;
    uint64_t start_index;
    if (GetOrCreateIndex(query.user_logspace)->FindNext(start_seqnum, query.user_tag,
        &found_start_seqnum, &start_engine_id, &start_localid, &start_index)) {
        HVLOG_F(1, "ProcessReadNextUntil find start: query_start_seqnum={:016X} seqnum={:016X} localid={:016X} logspace={} tag={}",
                start_seqnum, found_start_seqnum, start_localid, query.user_logspace, query.user_tag);

        if ((query.flags & IndexQuery::kReadLocalIdFlag) == 0) {
            // perform as normal ReadNext
            uint64_t end_seqnum = query.query_seqnum;
            pending_query_results_.push_back(
                BuildFoundRangeResult(query, view_->id(), found_start_seqnum, start_engine_id, start_localid,
                                      end_seqnum, protocol::kInvalidLogLocalId, result_id++));
        } else {
            const auto& it = log_index_map_.find(query.query_localid);
            if (it != log_index_map_.end()) {
                // syncto not including end_seqnum itself
                uint64_t end_seqnum = it->second;
                pending_query_results_.push_back(
                    BuildFoundRangeResult(query, view_->id(), found_start_seqnum, start_engine_id, start_localid,
                                          end_seqnum, protocol::kInvalidLogLocalId, result_id++));
            } else {
                // get existing last seqnum
                uint64_t last_seqnum;
                uint16_t last_engine_id;
                uint64_t last_localid;
                // get the last should be fast
                bool found = GetOrCreateIndex(query.user_logspace)->FindPrev(kMaxLogSeqNum, query.user_tag,
                                &last_seqnum, &last_engine_id, &last_localid, /*&index*/nullptr);
                DCHECK(found);  // found start_seqnum -> the stream is not empty
                pending_query_results_.push_back(
                    BuildFoundRangeResult(query, view_->id(), found_start_seqnum, start_engine_id, start_localid,
                                        last_seqnum, query.query_localid, result_id++));
            }
        }
    } else {
        if ((query.flags & IndexQuery::kReadLocalIdFlag) == 0) {
            // perform as normal ReadNext
            pending_query_results_.push_back(BuildNotFoundResult(query, result_id++));
            // not necessary but to make the progress more clear
            if (GetOrCreateIndex(query.user_logspace)->ContainsTag(query.user_tag)) {
                HVLOG_F(1, "ProcessReadNextUntil not found seqnum={:016X}. tag={} logspace={}",
                        query.query_start_seqnum, query.user_tag, query.user_logspace);
            } else {
                HVLOG_F(1, "ProcessReadNextUntil not found tag={}. seqnum={:016X} logspace={}",
                        query.user_tag, query.query_start_seqnum, query.user_logspace);
            }
        } else {
            if (!log_index_map_.contains(query.query_localid)) {
                pending_syncto_queries_.insert(std::make_pair(
                    query.query_localid,
                    BuildContinueQuery(query, /*end_index_found*/ false, /*index*/ 0,
                                    /*seqnum*/ 0, /*engine_id*/ 0, /*localid*/ 0, /*result_id*/ 0)));
                HVLOG_F(1, "ProcessReadNextUntil pending tag={} seqnum={:016X} localid={:016X} logspace={}",
                        query.user_tag, query.query_start_seqnum, query.query_localid, query.user_logspace);
            } else {
                // If the querying tag is not added but the target log exists,
                // the target log must be added to a different tag.
                // In such a scenario later logs to the querying tag must with
                // a seqnum > target log seqnum, so we can safely ensure that the
                // syncto operation had reached to the target.
                pending_query_results_.push_back(BuildResolvedResult(query, result_id++));
                HVLOG_F(1, "ProcessReadNextUntil resolved tag={} seqnum={:016X} localid={:016X} logspace={}",
                        query.user_tag, query.query_start_seqnum, query.query_localid, query.user_logspace);
            }
        }
    }
}

void Index::ProcessReadPrev(const IndexQuery& query) {
    DCHECK(query.direction == IndexQuery::kReadPrev ||
           query.direction == IndexQuery::kReadPrevAux);
    DCHECK((query.flags & IndexQuery::kReadLocalIdFlag) == 0);
    HVLOG_F(1, "ProcessReadPrev: seqnum={:016X}, logspace={}, tag={}",
            query.query_seqnum, query.user_logspace, query.user_tag);
    if (query.direction == IndexQuery::kReadPrevAux && !query.promised_auxdata.has_value()) {
        pending_query_results_.push_back(BuildNotFoundResult(query));
        HVLOG(1) << "ProcessReadPrevAux: NotFoundResult";
        return;
    }
    uint16_t query_view_id = log_utils::GetViewId(query.query_seqnum);
    if (query_view_id < view_->id()) {
        pending_query_results_.push_back(BuildViewContinueResult(query, false, 0, 0, 0));
        HVLOG(1) << "ProcessReadPrev: ContinueResult";
        return;
    }
    uint64_t seqnum;
    uint16_t engine_id;
    uint64_t localid;
    bool found = IndexFindPrev(query, &seqnum, &engine_id, &localid);
    if (found) {
        pending_query_results_.push_back(
            BuildFoundResult(query, view_->id(), seqnum, engine_id, localid));
        HVLOG_F(1, "ProcessReadPrev: FoundResult: seqnum={:016X}", seqnum);
    } else if (view_->id() > 0) {
        pending_query_results_.push_back(BuildViewContinueResult(query, false, 0, 0, 0));
        HVLOG(1) << "ProcessReadPrev: ContinueResult";
    } else {
        pending_query_results_.push_back(BuildNotFoundResult(query));
        if (query.direction == IndexQuery::kReadPrev) {
            HVLOG(1) << "ProcessReadPrev: NotFoundResult";
        } else {
            HVLOG(1) << "ProcessReadPrevAux: NotFoundResult";
        }
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
    uint64_t localid;
    bool found = IndexFindNext(query, &seqnum, &engine_id, &localid);
    if (query_view_id == view_->id()) {
        if (found) {
            pending_query_results_.push_back(
                BuildFoundResult(query, view_->id(), seqnum, engine_id, localid));
        }
        return found;
    } else {
        pending_query_results_.push_back(
            BuildViewContinueResult(query, found, seqnum, engine_id, localid));
        return true;
    }
}

bool Index::IndexFindNext(const IndexQuery& query, uint64_t* seqnum,
                          uint16_t* engine_id, uint64_t* localid) {
    DCHECK(query.direction == IndexQuery::kReadNext ||
           query.direction == IndexQuery::kReadNextB);
    if (!index_.contains(query.user_logspace)) {
        return false;
    }
    return GetOrCreateIndex(query.user_logspace)->FindNext(
        query.query_seqnum, query.user_tag, seqnum, engine_id, localid);
}

bool Index::IndexFindPrev(const IndexQuery& query, uint64_t* seqnum,
                          uint16_t* engine_id, uint64_t* localid) {
    DCHECK(query.direction == IndexQuery::kReadPrev ||
           query.direction == IndexQuery::kReadPrevAux);
    if (!index_.contains(query.user_logspace)) {
        return false;
    }
    return GetOrCreateIndex(query.user_logspace)->FindPrev(
        query.query_seqnum, query.user_tag, seqnum, engine_id, localid);
}

IndexQueryResult Index::BuildFoundRangeResult(const IndexQuery& query,
                                              uint16_t view_id, uint64_t seqnum,
                                              uint16_t engine_id, uint64_t localid,
                                              uint64_t end_seqnum, uint64_t future_localid,
                                              uint64_t result_id) {
    DCHECK(query.direction == IndexQuery::ReadDirection::kReadNextU);
    return IndexQueryResult {
        .state = IndexQueryResult::kFoundRange,
        .metalog_progress = query.initial ? index_metalog_progress()
                                          : query.metalog_progress,
        .next_view_id = view_id,
        .id = result_id,
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = view_id,
            .engine_id = engine_id,
            .seqnum = seqnum,
            .localid = localid,

            .end_seqnum = end_seqnum,
            .future_localid = future_localid,
        },
    };
}
IndexQueryResult Index::BuildFoundResult(const IndexQuery& query,
                                         uint16_t view_id, uint64_t seqnum,
                                         uint16_t engine_id, uint64_t localid,
                                         uint64_t result_id) {
    return IndexQueryResult {
        .state = IndexQueryResult::kFound,
        .metalog_progress = query.initial ? index_metalog_progress()
                                          : query.metalog_progress,
        .next_view_id = 0,
        .id = result_id,
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = view_id,
            .engine_id = engine_id,
            .seqnum = seqnum,
            .localid = localid
        },
    };
}

IndexQuery Index::BuildContinueQuery(const IndexQuery& query, bool found,
                                     size_t index, uint64_t seqnum, uint16_t engine_id, uint64_t localid,
                                     uint64_t next_result_id) {
    IndexQuery next_query = query;
    ++next_query.hop_times;
    if (found) {
        next_query.initial = false;
        next_query.next_result_id = next_result_id;
        next_query.prev_found_result = IndexFoundResult {
            .view_id = view_->id(),
            .engine_id = engine_id,
            .seqnum = seqnum,
            .localid = localid,
            .log_index = index
        };
    }
    return next_query;
}

IndexQueryResult Index::BuildResolvedResult(const IndexQuery& query, uint64_t result_id) {
    return IndexQueryResult {
        .state = IndexQueryResult::kEOF,
        .metalog_progress = query.initial ? index_metalog_progress()
                                          : query.metalog_progress,
        .next_view_id = 0,
        .id = result_id,
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = 0,
            .engine_id = 0,
            .seqnum = kInvalidLogSeqNum,
            .localid = kInvalidLogId
        },
    };
}

IndexQueryResult Index::BuildNotFoundResult(const IndexQuery& query, uint64_t result_id) {
    return IndexQueryResult {
        .state = IndexQueryResult::kEmpty,
        .metalog_progress = query.initial ? index_metalog_progress()
                                          : query.metalog_progress,
        .next_view_id = 0,
        .id = result_id,
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = 0,
            .engine_id = 0,
            .seqnum = kInvalidLogSeqNum,
            .localid = kInvalidLogId
        },
    };
}

IndexQueryResult Index::BuildAuxContinueResult(const IndexQuery& query,
                                               uint64_t target_seqnum) {
    DCHECK(query.direction == IndexQuery::kReadNextU);
    DCHECK(query.initial);
    IndexQueryResult result = {
        .state = IndexQueryResult::kAuxContinue,
        .metalog_progress = query.initial ? index_metalog_progress()
                                          : query.metalog_progress,
        .next_view_id = gsl::narrow_cast<uint16_t>(view_->id()),
        .id = 0,
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = 0,
            .engine_id = 0,
            .seqnum = target_seqnum,
            .localid = kInvalidLogId
        },
    };
    return result;
}

IndexQueryResult Index::BuildViewContinueResult(const IndexQuery& query, bool found,
                                                uint64_t seqnum, uint16_t engine_id, uint64_t localid,
                                                uint64_t result_id) {
    DCHECK(view_->id() > 0);
    IndexQueryResult result = {
        .state = IndexQueryResult::kViewContinue,
        .metalog_progress = query.initial ? index_metalog_progress()
                                          : query.metalog_progress,
        .next_view_id = gsl::narrow_cast<uint16_t>(view_->id() - 1),
        .id = result_id,
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = 0,
            .engine_id = 0,
            .seqnum = kInvalidLogSeqNum,
            .localid = kInvalidLogId
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
            .localid = localid
        };
    } else if (!query.initial && query.prev_found_result.seqnum != kInvalidLogSeqNum) {
        result.found_result = query.prev_found_result;
    }
    return result;
}

}  // namespace log
}  // namespace faas
