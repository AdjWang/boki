#include "log/utils.h"
#include "log/index_data.h"
#include "log/index_data_c.h"

namespace faas {
namespace log {

IndexDataManager::IndexDataManager(uint32_t logspace_id)
    : log_header_(fmt::format("IndexDataManager[{}]: ", logspace_id)),
      logspace_id_(logspace_id),
      indexed_seqnum_position_(0),
      indexed_metalog_position_(0)
    {}

void IndexDataManager::AddIndexData(uint32_t user_logspace,
                                    uint32_t seqnum_lowhalf, uint16_t engine_id,
                                    const UserTagVec& user_tags) {
    GetOrCreateIndex(user_logspace)->Add(seqnum_lowhalf, engine_id, user_tags);
}

void IndexDataManager::AddAsyncIndexData(uint64_t localid, uint32_t seqnum_lowhalf,
                                         UserTagVec user_tags) {
    DCHECK(log_index_map_.find(localid) == log_index_map_.end())
        << "Duplicate index_data.local_id for log_index_map_";
    log_index_map_[localid] = AsyncIndexData{
        .seqnum = bits::JoinTwo32(logspace_id_, seqnum_lowhalf),
        .user_tags = user_tags,
    };
}

IndexDataManager::QueryConsistencyType IndexDataManager::CheckConsistency(const IndexQuery& query) {
    if (query.initial) {
        HVLOG(1) << "Receive initial query";
        uint16_t func_view_id = log_utils::GetViewId(query.metalog_progress);
        if (func_view_id > view_id()) {
            return IndexDataManager::QueryConsistencyType::kInitFutureViewBail;
        } else if (func_view_id < view_id()) {
            return IndexDataManager::QueryConsistencyType::kInitPastViewOK;
        } else {
            DCHECK_EQ(func_view_id, view_id());
            uint32_t position = bits::LowHalf64(query.metalog_progress);
            if (position <= indexed_metalog_position()) {
                return IndexDataManager::QueryConsistencyType::kInitCurrentViewOK;
            } else {
                return IndexDataManager::QueryConsistencyType::kInitCurrentViewPending;
            }
        }
    } else {
        HVLOG(1) << "Receive continue query";
        return IndexDataManager::QueryConsistencyType::kContOK;
    }
}

IndexQueryResult IndexDataManager::ProcessLocalIdQuery(const IndexQuery& query) {
    DCHECK(query.direction == IndexQuery::kReadLocalId);
    // Local id is propagated by user atop function arguments, it may faster
    // than metalog_position_ propagation. In one user session, previously
    // appended async log's local id must be always perceived as readable to
    // gaurantee read-your-write. So local query is promised to be found, even
    // metalog_position_ is old.

    // replace seqnum if querying by localid
    uint64_t localid = query.query_seqnum;
    uint64_t seqnum = kInvalidLogSeqNum;
    if (IndexFindLocalId(localid, &seqnum)) {
        // HVLOG_F(1, "ProcessQuery: found async map from local_id=0x{:016X} to seqnum=0x{:016X}",
        //         local_id, seqnum);
        // BUG: WOW! A reaaaaaaaaly strange bug! Triggered so many times with the magic result 28524,
        // causing functions failed to send back the query result to the target engine node.
        DCHECK(query.origin_node_id != 28524) << utils::DumpStackTrace();

        uint16_t engine_id = gsl::narrow_cast<uint16_t>(bits::HighHalf64(localid));
        return BuildFoundResult(query, view_id(), seqnum, engine_id);
    } else {
        // not found
        // HVLOG_F(1, "pending ProcessQuery: NotFoundResult due to log_index_map_ not indexed local_id: 0x{:016X}",
        //         local_id);
        return BuildPendingResult(query);
    }
}

IndexQueryResult IndexDataManager::ProcessReadNext(const IndexQuery& query) {
    DCHECK(query.direction == IndexQuery::kReadNext);
    HVLOG_F(1, "ProcessReadNext: seqnum={}, logspace={}, tag={}",
            bits::HexStr0x(query.query_seqnum), query.user_logspace, query.user_tag);
    uint16_t query_view_id = log_utils::GetViewId(query.query_seqnum);
    if (query_view_id > view_id()) {
        HVLOG(1) << "ProcessReadNext: NotFoundResult";
        return BuildNotFoundResult(query);
    }
    uint64_t seqnum;
    uint16_t engine_id;
    bool found = IndexFindNext(query, &seqnum, &engine_id);
    if (query_view_id == view_id()) {
        if (found) {
            HVLOG_F(1, "ProcessReadNext: FoundResult: seqnum={}", seqnum);
            return BuildFoundResult(query, view_id(), seqnum, engine_id);
        } else {
            if (query.prev_found_result.seqnum != kInvalidLogSeqNum) {
                const IndexFoundResult& found_result = query.prev_found_result;
                HVLOG_F(1, "ProcessReadNext: FoundResult (from prev_result): seqnum={}",
                        found_result.seqnum);
                return BuildFoundResult(query, found_result.view_id,
                                        found_result.seqnum, found_result.engine_id);
            } else {
                HVLOG(1) << "ProcessReadNext: NotFoundResult";
                return BuildNotFoundResult(query);
            }
        }
    } else {
        HVLOG(1) << "ProcessReadNext: ContinueResult";
        return BuildContinueResult(query, found, seqnum, engine_id);
    }
}

IndexQueryResult IndexDataManager::ProcessReadPrev(const IndexQuery& query) {
    DCHECK(query.direction == IndexQuery::kReadPrev);
    HVLOG_F(1, "ProcessReadPrev: seqnum={}, logspace={}, tag={}",
            bits::HexStr0x(query.query_seqnum), query.user_logspace, query.user_tag);
    uint16_t query_view_id = log_utils::GetViewId(query.query_seqnum);
    if (query_view_id < view_id()) {
        HVLOG(1) << "ProcessReadPrev: ContinueResult";
        return BuildContinueResult(query, false, 0, 0);
    }
    uint64_t seqnum;
    uint16_t engine_id;
    bool found = IndexFindPrev(query, &seqnum, &engine_id);
    if (found) {
        HVLOG_F(1, "ProcessReadPrev: FoundResult: seqnum={}", seqnum);
        return BuildFoundResult(query, view_id(), seqnum, engine_id);
    } else if (view_id() > 0) {
        HVLOG(1) << "ProcessReadPrev: ContinueResult";
        return BuildContinueResult(query, false, 0, 0);
    } else {
        HVLOG(1) << "ProcessReadPrev: NotFoundResult";
        return BuildNotFoundResult(query);
    }
}

IndexQueryResult IndexDataManager::ProcessBlockingQuery(const IndexQuery& query) {
    DCHECK(query.direction == IndexQuery::kReadNextB && query.initial);
    uint16_t query_view_id = log_utils::GetViewId(query.query_seqnum);
    if (query_view_id > view_id()) {
        return BuildNotFoundResult(query);
    }
    uint64_t seqnum;
    uint16_t engine_id;
    bool found = IndexFindNext(query, &seqnum, &engine_id);
    if (query_view_id == view_id()) {
        if (found) {
            return BuildFoundResult(query, view_id(), seqnum, engine_id);
        } else {
            return BuildPendingResult(query);
        }
    } else {
        return BuildContinueResult(query, found, seqnum, engine_id);
    }
}

bool IndexDataManager::IndexFindNext(const IndexQuery& query, uint64_t* seqnum, uint16_t* engine_id) {
    DCHECK(query.direction == IndexQuery::kReadNext ||
           query.direction == IndexQuery::kReadNextB);
    if (!index_.contains(query.user_logspace)) {
        return false;
    }
    return GetOrCreateIndex(query.user_logspace)->FindNext(
        query.query_seqnum, query.user_tag, seqnum, engine_id);
}

bool IndexDataManager::IndexFindPrev(const IndexQuery& query, uint64_t* seqnum, uint16_t* engine_id) {
    DCHECK(query.direction == IndexQuery::kReadPrev);
    if (!index_.contains(query.user_logspace)) {
        return false;
    }
    return GetOrCreateIndex(query.user_logspace)->FindPrev(
        query.query_seqnum, query.user_tag, seqnum, engine_id);
}

bool IndexDataManager::IndexFindLocalId(uint64_t localid, uint64_t* seqnum) {
    auto it = log_index_map_.find(localid);
    if (it == log_index_map_.end()) {
        return false;
    } else {
        DCHECK_NE(seqnum, nullptr);
        *seqnum = it->second.seqnum;
        return true;
    }
}

// ----------------------------------------------------------------------------

PerSpaceIndex* IndexDataManager::GetOrCreateIndex(uint32_t user_logspace) {
    if (index_.contains(user_logspace)) {
        return index_.at(user_logspace).get();
    }
    HVLOG_F(1, "Create index of user logspace {}", user_logspace);
    PerSpaceIndex* index = new PerSpaceIndex(logspace_id_, user_logspace);
    index_[user_logspace].reset(index);
    return index;
}

IndexQueryResult IndexDataManager::BuildFoundResult(const IndexQuery& query, uint16_t view_id,
                                         uint64_t seqnum, uint16_t engine_id) {
    return IndexQueryResult {
        .state = IndexQueryResult::kFound,
        .metalog_progress = query.initial ? index_metalog_progress()
                                          : query.metalog_progress,
        .next_view_id = 0,
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = view_id,
            .engine_id = engine_id,
            .seqnum = seqnum
        },
    };
}

IndexQueryResult IndexDataManager::BuildNotFoundResult(const IndexQuery& query) {
    return IndexQueryResult {
        .state = IndexQueryResult::kEmpty,
        .metalog_progress = query.initial ? index_metalog_progress()
                                          : query.metalog_progress,
        .next_view_id = 0,
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = 0,
            .engine_id = 0,
            .seqnum = kInvalidLogSeqNum
        },
    };
}

IndexQueryResult IndexDataManager::BuildPendingResult(const IndexQuery& query) {
    return IndexQueryResult {
        .state = IndexQueryResult::kPending,
        .metalog_progress = query.initial ? index_metalog_progress()
                                          : query.metalog_progress,
        .next_view_id = 0,
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = 0,
            .engine_id = 0,
            .seqnum = kInvalidLogSeqNum
        },
    };
}

IndexQueryResult IndexDataManager::BuildContinueResult(const IndexQuery& query, bool found,
                                            uint64_t seqnum, uint16_t engine_id) {
    DCHECK(view_id() > 0);
    IndexQueryResult result = {
        .state = IndexQueryResult::kContinue,
        .metalog_progress = query.initial ? index_metalog_progress()
                                          : query.metalog_progress,
        .next_view_id = gsl::narrow_cast<uint16_t>(view_id() - 1),
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = 0,
            .engine_id = 0,
            .seqnum = kInvalidLogSeqNum
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
            .view_id = view_id(),
            .engine_id = engine_id,
            .seqnum = seqnum
        };
    } else if (!query.initial && query.prev_found_result.seqnum != kInvalidLogSeqNum) {
        result.found_result = query.prev_found_result;
    }
    return result;
}


PerSpaceIndex::PerSpaceIndex(uint32_t logspace_id, uint32_t user_logspace)
    : logspace_id_(logspace_id),
      user_logspace_(user_logspace),
      // TODO: more reasonable name and size
      segment_(create_only, "MySharedMemory", 100*1024*1024),
      alloc_inst_(segment_.get_segment_manager()),
      seqnums_(alloc_inst_),
      seqnums_by_tag_(*segment_.construct<log_stream_map_t>
        //(object name), (first ctor parameter, second ctor parameter)
        ("MyMap")(0u, boost::hash<uint64_t>(), std::equal_to<uint64_t>(), alloc_inst_))
    {}

PerSpaceIndex::~PerSpaceIndex() {
    shared_memory_object::remove("MySharedMemory");
}

void PerSpaceIndex::Add(uint32_t seqnum_lowhalf, uint16_t engine_id,
                        const UserTagVec& user_tags) {
    DCHECK(!engine_ids_.contains(seqnum_lowhalf));
    engine_ids_[seqnum_lowhalf] = engine_id;
    DCHECK(seqnums_.empty() || seqnum_lowhalf > seqnums_.back());
    seqnums_.push_back(seqnum_lowhalf);
    for (uint64_t user_tag : user_tags) {
        DCHECK_NE(user_tag, kEmptyLogTag);
        if (seqnums_by_tag_.contains(user_tag)) {
            seqnums_by_tag_.at(user_tag).push_back(seqnum_lowhalf);
        } else {
            vector<uint32_t, uint32_allocator_t> value_vec(alloc_inst_);
            value_vec.push_back(seqnum_lowhalf);
            seqnums_by_tag_.emplace(user_tag, value_vec);
        }
    }
}

bool PerSpaceIndex::FindPrev(uint64_t query_seqnum, uint64_t user_tag,
                             uint64_t* seqnum, uint16_t* engine_id) const {
    uint32_t seqnum_lowhalf;
    if (user_tag == kEmptyLogTag) {
        if (!FindPrev(seqnums_, query_seqnum, &seqnum_lowhalf)) {
            return false;
        }
    } else {
        if (!seqnums_by_tag_.contains(user_tag)) {
            return false;
        }
        if (!FindPrev(seqnums_by_tag_.at(user_tag), query_seqnum, &seqnum_lowhalf)) {
            return false;
        }
    }
    DCHECK(engine_ids_.contains(seqnum_lowhalf));
    *seqnum = bits::JoinTwo32(logspace_id_, seqnum_lowhalf);
    DCHECK_LE(*seqnum, query_seqnum);
    *engine_id = engine_ids_.at(seqnum_lowhalf);
    return true;
}

bool PerSpaceIndex::FindNext(uint64_t query_seqnum, uint64_t user_tag,
                             uint64_t* seqnum, uint16_t* engine_id) const {
    uint32_t seqnum_lowhalf;
    if (user_tag == kEmptyLogTag) {
        if (!FindNext(seqnums_, query_seqnum, &seqnum_lowhalf)) {
            return false;
        }
    } else {
        if (!seqnums_by_tag_.contains(user_tag)) {
            return false;
        }
        if (!FindNext(seqnums_by_tag_.at(user_tag), query_seqnum, &seqnum_lowhalf)) {
            return false;
        }
    }
    DCHECK(engine_ids_.contains(seqnum_lowhalf));
    *seqnum = bits::JoinTwo32(logspace_id_, seqnum_lowhalf);
    DCHECK_GE(*seqnum, query_seqnum);
    *engine_id = engine_ids_.at(seqnum_lowhalf);
    return true;
}

bool PerSpaceIndex::FindPrev(const log_stream_vec_t& seqnums,
                             uint64_t query_seqnum,
                             uint32_t* result_seqnum) const {
    if (seqnums.empty() || bits::JoinTwo32(logspace_id_, seqnums.front()) > query_seqnum) {
        return false;
    }
    if (query_seqnum == kMaxLogSeqNum) {
        *result_seqnum = *seqnums.rbegin();
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
        *result_seqnum = *(--iter);
        return true;
    }
}

bool PerSpaceIndex::FindNext(const log_stream_vec_t& seqnums,
                             uint64_t query_seqnum,
                             uint32_t* result_seqnum) const {
    if (seqnums.empty() || bits::JoinTwo32(logspace_id_, *seqnums.rbegin()) < query_seqnum) {
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
        return true;
    }
}

}  // namespace log
}  // namespace faas


void test_func() {
    printf("test func\n");
    auto index_data = faas::log::IndexDataManager(1u);
    index_data.set_indexed_metalog_position(4u);
    printf("test func create index_data\n");
}

void* ConstructIndexData(uint32_t logspace_id) {
    return new faas::log::IndexDataManager(logspace_id);
}

void DestructIndexData(void* index_data) {
    delete reinterpret_cast<faas::log::IndexDataManager*>(index_data);
}

int ProcessLocalIdQuery(void* index_data, /*InOut*/ uint64_t* metalog_progress,
                        uint64_t localid, /*Out*/ uint64_t* seqnum) {
    auto this_ = reinterpret_cast<faas::log::IndexDataManager*>(index_data);
    faas::log::IndexQuery query = faas::log::IndexQuery {
        .direction = faas::log::IndexQuery::kReadLocalId,
        .initial = true,
        .query_seqnum = localid,
        .metalog_progress = *metalog_progress,
    };
    faas::log::IndexDataManager::QueryConsistencyType consistency_type =
        this_->CheckConsistency(query);
    switch (consistency_type) {
        case faas::log::IndexDataManager::QueryConsistencyType::kInitFutureViewBail:
            return -1;
        case faas::log::IndexDataManager::QueryConsistencyType::kInitCurrentViewPending:
            return -2;
        case faas::log::IndexDataManager::QueryConsistencyType::kInitCurrentViewOK:
        case faas::log::IndexDataManager::QueryConsistencyType::kInitPastViewOK: {
            faas::log::IndexQueryResult result =
                this_->ProcessLocalIdQuery(query);
            if (result.state == faas::log::IndexQueryResult::kFound) {
                *metalog_progress = result.metalog_progress;
                *seqnum = result.found_result.seqnum;
            }
            return static_cast<int>(result.state);
        }
        case faas::log::IndexDataManager::QueryConsistencyType::kContOK:
        default:
            return -3;
    }
}

int ProcessReadNext(void* index_data, /*InOut*/ uint64_t* metalog_progress,
                    uint32_t user_logspace, uint64_t query_seqnum,
                    uint64_t query_tag, /*Out*/ uint64_t* seqnum) {
    auto this_ = reinterpret_cast<faas::log::IndexDataManager*>(index_data);
    faas::log::IndexQuery query = faas::log::IndexQuery {
        .direction = faas::log::IndexQuery::kReadNext,
        .initial = true,
        .user_logspace = user_logspace,
        .user_tag = query_tag,
        .query_seqnum = query_seqnum,
        .metalog_progress = *metalog_progress,
    };
    faas::log::IndexDataManager::QueryConsistencyType consistency_type =
        this_->CheckConsistency(query);
    switch (consistency_type) {
        case faas::log::IndexDataManager::QueryConsistencyType::kInitFutureViewBail:
            return -1;
        case faas::log::IndexDataManager::QueryConsistencyType::kInitCurrentViewPending:
            return -2;
        case faas::log::IndexDataManager::QueryConsistencyType::kInitCurrentViewOK:
        case faas::log::IndexDataManager::QueryConsistencyType::kInitPastViewOK: {
            faas::log::IndexQueryResult result = this_->ProcessReadNext(query);
            if (result.state == faas::log::IndexQueryResult::kFound) {
                *metalog_progress = result.metalog_progress;
                *seqnum = result.found_result.seqnum;
            }
            return static_cast<int>(result.state);
        }
        case faas::log::IndexDataManager::QueryConsistencyType::kContOK:
        default:
            return -3;
    }
}

int ProcessReadPrev(void* index_data, /*InOut*/ uint64_t* metalog_progress,
                    uint32_t user_logspace, uint64_t query_seqnum,
                    uint64_t query_tag, /*Out*/ uint64_t* seqnum) {
    auto this_ = reinterpret_cast<faas::log::IndexDataManager*>(index_data);
    faas::log::IndexQuery query = faas::log::IndexQuery {
        .direction = faas::log::IndexQuery::kReadPrev,
        .initial = true,
        .user_logspace = user_logspace,
        .user_tag = query_tag,
        .query_seqnum = query_seqnum,
        .metalog_progress = *metalog_progress,
    };
    faas::log::IndexDataManager::QueryConsistencyType consistency_type =
        this_->CheckConsistency(query);
    switch (consistency_type) {
        case faas::log::IndexDataManager::QueryConsistencyType::kInitFutureViewBail:
            return -1;
        case faas::log::IndexDataManager::QueryConsistencyType::kInitCurrentViewPending:
            return -2;
        case faas::log::IndexDataManager::QueryConsistencyType::kInitCurrentViewOK:
        case faas::log::IndexDataManager::QueryConsistencyType::kInitPastViewOK: {
            faas::log::IndexQueryResult result = this_->ProcessReadPrev(query);
            if (result.state == faas::log::IndexQueryResult::kFound) {
                *metalog_progress = result.metalog_progress;
                *seqnum = result.found_result.seqnum;
            }
            return static_cast<int>(result.state);
        }
        case faas::log::IndexDataManager::QueryConsistencyType::kContOK:
        default:
            return -3;
    }
}
