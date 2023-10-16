#include "log/index_data.h"
#include "utils/fs.h"
#include "ipc/base.h"

namespace faas {
namespace log {

#define SHM_SEG_PATH \
    ipc::GetIndexSegmentName(user_logspace_, logspace_id_).c_str()

#define SHM_OBJECT_NAME(name) \
    ipc::GetIndexSegmentObjectName(name, user_logspace_, logspace_id_).c_str()

// TODO: set index size in args
#define ENGINE_SHM_INDEX_INITIALIZER_LIST                                       \
    segment_(create_only, SHM_SEG_PATH, 1000 * 1024 * 1024),                    \
    alloc_inst_(segment_.get_segment_manager()),                                \
    engine_ids_(segment_.construct<log_engine_id_map_t>                         \
        (SHM_OBJECT_NAME("EngineIdMap"))(0u, boost::hash<uint32_t>(),           \
                                       std::equal_to<uint32_t>(),               \
                                       alloc_inst_)),                           \
    seqnums_(segment_.construct<log_stream_vec_t>                               \
        (SHM_OBJECT_NAME("StreamVec"))(alloc_inst_)),                           \
    seqnums_by_tag_(segment_.construct<log_stream_map_t>                        \
        (SHM_OBJECT_NAME("StreamMap"))(0u, boost::hash<uint64_t>(),             \
                                     std::equal_to<uint64_t>(),                 \
                                     alloc_inst_)),                             \
    seqnum_by_localid_(segment_.construct<log_async_index_map_t>                \
        (SHM_OBJECT_NAME("AsyncIndexMap"))(0u, boost::hash<uint64_t>(),         \
                                         std::equal_to<uint64_t>(),             \
                                         alloc_inst_))

#define FAASFUNC_SHM_INDEX_INITIALIZER_LIST                                     \
    segment_(open_only, SHM_SEG_PATH),                                          \
    engine_ids_(segment_.find<log_engine_id_map_t>                              \
        (SHM_OBJECT_NAME("EngineIdMap")).first),                                \
    seqnums_(segment_.find<log_stream_vec_t>                                    \
        (SHM_OBJECT_NAME("StreamVec")).first),                                  \
    seqnums_by_tag_(segment_.find<log_stream_map_t>                             \
        (SHM_OBJECT_NAME("StreamMap")).first),                                  \
    seqnum_by_localid_(segment_.find<log_async_index_map_t>                     \
        (SHM_OBJECT_NAME("AsyncIndexMap")).first)

PerSpaceIndex::PerSpaceIndex(uint32_t logspace_id, uint32_t user_logspace)
    : logspace_id_(logspace_id),
      user_logspace_(user_logspace),
#if defined(__COMPILE_AS_SHARED)
      FAASFUNC_SHM_INDEX_INITIALIZER_LIST
#else
      ENGINE_SHM_INDEX_INITIALIZER_LIST
#endif
    {}

PerSpaceIndex::~PerSpaceIndex() {
#if !defined(__COMPILE_AS_SHARED)
    file_mapping::remove(SHM_SEG_PATH);
#endif
}

#undef FAASFUNC_SHM_INDEX_INITALIZER_LIST
#undef ENGINE_SHM_INDEX_INITALIZER_LIST
#undef SHM_OBJECT_NAME
#undef SHM_SEG_PATH

#if !defined(__COMPILE_AS_SHARED)
void PerSpaceIndex::Add(uint64_t localid, uint32_t seqnum_lowhalf, uint16_t engine_id,
                        const UserTagVec& user_tags) {
    DCHECK(seqnum_by_localid_->find(localid) == seqnum_by_localid_->end())
        << "Duplicate index_data.local_id for seqnum_by_localid_";
    seqnum_by_localid_->emplace(localid, seqnum_lowhalf);

    DCHECK(!engine_ids_->contains(seqnum_lowhalf));
    engine_ids_->emplace(seqnum_lowhalf, engine_id);

    DCHECK(seqnums_->empty() || seqnum_lowhalf > seqnums_->back());
    seqnums_->push_back(seqnum_lowhalf);

    // DEBUG
    DCHECK_EQ(engine_ids_->size(), seqnums_->size()) << Inspect();

    for (uint64_t user_tag : user_tags) {
        DCHECK_NE(user_tag, kEmptyLogTag);
        if (seqnums_by_tag_->contains(user_tag)) {
            seqnums_by_tag_->at(user_tag).push_back(seqnum_lowhalf);
        } else {
            vector<uint32_t, uint32_allocator_t> value_vec(alloc_inst_);
            value_vec.push_back(seqnum_lowhalf);
            seqnums_by_tag_->emplace(user_tag, value_vec);
        }
    }
}
#endif

bool PerSpaceIndex::FindPrev(uint64_t query_seqnum, uint64_t user_tag,
                             uint64_t* seqnum, uint16_t* engine_id) const {
    uint32_t seqnum_lowhalf;
    if (user_tag == kEmptyLogTag) {
        if (!FindPrev(*seqnums_, query_seqnum, &seqnum_lowhalf)) {
            return false;
        }
    } else {
        if (!seqnums_by_tag_->contains(user_tag)) {
            return false;
        }
        if (!FindPrev(seqnums_by_tag_->at(user_tag), query_seqnum, &seqnum_lowhalf)) {
            return false;
        }
    }
    DCHECK(engine_ids_->contains(seqnum_lowhalf))
        << fmt::format("seqnum_lowhalf={:08X}", seqnum_lowhalf)
        << Inspect();
    *seqnum = bits::JoinTwo32(logspace_id_, seqnum_lowhalf);
    DCHECK_LE(*seqnum, query_seqnum) << Inspect();
    *engine_id = engine_ids_->at(seqnum_lowhalf);
    return true;
}

bool PerSpaceIndex::FindNext(uint64_t query_seqnum, uint64_t user_tag,
                             uint64_t* seqnum, uint16_t* engine_id) const {
    uint32_t seqnum_lowhalf;
    if (user_tag == kEmptyLogTag) {
        if (!FindNext(*seqnums_, query_seqnum, &seqnum_lowhalf)) {
            return false;
        }
    } else {
        if (!seqnums_by_tag_->contains(user_tag)) {
            return false;
        }
        if (!FindNext(seqnums_by_tag_->at(user_tag), query_seqnum, &seqnum_lowhalf)) {
            return false;
        }
    }
    DCHECK(engine_ids_->contains(seqnum_lowhalf))
        << fmt::format("seqnum_lowhalf={:08X}", seqnum_lowhalf)
        << Inspect();
    *seqnum = bits::JoinTwo32(logspace_id_, seqnum_lowhalf);
    DCHECK_GE(*seqnum, query_seqnum) << Inspect();
    *engine_id = engine_ids_->at(seqnum_lowhalf);
    return true;
}

bool PerSpaceIndex::FindLocalId(uint64_t localid, uint64_t* seqnum, uint16_t* engine_id) const {
    auto it = seqnum_by_localid_->find(localid);
    if (it == seqnum_by_localid_->end()) {
        return false;
    } else {
        DCHECK_NE(seqnum, nullptr);
        uint32_t seqnum_lowhalf = it->second;
        *seqnum = bits::JoinTwo32(logspace_id_, seqnum_lowhalf);
        *engine_id = gsl::narrow_cast<uint16_t>(bits::HighHalf64(localid));
        return true;
    }
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

// DEBUG
template<class T>
static std::string VecToString(T vec) {
    std::string result("[");
    for (auto item : vec) {
        result.append(fmt::format("{:08X}, ", item));
    }
    result.append("]");
    return result;
}

std::string PerSpaceIndex::Inspect() const {
    if (engine_ids_->empty()) {
        LOG_F(INFO, "PerSpaceIndex_{} engine_ids_ is empty", user_logspace_);
    } else {
        LOG_F(INFO, "PerSpaceIndex_{} engine_ids_.size()={}", user_logspace_, engine_ids_->size());
        for (auto& [seqnum, engine_id] : *engine_ids_) {
            LOG_F(INFO, "PerSpaceIndex_{}: seqnum={:08X} engine_id={:04X}", user_logspace_, seqnum, engine_id);
        }
    }

    if (seqnums_->empty()) {
        LOG_F(INFO, "PerSpaceIndex_{} seqnums_ is empty", user_logspace_);
        DCHECK(seqnums_by_tag_->empty());
    } else {
        LOG_F(INFO, "PerSpaceIndex_{}: seqnums_.size()={} seqnums={}",
                    user_logspace_, seqnums_->size(), VecToString(*seqnums_));
        for (auto& [tag, seqnums] : *seqnums_by_tag_) {
            LOG_F(INFO, "PerSpaceIndex_{}: tag={} seqnums={}", user_logspace_, tag, VecToString(seqnums));
        }
    }
    return "";
}

// ----------------------------------------------------------------------------

IndexDataManager::IndexDataManager(uint32_t logspace_id)
    : log_header_(fmt::format("IndexDataManager[{}]: ", logspace_id)),
      logspace_id_(logspace_id),
      indexed_seqnum_position_(fs_utils::JoinPath(
          ipc::GetOrCreateIndexMetaPath(logspace_id), "indexed_seqnum_position")),
      indexed_metalog_position_(fs_utils::JoinPath(
          ipc::GetOrCreateIndexMetaPath(logspace_id), "indexed_metalog_position")) {
    // DEBUG
    indexed_seqnum_position_.Check();
    indexed_metalog_position_.Check();
}

#if defined(__COMPILE_AS_SHARED)
void IndexDataManager::LoadIndexData(uint32_t user_logspace) {
    GetOrCreateIndex(user_logspace);
}
#else
void IndexDataManager::AddIndexData(uint32_t user_logspace, uint64_t localid,
                                    uint32_t seqnum_lowhalf, uint16_t engine_id,
                                    const UserTagVec& user_tags) {
    // DEBUG
    HVLOG_F(1, "AddIndexData user_logspace={} localid={:016X} seqnum_lowhalf={:08X} engine_id={:04X}",
               user_logspace, localid, seqnum_lowhalf, engine_id);
    GetOrCreateIndex(user_logspace)->Add(localid, seqnum_lowhalf, engine_id, user_tags);
}
#endif

IndexDataManager::QueryConsistencyType IndexDataManager::CheckConsistency(const IndexQuery& query) {
    if (query.initial) {
        HVLOG(1) << "Receive initial query";
        uint16_t func_view_id = GetViewId(query.metalog_progress);
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

    // TODO: perform view check

    uint16_t engine_id = 0;
    uint64_t seqnum = kInvalidLogSeqNum;
    if (IndexFindLocalId(query, &seqnum, &engine_id)) {
        // HVLOG_F(1, "ProcessQuery: found async map from local_id=0x{:016X} to seqnum=0x{:016X}",
        //         local_id, seqnum);
        // BUG: WOW! A reaaaaaaaaly strange bug! Triggered so many times with the magic result 28524,
        // causing functions failed to send back the query result to the target engine node.
        DCHECK(query.origin_node_id != 28524) << utils::DumpStackTrace();

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
    HVLOG_F(1, "ProcessReadNext: metalog_progress={:016X} seqnum={:016X} user_logspace={} tag={}",
            query.metalog_progress, query.query_seqnum, query.user_logspace, query.user_tag);
    uint16_t query_view_id = GetViewId(query.query_seqnum);
    if (query_view_id > view_id()) {
        HVLOG(1) << "ProcessReadNext: NotFoundResult";
        return BuildNotFoundResult(query);
    }
    uint64_t seqnum;
    uint16_t engine_id;
    bool found = IndexFindNext(query, &seqnum, &engine_id);
    if (query_view_id == view_id()) {
        if (found) {
            HVLOG_F(1, "ProcessReadNext: FoundResult: seqnum={:016X}", seqnum);
            return BuildFoundResult(query, view_id(), seqnum, engine_id);
        } else {
            if (query.prev_found_result.seqnum != kInvalidLogSeqNum) {
                const IndexFoundResult& found_result = query.prev_found_result;
                HVLOG_F(1, "ProcessReadNext: FoundResult (from prev_result): seqnum={:016X}",
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
    HVLOG_F(1, "ProcessReadPrev: metalog_progress={:016X} seqnum={:016X} user_logspace={} tag={}",
            query.metalog_progress, query.query_seqnum, query.user_logspace, query.user_tag);
    uint16_t query_view_id = GetViewId(query.query_seqnum);
    if (query_view_id < view_id()) {
        HVLOG(1) << "ProcessReadPrev: ContinueResult";
        return BuildContinueResult(query, false, 0, 0);
    }
    uint64_t seqnum;
    uint16_t engine_id;
    bool found = IndexFindPrev(query, &seqnum, &engine_id);
    if (found) {
        HVLOG_F(1, "ProcessReadPrev: FoundResult: seqnum={:016X}", seqnum);
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
    uint16_t query_view_id = GetViewId(query.query_seqnum);
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

bool IndexDataManager::IndexFindLocalId(const IndexQuery& query, uint64_t* seqnum, uint16_t* engine_id) {
    DCHECK(query.direction == IndexQuery::kReadLocalId);
    if (!index_.contains(query.user_logspace)) {
        return false;
    }
    // replace seqnum if querying by localid
    uint64_t localid = query.query_seqnum;
    return GetOrCreateIndex(query.user_logspace)->FindLocalId(
        localid, seqnum, engine_id);
}

PerSpaceIndex* IndexDataManager::GetOrCreateIndex(uint32_t user_logspace) {
    if (index_.contains(user_logspace)) {
        return index_.at(user_logspace).get();
    }
#if defined(__COMPILE_AS_SHARED)
    HVLOG_F(1, "Open index of user_logspace={}", user_logspace);
    std::string index_path = ipc::GetIndexSegmentPath("IndexShm", user_logspace, logspace_id_);
    DCHECK(fs_utils::Exists(index_path)) << index_path;
#else
    HVLOG_F(1, "Create index of user_logspace={}", user_logspace);
#endif
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

// DEBUG
void IndexDataManager::Inspect() const {
    LOG(INFO) << ">>>>> Inspect index data";
    LOG_F(INFO, "indexed_seqnum_position={:016X}", indexed_seqnum_position());
    LOG_F(INFO, "indexed_metalog_position={:016X}", indexed_metalog_position());

    LOG_F(INFO, "user index size={}", index_.size());
    for (auto& [key, index] : index_) {
        LOG_F(INFO, "user index key={}", key);
        index->Inspect();
    }
    LOG(INFO) << "<<<<<";
}

}  // namespace log
}  // namespace faas
