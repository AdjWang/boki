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

PerSpaceIndex* IndexDataManager::GetOrCreateIndex(uint32_t user_logspace) {
    if (index_.contains(user_logspace)) {
        return index_.at(user_logspace).get();
    }
    HVLOG_F(1, "Create index of user logspace {}", user_logspace);
    PerSpaceIndex* index = new PerSpaceIndex(logspace_id_, user_logspace);
    index_[user_logspace].reset(index);
    return index;
}

bool IndexDataManager::IndexFindNext(const IndexQuery& query, uint64_t* seqnum, uint16_t* engine_id) {
    return IndexFindNext(query.direction, query.user_logspace,
                         query.query_seqnum, query.user_tag, seqnum, engine_id);
}

bool IndexDataManager::IndexFindPrev(const IndexQuery& query, uint64_t* seqnum, uint16_t* engine_id) {
    return IndexFindPrev(query.direction, query.user_logspace,
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

bool IndexDataManager::IndexFindNext(IndexQuery::ReadDirection direction,
                                     uint32_t user_logspace,
                                     uint64_t query_seqnum, uint64_t query_tag,
                                     uint64_t* seqnum, uint16_t* engine_id) {
    DCHECK(direction == IndexQuery::kReadNext ||
           direction == IndexQuery::kReadNextB);
    if (!index_.contains(user_logspace)) {
        return false;
    }
    return GetOrCreateIndex(user_logspace)->FindNext(query_seqnum, query_tag, seqnum, engine_id);
}

bool IndexDataManager::IndexFindPrev(IndexQuery::ReadDirection direction,
                                     uint32_t user_logspace,
                                     uint64_t query_seqnum, uint64_t query_tag,
                                     uint64_t* seqnum, uint16_t* engine_id) {
    DCHECK(direction == IndexQuery::kReadPrev);
    if (!index_.contains(user_logspace)) {
        return false;
    }
    return GetOrCreateIndex(user_logspace)->FindPrev(query_seqnum, query_tag, seqnum, engine_id);
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

bool IndexFindNext(void* index_data, uint8_t direction,
                   uint32_t user_logspace, uint64_t query_seqnum,
                   uint64_t query_tag, uint64_t* seqnum, uint16_t* engine_id) {
    return reinterpret_cast<faas::log::IndexDataManager*>(index_data)->IndexFindNext(
        static_cast<faas::log::IndexQuery::ReadDirection>(direction),
        user_logspace, query_seqnum, query_tag, seqnum, engine_id);
}
bool IndexFindPrev(void* index_data, uint8_t direction,
                   uint32_t user_logspace, uint64_t query_seqnum,
                   uint64_t query_tag, uint64_t* seqnum, uint16_t* engine_id) {
    return reinterpret_cast<faas::log::IndexDataManager*>(index_data)->IndexFindPrev(
        static_cast<faas::log::IndexQuery::ReadDirection>(direction),
        user_logspace, query_seqnum, query_tag, seqnum, engine_id);
}

bool IndexFindLocalId(void* index_data, uint64_t localid, uint64_t* seqnum) {
    return reinterpret_cast<faas::log::IndexDataManager*>(index_data)
        ->IndexFindLocalId(localid, seqnum);
}