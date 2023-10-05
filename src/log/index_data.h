// TODO: put all status on shm
// [x] 1. engine_ids_
// [DEPRECATED] 2. index_
// 3. indexed_seqnum_position_
// 4. indexed_metalog_position_
// [x] 5. seqnum_by_localid_
#pragma once

#include "log/common.h"
#include "log/index_types.h"

namespace faas {
namespace log {
using namespace boost::interprocess;

typedef managed_shared_memory::segment_manager segment_manager_t;
typedef allocator<void, segment_manager_t> void_allocator_t;
typedef allocator<uint32_t, segment_manager_t> uint32_allocator_t;

// log engine id map allocator
typedef std::pair<const uint32_t, uint16_t> log_engine_id_map_value_type_t;
typedef allocator<log_engine_id_map_value_type_t, segment_manager_t> log_engine_id_map_value_type_allocator_t;
typedef boost::unordered_map<uint32_t, uint16_t, boost::hash<uint32_t>,
                             std::equal_to<uint32_t>, log_engine_id_map_value_type_allocator_t>
    log_engine_id_map_t;

// log stream allocator
typedef vector<uint32_t, uint32_allocator_t> log_stream_vec_t;
typedef std::pair<const uint64_t, log_stream_vec_t> log_stream_map_value_type_t;
typedef allocator<log_stream_map_value_type_t, segment_manager_t> log_stream_map_value_type_allocator_t;
// typedef flat_map<uint64_t, log_stream_vec_t, std::less<uint64_t>, log_stream_map_value_type_allocator_t> log_stream_map_t;
typedef boost::unordered_map<uint64_t, log_stream_vec_t, boost::hash<uint64_t>,
                             std::equal_to<uint64_t>, log_stream_map_value_type_allocator_t>
    log_stream_map_t;

// log async index allocator
typedef std::pair<const uint64_t, uint32_t> log_async_index_map_value_type_t;
typedef allocator<log_async_index_map_value_type_t, segment_manager_t> log_async_index_map_value_type_allocator_t;
typedef boost::unordered_map<uint64_t, uint32_t, boost::hash<uint64_t>,
                             std::equal_to<uint64_t>, log_async_index_map_value_type_allocator_t>
    log_async_index_map_t;

class PerSpaceIndex {
public:
    // TODO: should have different implementation on user func and engine
    // user func: open shm
    // engine: create shm
    PerSpaceIndex(uint32_t logspace_id, uint32_t user_logspace);
    ~PerSpaceIndex();

    void Add(uint64_t localid, uint32_t seqnum_lowhalf, uint16_t engine_id,
             const UserTagVec& user_tags);

    bool FindPrev(uint64_t query_seqnum, uint64_t user_tag,
                  uint64_t* seqnum, uint16_t* engine_id) const;
    bool FindNext(uint64_t query_seqnum, uint64_t user_tag,
                  uint64_t* seqnum, uint16_t* engine_id) const;
    bool FindLocalId(uint64_t localid, uint64_t* seqnum, uint16_t* engine_id) const;

private:
    uint32_t logspace_id_;
    uint32_t user_logspace_;
    // shm allocator
    managed_shared_memory segment_;
    void_allocator_t alloc_inst_;

    // absl::flat_hash_map</* seqnum */ uint32_t, uint16_t> engine_ids_;
    log_engine_id_map_t engine_ids_;
    // std::vector<uint32_t> seqnums_;
    log_stream_vec_t seqnums_;
    // absl::flat_hash_map</* tag */ uint64_t, std::vector<uint32_t>> seqnums_by_tag_;
    log_stream_map_t seqnums_by_tag_;

    // updated when receiving an index, used to serve async log query
    // std::unordered_map</*local_id*/ uint64_t, /*seqnum_lowhalf*/ uint32_t>
    //     seqnum_by_localid_;
    log_async_index_map_t seqnum_by_localid_;

    bool FindPrev(const log_stream_vec_t& seqnums, uint64_t query_seqnum,
                  uint32_t* result_seqnum) const;
    bool FindNext(const log_stream_vec_t& seqnums, uint64_t query_seqnum,
                  uint32_t* result_seqnum) const;

    DISALLOW_COPY_AND_ASSIGN(PerSpaceIndex);
};

// ----------------------------------------------------------------------------

// A wrapper holding all index datas for reading and writing.
// Separate data accessing interface from control flow, so the module can be
// shared with user functions to direct read on the index data.
class IndexDataManager {
public:
    IndexDataManager(uint32_t logspace_id);

    uint16_t view_id() const { return bits::HighHalf32(logspace_id_); }
    uint32_t indexed_seqnum_position() const {
        return indexed_seqnum_position_;
    }
    void set_indexed_seqnum_position(uint32_t indexed_seqnum_position) {
        indexed_seqnum_position_ = indexed_seqnum_position;
    }
    uint32_t indexed_metalog_position() const {
        return indexed_metalog_position_;
    }
    void set_indexed_metalog_position(uint32_t indexed_metalog_position) {
        indexed_metalog_position_ = indexed_metalog_position;
    }

    uint64_t index_metalog_progress() const {
        return bits::JoinTwo32(logspace_id_, indexed_metalog_position_);
    }

    // Used by engine
    void AddIndexData(uint32_t user_logspace, uint64_t localid,
                      uint32_t seqnum_lowhalf, uint16_t engine_id,
                      const UserTagVec& user_tags);

    enum QueryConsistencyType {
        kInitFutureViewBail,
        kInitPastViewOK,
        kInitCurrentViewPending,
        kInitCurrentViewOK,
        kContOK,
    };
    QueryConsistencyType CheckConsistency(const IndexQuery& query);
    // Used by engine and shared library
    IndexQueryResult ProcessLocalIdQuery(const IndexQuery& query);
    IndexQueryResult ProcessReadNext(const IndexQuery& query);
    IndexQueryResult ProcessReadPrev(const IndexQuery& query);
    IndexQueryResult ProcessBlockingQuery(const IndexQuery& query);

    // Index requires this function to handle timeout
    IndexQueryResult BuildNotFoundResult(const IndexQuery& query);

private:
    std::string log_header_;
    uint32_t logspace_id_;
    absl::flat_hash_map</* user_logspace */ uint32_t,
                        std::unique_ptr<PerSpaceIndex>> index_;
    uint32_t indexed_seqnum_position_;
    uint32_t indexed_metalog_position_;

    bool IndexFindNext(const IndexQuery& query, uint64_t* seqnum, uint16_t* engine_id);
    bool IndexFindPrev(const IndexQuery& query, uint64_t* seqnum, uint16_t* engine_id);
    bool IndexFindLocalId(const IndexQuery& query, uint64_t* seqnum, uint16_t* engine_id);

    // TODO: should have different implementation on user func and engine
    // user func: install view from shm
    // engine: allocate on the heap
    PerSpaceIndex* GetOrCreateIndex(uint32_t user_logspace);

    IndexQueryResult BuildFoundResult(const IndexQuery& query, uint16_t view_id,
                                      uint64_t seqnum, uint16_t engine_id);
    IndexQueryResult BuildPendingResult(const IndexQuery& query);
    IndexQueryResult BuildContinueResult(const IndexQuery& query, bool found,
                                         uint64_t seqnum, uint16_t engine_id);

    DISALLOW_COPY_AND_ASSIGN(IndexDataManager);
};

}  // namespace log
}  // namespace faas
