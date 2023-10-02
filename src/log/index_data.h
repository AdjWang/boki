#pragma once

#include "log/common.h"
#include "log/index_types.h"

namespace faas {
namespace log {

class PerSpaceIndex {
public:
    PerSpaceIndex(uint32_t logspace_id, uint32_t user_logspace);
    ~PerSpaceIndex() {}

    void Add(uint32_t seqnum_lowhalf, uint16_t engine_id, const UserTagVec& user_tags);

    bool FindPrev(uint64_t query_seqnum, uint64_t user_tag,
                  uint64_t* seqnum, uint16_t* engine_id) const;
    bool FindNext(uint64_t query_seqnum, uint64_t user_tag,
                  uint64_t* seqnum, uint16_t* engine_id) const;

private:
    uint32_t logspace_id_;
    uint32_t user_logspace_;

    absl::flat_hash_map</* seqnum */ uint32_t, uint16_t> engine_ids_;
    std::vector<uint32_t> seqnums_;
    absl::flat_hash_map</* tag */ uint64_t, std::vector<uint32_t>> seqnums_by_tag_;

    bool FindPrev(const std::vector<uint32_t>& seqnums, uint64_t query_seqnum,
                  uint32_t* result_seqnum) const;
    bool FindNext(const std::vector<uint32_t>& seqnums, uint64_t query_seqnum,
                  uint32_t* result_seqnum) const;

    DISALLOW_COPY_AND_ASSIGN(PerSpaceIndex);
};

// A wrapper holding all index datas for reading and writing.
// Separate data accessing interface from control flow, so the module can be
// shared with user functions to direct read on the index data.
class IndexDataManager {
public:
    IndexDataManager(uint32_t logspace_id);

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

    PerSpaceIndex* GetOrCreateIndex(uint32_t user_logspace);
    bool IndexFindNext(const IndexQuery& query, uint64_t* seqnum, uint16_t* engine_id);
    bool IndexFindPrev(const IndexQuery& query, uint64_t* seqnum, uint16_t* engine_id);

    void AddAsyncIndexData(uint64_t localid, uint32_t seqnum_lowhalf, UserTagVec user_tags);
    bool IndexFindLocalId(uint64_t localid, uint64_t* seqnum);

private:
    std::string log_header_;
    uint32_t logspace_id_;
    absl::flat_hash_map</* user_logspace */ uint32_t,
                        std::unique_ptr<PerSpaceIndex>> index_;
    uint32_t indexed_seqnum_position_;
    uint32_t indexed_metalog_position_;

    // updated when receiving an index, used to serve async log query
    struct AsyncIndexData {
        uint64_t seqnum;
        UserTagVec user_tags;
    };
    std::map</* local_id */ uint64_t, AsyncIndexData> log_index_map_;

    DISALLOW_COPY_AND_ASSIGN(IndexDataManager);
};

}  // namespace log
}  // namespace faas
