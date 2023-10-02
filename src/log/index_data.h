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
// shared with user function to direct read on the index data.
class IndexDataManager {
public:
    IndexDataManager(uint32_t logspace_id);

    PerSpaceIndex* GetOrCreateIndex(uint32_t user_logspace);
    bool IndexFindNext(const IndexQuery& query, uint64_t* seqnum, uint16_t* engine_id);
    bool IndexFindPrev(const IndexQuery& query, uint64_t* seqnum, uint16_t* engine_id);

private:
    std::string log_header_;
    uint32_t logspace_id_;
    absl::flat_hash_map</* user_logspace */ uint32_t,
                        std::unique_ptr<PerSpaceIndex>> index_;

    DISALLOW_COPY_AND_ASSIGN(IndexDataManager);
};

}  // namespace log
}  // namespace faas
