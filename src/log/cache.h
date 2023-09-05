#pragma once

#include "log/common.h"

// Forward declarations
__BEGIN_THIRD_PARTY_HEADERS
#include <tkrzw_dbm_cache.h>
#include <tkrzw_dbm.h>
#include <tkrzw_thread_util.h>

#include <nlohmann/json.hpp>
using json = nlohmann::json;
__END_THIRD_PARTY_HEADERS

namespace tkrzw {
class CacheDBMUpdateLogger final : public DBM::UpdateLogger {
   public:
    /**
     * Enumeration for operation types.
     */
    enum OpType : int32_t {
        /** Invalid operation. */
        OP_VOID = 0,
        /** To modify or add a record. */
        OP_SET = 1,
        /** To remove a record. */
        OP_REMOVE = 2,
        /** To remove all records. */
        OP_CLEAR = 3,
    };
    static std::string OpTypeToString(OpType t) {
        switch (t) {
            case OP_VOID:
                return "OP_VOID";
            case OP_SET:
                return "OP_SET";
            case OP_REMOVE:
                return "OP_REMOVE";
            case OP_CLEAR:
                return "OP_CLEAR";
            default:
                return "OP_UNKNOWN";
        }
    }

    explicit CacheDBMUpdateLogger() {}
    virtual ~CacheDBMUpdateLogger() {}

    Status WriteSet(std::string_view key, std::string_view value) override;
    Status WriteRemove(std::string_view key) override;
    Status WriteClear() override;

    using UpdatesVec = absl::InlinedVector<std::pair<OpType, std::string /*key*/>, 4>;
    UpdatesVec PopUpdates();

   private:
    SpinMutex mutex_;
    UpdatesVec ops_;

    DISALLOW_COPY_AND_ASSIGN(CacheDBMUpdateLogger);
};
}  // namespace tkrzw

namespace faas {
namespace log {

class LRUCache {
public:
    explicit LRUCache(int mem_cap_mb);
    ~LRUCache();

    void Put(const LogMetaData& log_metadata, std::span<const uint64_t> user_tags,
             std::span<const char> log_data);
    std::optional<LogEntry> Get(uint64_t seqnum);

    void PutAuxData(uint64_t seqnum, std::span<const char> data);
    std::optional<std::string> GetAuxData(uint64_t seqnum);

private:
    std::unique_ptr<tkrzw::CacheDBM> dbm_;

    DISALLOW_COPY_AND_ASSIGN(LRUCache);
};


// Similar to Index::PerSpaceIndex, but:
// - remove view management, use uint64_t seqnum
// - remove engine_id management
// - use ordered set as seqnum_ container because LRUCache may deletes entries.
class AuxIndex {
public:
    explicit AuxIndex() {}
    virtual ~AuxIndex() {}

    void Add(uint64_t seqnum, uint64_t localid, uint64_t tag);
    void Remove(uint64_t seqnum, uint64_t tag);
    bool Contains(uint64_t seqnum) const;
    bool Contains(uint64_t seqnum, uint64_t tag) const;
    UserTagVec GetTags(uint64_t seqnum) const;
    uint64_t GetLocalid(uint64_t seqnum) const;

    bool FindPrev(uint64_t query_seqnum, uint64_t aux_tag,
                  uint64_t* seqnum) const;
    bool FindNext(uint64_t query_seqnum, uint64_t aux_tag,
                  uint64_t* seqnum) const;

    // DEBUG
    std::string Inspect() const;

   private:
    absl::flat_hash_map</* tag */ uint64_t, absl::btree_set<uint64_t>> seqnums_by_tag_;
    absl::flat_hash_map</* seqnum */ uint64_t, absl::btree_set<uint64_t>> tags_by_seqnum_;

    absl::flat_hash_map</*seqnum*/ uint64_t, /*local*/ uint64_t,
                        absl::Hash<uint64_t>> localid_by_seqnum_;

    bool FindPrev(const absl::btree_set<uint64_t>& seqnums, uint64_t query_seqnum,
                  uint64_t* result_seqnum) const;
    bool FindNext(const absl::btree_set<uint64_t>& seqnums, uint64_t query_seqnum,
                  uint64_t* result_seqnum) const;

    DISALLOW_COPY_AND_ASSIGN(AuxIndex);
};

class ShardedLRUCache {
public:
    explicit ShardedLRUCache(int mem_cap_mb);
    virtual ~ShardedLRUCache();

    void PutLogData(const LogMetaData& log_metadata,
                    std::span<const uint64_t> user_tags,
                    std::span<const char> log_data);
    std::optional<LogEntry> GetLogData(uint64_t seqnum) ABSL_NO_THREAD_SAFETY_ANALYSIS;

    void PutAuxData(const AuxEntry& aux_entry);
    void PutAuxData(uint64_t seqnum, uint64_t localid, uint64_t tag,
                    std::span<const char> aux_data);
    std::optional<AuxEntry> GetAuxData(uint64_t seqnum);
    std::optional<AuxEntry> GetAuxDataPrev(uint64_t tag, uint64_t seqnum);
    std::optional<AuxEntry> GetAuxDataNext(uint64_t tag, uint64_t seqnum);

private:
    std::string log_header_;

    // make index and dbm operations atomic
    absl::Mutex cache_mu_;
    std::unique_ptr<AuxIndex> aux_index_ ABSL_GUARDED_BY(cache_mu_);
    std::unique_ptr<tkrzw::CacheDBM> dbm_ ABSL_GUARDED_BY(cache_mu_);

    std::unique_ptr<tkrzw::CacheDBMUpdateLogger> ulogger_;

    std::optional<AuxEntry> GetAuxDataChecked(uint64_t seqnum, uint64_t tag) const ABSL_SHARED_LOCKS_REQUIRED(cache_mu_);
    // this function had been placed into the guarded scope.
    void UpdateCacheIndex() ABSL_EXCLUSIVE_LOCKS_REQUIRED(cache_mu_);

    DISALLOW_COPY_AND_ASSIGN(ShardedLRUCache);
};

}  // namespace log
}  // namespace faas
