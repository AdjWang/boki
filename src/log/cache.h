#pragma once

#include "log/common.h"

// Forward declarations
namespace tkrzw { class CacheDBM; }

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

// TODO: make capacity unified among shareds
class ShardedLRUCache {
public:
    explicit ShardedLRUCache(int mem_cap_mb);
    ~ShardedLRUCache();

    void PutAuxData(uint64_t tag, uint64_t seqnum, std::span<const char> data);
    std::optional<std::string> GetAuxData(uint64_t tag, uint64_t seqnum);
    std::optional<std::pair<std::uint64_t, std::string>> GetLastAuxData(uint64_t tag);

private:
    std::string log_header_;
    int mem_cap_mb_;

    absl::Mutex mu_;
    absl::flat_hash_map</* tag */ uint64_t,
                        std::unique_ptr<tkrzw::CacheDBM>>
        dbs_ ABSL_GUARDED_BY(mu_);

    tkrzw::CacheDBM* GetDBM(uint64_t tag);
    tkrzw::CacheDBM* GetOrCreateDBM(uint64_t tag);

    DISALLOW_COPY_AND_ASSIGN(ShardedLRUCache);
};

}  // namespace log
}  // namespace faas
