#pragma once

#include "log/common.h"

__BEGIN_THIRD_PARTY_HEADERS
#include <tkrzw_dbm_cache.h>
#include <tkrzw_dbm_hash.h>
#include <tkrzw_file_mmap.h>
__END_THIRD_PARTY_HEADERS

// Forward declarations
namespace tkrzw { class CacheDBM; }

namespace faas {
namespace log {

class LRUCache {
public:
    explicit LRUCache(int mem_cap_mb);
    explicit LRUCache(int mem_cap_mb, const char* shared_path,
                      int32_t options = 0);
    LRUCache(LRUCache&& other) {
        dbm_ = std::move(other.dbm_);
    }
    ~LRUCache();

    void Put(const LogMetaData& log_metadata, std::span<const uint64_t> user_tags,
             std::span<const char> log_data);
    std::optional<LogEntry> Get(uint64_t seqnum);

    void PutAuxData(uint64_t seqnum, std::span<const char> data);
    std::optional<std::string> GetAuxData(uint64_t seqnum);

private:
    struct DeleteDBM {
        void operator()(tkrzw::CacheDBM* dbm) { dbm->Close(); }
    };
    std::unique_ptr<tkrzw::CacheDBM, DeleteDBM> dbm_;

    DISALLOW_COPY_AND_ASSIGN(LRUCache);
};

}  // namespace log
}  // namespace faas
