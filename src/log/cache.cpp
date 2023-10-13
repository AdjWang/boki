#include "log/cache.h"

namespace faas {
namespace log {

LRUCache::LRUCache(uint32_t user_logspace, int mem_cap_mb)
    : log_header_(fmt::format("LRUCache[{}]: ", user_logspace)) {
    int64_t cap_mem_size = -1;
    if (mem_cap_mb > 0) {
        cap_mem_size = int64_t{mem_cap_mb} << 20;
    }
    dbm_.reset(new tkrzw::CacheDBM(/* cap_rec_num= */ -1, cap_mem_size));
}

LRUCache::~LRUCache() {}

namespace {
static inline std::string EncodeLogEntry(const LogMetaData& log_metadata,
                                         std::span<const uint64_t> user_tags,
                                         std::span<const char> log_data) {
    DCHECK_EQ(log_metadata.num_tags, user_tags.size());
    DCHECK_EQ(log_metadata.data_size, log_data.size());
    size_t total_size = log_data.size()
                      + user_tags.size() * sizeof(uint64_t)
                      + sizeof(LogMetaData);
    std::string encoded;
    encoded.resize(total_size);
    char* ptr = encoded.data();
    DCHECK_GT(log_data.size(), 0U);
    memcpy(ptr, log_data.data(), log_data.size());
    ptr += log_data.size();
    if (user_tags.size() > 0) {
        memcpy(ptr, user_tags.data(), user_tags.size() * sizeof(uint64_t));
        ptr += user_tags.size() * sizeof(uint64_t);
    }
    memcpy(ptr, &log_metadata, sizeof(LogMetaData));
    return encoded;
}

static inline void DecodeLogEntry(std::string encoded, LogEntry* log_entry) {
    DCHECK_GT(encoded.size(), sizeof(LogMetaData));
    LogMetaData& metadata = log_entry->metadata;
    memcpy(&metadata,
           encoded.data() + encoded.size() - sizeof(LogMetaData),
           sizeof(LogMetaData));
    size_t total_size = metadata.data_size
                      + metadata.num_tags * sizeof(uint64_t)
                      + sizeof(LogMetaData);
    DCHECK_EQ(total_size, encoded.size());
    if (metadata.num_tags > 0) {
        std::span<const uint64_t> user_tags(
            reinterpret_cast<const uint64_t*>(encoded.data() + metadata.data_size),
            metadata.num_tags);
        log_entry->user_tags.assign(user_tags.begin(), user_tags.end());
    } else {
        log_entry->user_tags.clear();
    }
    encoded.resize(metadata.data_size);
    log_entry->data = std::move(encoded);
}
}  // namespace

void LRUCache::Put(const LogMetaData& log_metadata, std::span<const uint64_t> user_tags,
                   std::span<const char> log_data) {
    std::string key_str = fmt::format("0_{:016x}", log_metadata.seqnum);
    std::string data = EncodeLogEntry(log_metadata, user_tags, log_data);
    dbm_->Set(key_str, data, /* overwrite= */ true);
}

std::optional<LogEntry> LRUCache::Get(uint64_t seqnum) {
    std::string key_str = fmt::format("0_{:016x}", seqnum);
    std::string data;
    auto status = dbm_->Get(key_str, &data);
    if (status.IsOK()) {
        LogEntry log_entry;
        DecodeLogEntry(std::move(data), &log_entry);
        DCHECK_EQ(seqnum, log_entry.metadata.seqnum);
        return log_entry;
    } else {
        return std::nullopt;
    }
}

void LRUCache::PutAuxData(uint64_t seqnum, std::span<const char> data) {
    std::string key_str = fmt::format("1_{:016x}", seqnum);
    dbm_->Set(key_str, std::string_view(data.data(), data.size()),
              /* overwrite= */ true);
}

std::optional<std::string> LRUCache::GetAuxData(uint64_t seqnum) {
    std::string key_str = fmt::format("1_{:016x}", seqnum);
    std::string data;
    auto status = dbm_->Get(key_str, &data);
    if (status.IsOK()) {
        return data;
    } else {
        return std::nullopt;
    }
}


SharedLRUCache::SharedLRUCache(uint32_t user_logspace, int mem_cap_mb,
                               std::string_view path)
    : log_header_(fmt::format("SharedLRUCache[{}]: ", user_logspace)) {
    int64_t cap_mem_size = -1;
    if (mem_cap_mb > 0) {
        cap_mem_size = int64_t{mem_cap_mb} << 20;
    }
    HVLOG_F(1, "Construct user_logspace={} cap_size={} path={}",
               user_logspace, cap_mem_size, path);
    auto lru_cache = std::unique_ptr<boost::interprocess::LRUCache>(
        new boost::interprocess::LRUCache(size_t(cap_mem_size), path));
    DCHECK(lru_cache != nullptr);
    lockable_dbm_ = LockablePtr<boost::interprocess::LRUCache>(
        std::move(lru_cache),
        ipc::GetCacheMutexName(user_logspace));
}

void SharedLRUCache::Put(const LogMetaData& log_metadata,
                         std::span<const uint64_t> user_tags,
                         std::span<const char> log_data) {
    auto dbm = lockable_dbm_.Lock();
    std::string key_str = fmt::format("0_{:016x}", log_metadata.seqnum);
    std::string data = EncodeLogEntry(log_metadata, user_tags, log_data);
    // DEBUG
    // HVLOG_F(1, "Put seqnum={:016X} data size={}", log_metadata.seqnum, data.size());

    dbm->insert(key_str, data);
}

std::optional<LogEntry> SharedLRUCache::Get(uint64_t seqnum) {
    auto dbm = lockable_dbm_.Lock();
    std::string key_str = fmt::format("0_{:016x}", seqnum);
    auto data = dbm->get(key_str);
    if (data.has_value()) {
        // DEBUG
        // HVLOG_F(1, "Get seqnum={:016X} data size={}", seqnum, data.value().size());

        LogEntry log_entry;
        DecodeLogEntry(std::move(data.value()), &log_entry);
        DCHECK_EQ(seqnum, log_entry.metadata.seqnum);
        return log_entry;
    } else {
        // DEBUG
        // HVLOG_F(1, "Get seqnum={:016X} data size=0", seqnum);
        return std::nullopt;
    }
}

void SharedLRUCache::PutAuxData(uint64_t seqnum, std::span<const char> data) {
    // DEBUG
    // HVLOG_F(1, "PutAuxData seqnum={:016X} data size={}", seqnum, data.size());
    auto dbm = lockable_dbm_.Lock();
    std::string key_str = fmt::format("1_{:016x}", seqnum);
    dbm->insert(key_str, std::string(data.data(), data.size()));
}

std::optional<std::string> SharedLRUCache::GetAuxData(uint64_t seqnum) {
    auto dbm = lockable_dbm_.Lock();
    std::string key_str = fmt::format("1_{:016x}", seqnum);
    auto data = dbm->get(key_str);
    if (data.has_value()) {
        // DEBUG
        // HVLOG_F(1, "GetAuxData seqnum={:016X} data size={}", seqnum, data.value().size());
        return data.value();
    } else {
        // DEBUG
        // HVLOG_F(1, "GetAuxData seqnum={:016X} data size=0", seqnum);
        return std::nullopt;
    }
}

}  // namespace log
}  // namespace faas
