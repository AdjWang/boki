#include "log/cache.h"

namespace tkrzw {
Status CacheDBMUpdateLogger::WriteSet(std::string_view key,
                                         std::string_view value) {
    std::lock_guard lock(mutex_);
    ops_.emplace_back(OpType::OP_SET, key);
    return Status(Status::Code::SUCCESS);
}

Status CacheDBMUpdateLogger::WriteRemove(std::string_view key) {
    std::lock_guard lock(mutex_);
    ops_.emplace_back(OpType::OP_REMOVE, key);
    return Status(Status::Code::SUCCESS);
}

Status CacheDBMUpdateLogger::WriteClear() {
    return Status(Status::Code::NOT_IMPLEMENTED_ERROR);
}

std::vector<std::pair<CacheDBMUpdateLogger::OpType, std::string /*key*/>>
CacheDBMUpdateLogger::PopUpdates() {
    std::lock_guard lock(mutex_);
    std::vector<std::pair<OpType, std::string /*key*/>> ops(ops_);
    ops_.clear();
    return ops;
}
}  // namespace tkrzw

namespace faas {
namespace log {

LRUCache::LRUCache(int mem_cap_mb) {
    int64_t cap_mem_size = -1;
    if (mem_cap_mb > 0) {
        cap_mem_size = int64_t{mem_cap_mb} << 20;
    }
    dbm_.reset(new tkrzw::CacheDBM(/* cap_rec_num= */ -1, cap_mem_size));
    ulogger_ = new tkrzw::CacheDBMUpdateLogger();
    CHECK(ulogger_ != nullptr);
    dbm_->SetUpdateLogger(ulogger_);
}

LRUCache::~LRUCache() {
    if (ulogger_ != nullptr) {
        delete ulogger_;
        ulogger_ = nullptr;
    }
}

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

void LRUCache::Put(const LogMetaData& log_metadata,
                   std::span<const uint64_t> user_tags,
                   std::span<const char> log_data) {
    std::string key_str = fmt::format("0_{:016x}", log_metadata.seqnum);
    std::string data = EncodeLogEntry(log_metadata, user_tags, log_data);
    dbm_->Set(key_str, data, /* overwrite= */ false);
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

void LRUCache::PutAuxData(uint32_t user_logspace, uint64_t seqnum,
                          std::span<const char> data) {
    std::string key_str = fmt::format("1_{:016x}_{:08x}",
                                      seqnum, user_logspace);
    dbm_->Set(key_str, std::string_view(data.data(), data.size()),
              /* overwrite= */ true);
}

std::optional<std::string> LRUCache::GetAuxData(uint32_t user_logspace,
                                                uint64_t seqnum) {
    std::string key_str = fmt::format("1_{:016x}_{:08x}",
                                      seqnum, user_logspace);
    std::string data;
    auto status = dbm_->Get(key_str, &data);
    if (status.IsOK()) {
        return data;
    } else {
        return std::nullopt;
    }
}

std::optional<std::vector<LRUCache::IndexUpdate>> LRUCache::PopAuxUpdates() {
    std::vector<std::pair<tkrzw::CacheDBMUpdateLogger::OpType,
                std::string /*key*/>> updates = ulogger_->PopUpdates();
    if (updates.size() == 0) {
        return std::nullopt;
    }
    // drop duplicates
    absl::flat_hash_set<uint64_t> seqnum_add;
    absl::flat_hash_set<uint64_t> seqnum_remove;
    absl::flat_hash_map<uint64_t, uint32_t> seqnum_to_logspace; // user_logspace
    for (const auto& [op_type, seqnum_key] : updates) {
        DCHECK(seqnum_key != "");
        // only aux updates, heading with "1_"
        if (seqnum_key[0] == '0') {
            continue;
        }
        // +2 to skip "1_"
        uint64_t seqnum = std::strtoul(seqnum_key.substr(2, 16).c_str(), nullptr, 16);
        uint32_t user_logspace = static_cast<uint32_t>(
            std::strtoul(seqnum_key.substr(18, 8).c_str(), nullptr, 16));
        seqnum_to_logspace[seqnum] = user_logspace;
        if (op_type == tkrzw::CacheDBMUpdateLogger::OpType::OP_SET) {
            seqnum_remove.erase(seqnum);
            seqnum_add.emplace(seqnum);
        } else if (op_type == tkrzw::CacheDBMUpdateLogger::OpType::OP_REMOVE) {
            seqnum_remove.emplace(seqnum);
            seqnum_add.erase(seqnum);
        } else if (op_type == tkrzw::CacheDBMUpdateLogger::OpType::OP_CLEAR) {
            NOT_IMPLEMENTED();
        } else {
            UNREACHABLE();
        }
    }
    // make result
    std::vector<LRUCache::IndexUpdate> result;
    for (const uint64_t seqnum : seqnum_add) {
        DCHECK(seqnum_to_logspace.contains(seqnum));
        uint32_t user_logspace = seqnum_to_logspace[seqnum];
        result.emplace_back(tkrzw::CacheDBMUpdateLogger::OpType::OP_SET,
                            user_logspace, seqnum);
    }
    for (const uint64_t seqnum : seqnum_remove) {
        DCHECK(seqnum_to_logspace.contains(seqnum));
        uint32_t user_logspace = seqnum_to_logspace[seqnum];
        result.emplace_back(tkrzw::CacheDBMUpdateLogger::OpType::OP_REMOVE,
                            user_logspace, seqnum);
    }
    return result;
}


// ShardedLRUCache::ShardedLRUCache(int mem_cap_mb)
//     : log_header_("LogCache[]: "),
//       mem_cap_mb_(mem_cap_mb) {}
// 
// ShardedLRUCache::~ShardedLRUCache() {}
// 
// void ShardedLRUCache::PutAuxData(uint64_t tag, uint64_t seqnum, std::span<const char> data) {
//     HVLOG_F(1, "ShardedLRUCache::PutAuxData tag={}, seqnum=0x{:016X}", tag, seqnum);
//     std::string key_str = fmt::format("1_{:016x}", seqnum);
//     GetOrCreateDBM(tag)->Set(key_str, std::string_view(data.data(), data.size()),
//                              /* overwrite= */ true);
// }
// 
// std::optional<std::string> ShardedLRUCache::GetAuxData(uint32_t tag, uint64_t seqnum) {
//     std::string key_str = fmt::format("1_{:016x}", seqnum);
//     std::string data;
//     tkrzw::CacheDBM* dbm = GetDBM(tag);
//     int64_t count = 0;
//     if (dbm == nullptr || dbm->Count(&count) != tkrzw::Status::SUCCESS || count == 0) {
//         HVLOG_F(1, "ShardedLRUCache::GetAuxData tag={}, seqnum=0x{:016X}, found=false", tag, seqnum);
//         return std::nullopt;
//     }
//     auto status = dbm->Get(key_str, &data);
//     HVLOG_F(1, "ShardedLRUCache::GetAuxData tag={}, seqnum=0x{:016X}, found={}", tag, seqnum, status.IsOK());
//     if (status.IsOK()) {
//         return data;
//     } else {
//         return std::nullopt;
//     }
// }
// 
// std::optional<std::pair<std::uint64_t, std::string>> ShardedLRUCache::GetPrevAuxData(uint64_t tag) {
//     tkrzw::CacheDBM* dbm = GetDBM(tag);
//     int64_t count = 0;
//     if (dbm == nullptr || !dbm->Count(&count).IsOK() || count == 0) {
//         HVLOG_F(1, "ShardedLRUCache::GetTailAuxData tag={}, found=false", tag);
//         return std::nullopt;
//     }
//     bool found = false;
//     std::string max_seqnum(fmt::format("1_{:016x}", 0));
//     std::string data;
//     std::unique_ptr<tkrzw::DBM::Iterator> iter = dbm->MakeIterator();
//     iter->First();
//     std::string key, value;
//     HVLOG_F(1, "ShardedLRUCache::GetTailAuxData iter count={}", count);
//     while (iter->Get(&key, &value).IsOK()) {
//         // HVLOG_F(1, "ShardedLRUCache::GetTailAuxData iter recs key={}, value={}", key, value);
//         if (max_seqnum.compare(key) <= 0) {
//             found = true;
//             max_seqnum = key;
//             data = value;
//         }
//         iter->Next();
//     }
//     if (found) {
//         DCHECK(data != "");
//         char *end = nullptr;
//         // +2 to skip "1_"
//         uint64_t seqnum = std::strtoul((max_seqnum.c_str()+2), &end, 16);
//         HVLOG_F(1, "ShardedLRUCache::GetTailAuxData tag={}, found: seqnum=0x{:016X}", tag, seqnum);
//         return std::make_pair(seqnum, data);
//     } else {
//         HVLOG_F(1, "ShardedLRUCache::GetTailAuxData tag={}, found=false", tag);
//         return std::nullopt;
//     }
// }
// 
// tkrzw::CacheDBM* ShardedLRUCache::GetDBM(uint64_t tag) {
//     absl::ReaderMutexLock lk(&mu_);
//     if (dbs_.contains(tag)) {
//         return dbs_.at(tag).get();
//     } else {
//         return nullptr;
//     }
// }
// 
// tkrzw::CacheDBM* ShardedLRUCache::GetOrCreateDBM(uint64_t tag) {
//     tkrzw::CacheDBM* dbm = GetDBM(tag);
//     if (dbm != nullptr) {
//         return dbm;
//     } else {
//         int64_t cap_mem_size = -1;
//         if (mem_cap_mb_ > 0) {
//             cap_mem_size = int64_t{mem_cap_mb_} << 20;
//         }
//         HVLOG_F(1, "ShardedLRUCache::GetOrCreateDBM new CacheDBM for tag={}, cap={}", tag, cap_mem_size);
//         dbm = new tkrzw::CacheDBM(/* cap_rec_num= */ -1, cap_mem_size);
//         {
//             absl::MutexLock lk(&mu_);
//             if (!dbs_.contains(tag)) {
//                 dbs_[tag].reset(DCHECK_NOTNULL(dbm));
//                 return dbm;
//             } else {
//                 delete dbm;
//                 return dbs_.at(tag).get();
//             }
//         }
//     }
// }

}  // namespace log
}  // namespace faas
