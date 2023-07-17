#include "log/cache.h"
#include "log/utils.h"

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

CacheDBMUpdateLogger::UpdatesVec CacheDBMUpdateLogger::PopUpdates() {
    std::lock_guard lock(mutex_);
    CacheDBMUpdateLogger::UpdatesVec ops(std::move(ops_));
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
}

LRUCache::~LRUCache() {}

void LRUCache::Put(const LogMetaData& log_metadata, std::span<const uint64_t> user_tags,
                   std::span<const char> log_data) {
    std::string key_str = fmt::format("0_{:016x}", log_metadata.seqnum);
    std::string data = log_utils::EncodeEntry<LogMetaData>(log_metadata, user_tags, log_data);
    dbm_->Set(key_str, data, /* overwrite= */ false);
}

std::optional<LogEntry> LRUCache::Get(uint64_t seqnum) {
    std::string key_str = fmt::format("0_{:016x}", seqnum);
    std::string data;
    auto status = dbm_->Get(key_str, &data);
    if (status.IsOK()) {
        LogEntry log_entry;
        log_utils::DecodeEntry<LogMetaData, LogEntry>(std::move(data), &log_entry);
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

void AuxIndex::Add(std::span<const uint64_t> user_tags, uint64_t seqnum) {
    if (!user_tags.empty()) {
        for (const uint64_t user_tag : user_tags) {
            seqnums_by_tag_[user_tag].emplace(seqnum);
            tags_by_seqnum_[seqnum].emplace(user_tag);
        }
    } else {
        // use default empty tag
        seqnums_by_tag_[kEmptyLogTag].emplace(seqnum);
        tags_by_seqnum_[seqnum].emplace(kEmptyLogTag);
    }
}

void AuxIndex::Remove(uint64_t seqnum) {
    if (!tags_by_seqnum_.contains(seqnum)) {
        return;
    }
    const absl::btree_set<uint64_t>& user_tags = tags_by_seqnum_.at(seqnum);
    for (uint64_t user_tag : user_tags) {
        DCHECK(seqnums_by_tag_.contains(user_tag));
        seqnums_by_tag_.erase(user_tag);
    }
    tags_by_seqnum_.erase(seqnum);
}

bool AuxIndex::Contains(uint64_t seqnum) {
    return tags_by_seqnum_.contains(seqnum);
}

bool AuxIndex::FindPrev(uint64_t query_seqnum, uint64_t user_tag,
                          uint64_t* seqnum) const {
    if (!seqnums_by_tag_.contains(user_tag)) {
        return false;
    }
    if (!FindPrev(seqnums_by_tag_.at(user_tag), query_seqnum,
                  /*out*/ seqnum)) {
        return false;
    }
    return true;
}

bool AuxIndex::FindNext(uint64_t query_seqnum, uint64_t user_tag,
                          uint64_t* seqnum) const {
    if (!seqnums_by_tag_.contains(user_tag)) {
        return false;
    }
    if (!FindNext(seqnums_by_tag_.at(user_tag), query_seqnum,
                  /*out*/ seqnum)) {
        return false;
    }
    return true;
}

bool AuxIndex::FindPrev(const absl::btree_set<uint64_t>& seqnums,
                          uint64_t query_seqnum,
                          uint64_t* result_seqnum) const {
    if (tags_by_seqnum_.empty()) {
        return false;
    }
    if (query_seqnum == kMaxLogSeqNum) {
        *result_seqnum = *seqnums.rbegin();
        return true;
    }
    auto iter = seqnums.upper_bound(query_seqnum);
    if (iter == seqnums.begin()) {
        return false;
    } else {
        *result_seqnum = *(--iter);
        return true;
    }
}

bool AuxIndex::FindNext(const absl::btree_set<uint64_t>& seqnums,
                          uint64_t query_seqnum,
                          uint64_t* result_seqnum) const {
    if (tags_by_seqnum_.empty()) {
        return false;
    }
    auto iter = seqnums.lower_bound(query_seqnum);
    if (iter == seqnums.end()) {
        return false;
    } else {
        *result_seqnum = *iter;
        return true;
    }
}

std::string AuxIndex::Inspect() const {
    std::string output("AuxIndex::Inspact:\n");
    for (const auto& [tag, seqnums] : seqnums_by_tag_) {
        std::stringstream ss;
        for (const uint64_t seqnum : seqnums) {
            ss << fmt::format("0x{:016X}, ", seqnum);
        }
        ss << "\b\b  \b\b";  // remove tailing 2 characters
        if (seqnums.empty()) {
            output.append(fmt::format("tag={}, seqnums=[none]\n", tag));
        } else {
            output.append(fmt::format("tag={}, seqnums=[{}]\n", tag, ss.str()));
        }
    }
    for (const auto& [seqnum, tags] : tags_by_seqnum_) {
        std::stringstream ss;
        for (const uint64_t tag : tags) {
            ss << fmt::format("{}, ", tag);
        }
        ss << "\b\b  \b\b";  // remove tailing 2 characters
        if (tags.empty()) {
            output.append(fmt::format("seqnum={:016X}, tags=[none]\n", seqnum));
        } else {
            output.append(fmt::format("seqnum={:016X}, tags=[{}]\n", seqnum, ss.str()));
        }
    }
    return output;
}

ShardedLRUCache::ShardedLRUCache(int mem_cap_mb)
    : log_header_("ShardedLogCache[]: ") {
    aux_index_.reset(new AuxIndex());
    int64_t cap_mem_size = -1;
    if (mem_cap_mb > 0) {
        cap_mem_size = int64_t{mem_cap_mb} << 20;
    }
    dbm_.reset(new tkrzw::CacheDBM(/* cap_rec_num= */ -1, cap_mem_size));
    ulogger_.reset(new tkrzw::CacheDBMUpdateLogger());
    dbm_->SetUpdateLogger(ulogger_.get());
}

ShardedLRUCache::~ShardedLRUCache() {}

void ShardedLRUCache::PutLogData(const LogMetaData& log_metadata,
                                 std::span<const uint64_t> user_tags,
                                 std::span<const char> log_data) {
    std::string key_str = fmt::format("0_{:016x}", log_metadata.seqnum);
    std::string data = log_utils::EncodeEntry<LogMetaData>(log_metadata, user_tags, log_data);
    {
        absl::MutexLock cache_lk_(&cache_mu_);
        dbm_->Set(key_str, data, /* overwrite= */ false);
        UpdateCacheIndex();
    }
}

std::optional<LogEntry> ShardedLRUCache::GetLogData(uint64_t seqnum) {
    std::string key_str = fmt::format("0_{:016x}", seqnum);
    std::string data;
    // not touching index, so no lock here
    auto status = dbm_->Get(key_str, &data);
    if (status.IsOK()) {
        LogEntry log_entry;
        log_utils::DecodeEntry<LogMetaData, LogEntry>(std::move(data), &log_entry);
        DCHECK_EQ(seqnum, log_entry.metadata.seqnum);
        return log_entry;
    } else {
        return std::nullopt;
    }
}

void ShardedLRUCache::PutAuxData(const AuxEntry& aux_entry) {
    PutAuxData(aux_entry.metadata, VECTOR_AS_SPAN(aux_entry.user_tags),
               STRING_AS_SPAN(aux_entry.data));
}

void ShardedLRUCache::PutAuxData(const AuxMetaData& aux_metadata,
                                 std::span<const uint64_t> user_tags,
                                 std::span<const char> aux_data) {
    std::string key_str = fmt::format("1_{:016x}", aux_metadata.seqnum);
    {
        absl::MutexLock cache_lk_(&cache_mu_);
        aux_index_->Add(user_tags, aux_metadata.seqnum);
        HVLOG_F(1, "ShardedLRUCache::PutAuxData tags={}, seqnum=0x{:016X}",
                    user_tags, aux_metadata.seqnum);
        std::string data = log_utils::EncodeEntry<AuxMetaData>(aux_metadata, user_tags, aux_data);
        dbm_->Set(key_str, data, /* overwrite= */ true);
        UpdateCacheIndex();
    }
}

std::optional<AuxEntry> ShardedLRUCache::GetAuxData(uint64_t seqnum) {
    std::string key_str = fmt::format("1_{:016x}", seqnum);
    std::string data;
    // not touching index, so no lock here
    auto status = dbm_->Get(key_str, &data);
    HVLOG_F(1, "ShardedLRUCache::GetAuxData seqnum=0x{:016X}, found={}", seqnum, status.IsOK());
    if (status.IsOK()) {
        AuxEntry aux_entry;
        log_utils::DecodeEntry<AuxMetaData, AuxEntry>(std::move(data), &aux_entry);
        DCHECK_EQ(seqnum, aux_entry.metadata.seqnum);
        return aux_entry;
    } else {
        return std::nullopt;
    }
}

std::optional<AuxEntry> ShardedLRUCache::GetAuxDataPrev(uint64_t tag, uint64_t seqnum) {
    absl::ReaderMutexLock cache_rlk_(&cache_mu_);
    uint64_t result_seqnum;
    if (aux_index_->FindPrev(seqnum, tag, &result_seqnum)) {
        std::optional<AuxEntry> aux_entry = GetAuxData(result_seqnum);
        DCHECK(aux_entry.has_value());
        return aux_entry;
    } else {
        return std::nullopt;
    }
    // HVLOG_F(1, "ShardedLRUCache::GetAuxDataPrev tag={}, seqnum=0x{:016X}, found={}",
    //             tag, seqnum, status.IsOK());
}

std::optional<AuxEntry> ShardedLRUCache::GetAuxDataNext(uint64_t tag, uint64_t seqnum) {
    absl::ReaderMutexLock cache_rlk_(&cache_mu_);
    uint64_t result_seqnum;
    if (aux_index_->FindNext(seqnum, tag, &result_seqnum)) {
        std::optional<AuxEntry> aux_entry = GetAuxData(result_seqnum);
        DCHECK(aux_entry.has_value());
        return aux_entry;
    } else {
        return std::nullopt;
    }
    // HVLOG_F(1, "ShardedLRUCache::GetAuxDataNext tag={}, seqnum=0x{:016X}, found={}",
    //             tag, seqnum, status.IsOK());
}

// NOTE: MUST be protected by cache_mu_ because modifies aux_index_
void ShardedLRUCache::UpdateCacheIndex() {
    const tkrzw::CacheDBMUpdateLogger::UpdatesVec& updates =
        ulogger_->PopUpdates();
    if (updates.size() == 0) {
        return;
    }
    // provide updates to aux index
    for (const auto& [op_type, seqnum_key] : updates) {
        DCHECK(seqnum_key != "");
        // only apply aux updates, heading with "1_"
        if (seqnum_key[0] == '0') {
            continue;
        }
        // +2 to skip "1_"
        uint64_t seqnum = std::strtoul(seqnum_key.substr(2, 16).c_str(), nullptr, 16);
        if (op_type == tkrzw::CacheDBMUpdateLogger::OpType::OP_SET) {
            // should have been added when putting new aux entries
            DCHECK(aux_index_->Contains(seqnum))
                << fmt::format("aux index not contains seqnum={:016X} to set", seqnum)
                << aux_index_->Inspect();
        } else if (op_type == tkrzw::CacheDBMUpdateLogger::OpType::OP_REMOVE) {
            // removes must had been added
            DCHECK(aux_index_->Contains(seqnum))
                << fmt::format("aux index not contains seqnum={:016X} to remove", seqnum)
                << aux_index_->Inspect();
            aux_index_->Remove(seqnum);
        } else if (op_type == tkrzw::CacheDBMUpdateLogger::OpType::OP_CLEAR) {
            NOT_IMPLEMENTED();
        } else {
            UNREACHABLE();
        }
    }
}

}  // namespace log
}  // namespace faas
