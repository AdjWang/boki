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

// cache key utilities
namespace {
    inline std::string MakeLogCacheKey(uint64_t seqnum) {
        return fmt::format("0_{:016x}", seqnum);
    }
    inline std::string MakeAuxCacheKey(uint64_t seqnum) {
        return fmt::format("1_{:016x}", seqnum);
    }
    inline std::string MakeAuxCacheKeyWithIndex(uint64_t seqnum, uint64_t tag) {
        return fmt::format("2_{:016x}_{:016x}", seqnum, tag);
    }

    inline void GetMetaFromAuxCacheKeyWithIndex(const std::string& key_str,
                                                uint64_t* seqnum, uint64_t* tag) {
        DCHECK(seqnum != nullptr);
        DCHECK(tag != nullptr);
        *seqnum = std::strtoul(key_str.substr(2, 16).c_str(), nullptr, 16);
        *tag = std::strtoul(key_str.substr(19, 16).c_str(), nullptr, 16);
    }

    inline bool IsAuxCacheKeyWithIndex(std::string_view key_str) {
        return key_str[0] == '2';
    }
}

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
    std::string key_str = MakeLogCacheKey(log_metadata.seqnum);
    std::string data = log_utils::EncodeEntry(log_metadata, user_tags, log_data);
    dbm_->Set(key_str, data, /* overwrite= */ false);
}

std::optional<LogEntry> LRUCache::Get(uint64_t seqnum) {
    std::string key_str = MakeLogCacheKey(seqnum);
    std::string data;
    auto status = dbm_->Get(key_str, &data);
    if (status.IsOK()) {
        LogEntry log_entry;
        log_utils::DecodeEntry(std::move(data), &log_entry);
        DCHECK_EQ(seqnum, log_entry.metadata.seqnum);
        return log_entry;
    } else {
        return std::nullopt;
    }
}

void LRUCache::PutAuxData(uint64_t seqnum, std::span<const char> data) {
    std::string key_str = MakeAuxCacheKey(seqnum);
    dbm_->Set(key_str, std::string_view(data.data(), data.size()),
              /* overwrite= */ true);
}

std::optional<std::string> LRUCache::GetAuxData(uint64_t seqnum) {
    std::string key_str = MakeAuxCacheKey(seqnum);
    std::string data;
    auto status = dbm_->Get(key_str, &data);
    if (status.IsOK()) {
        return data;
    } else {
        return std::nullopt;
    }
}

void AuxIndex::Add(uint64_t seqnum, uint64_t tag) {
    seqnums_by_tag_[tag].emplace(seqnum);
    tags_by_seqnum_[seqnum].emplace(tag);
}

void AuxIndex::Remove(uint64_t seqnum, uint64_t tag) {
    seqnums_by_tag_[tag].erase(seqnum);
    tags_by_seqnum_[seqnum].erase(tag);
}

bool AuxIndex::Contains(uint64_t seqnum) const {
    return tags_by_seqnum_.contains(seqnum);
}

bool AuxIndex::Contains(uint64_t seqnum, uint64_t tag) const {
    if (tags_by_seqnum_.contains(seqnum)) {
        return tags_by_seqnum_.at(seqnum).contains(tag);
    } else {
        return false;
    }
}

UserTagVec AuxIndex::GetTags(uint64_t seqnum) const {
    if (!tags_by_seqnum_.contains(seqnum)) {
        return UserTagVec();
    }
    auto tag_set = tags_by_seqnum_.at(seqnum);
    return UserTagVec(tag_set.begin(), tag_set.end());
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
    if (seqnums.empty()) {
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
    if (seqnums.empty()) {
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
    std::string key_str = MakeLogCacheKey(log_metadata.seqnum);
    std::string data = log_utils::EncodeEntry(log_metadata, user_tags, log_data);
    {
        absl::MutexLock cache_lk_(&cache_mu_);
        dbm_->Set(key_str, data, /* overwrite= */ false);
        UpdateCacheIndex();
    }
}

std::optional<LogEntry> ShardedLRUCache::GetLogData(uint64_t seqnum) ABSL_NO_THREAD_SAFETY_ANALYSIS {
    std::string key_str = MakeLogCacheKey(seqnum);
    std::string data;
    // not touching index, so no lock here
    auto status = dbm_->Get(key_str, &data);
    if (status.IsOK()) {
        LogEntry log_entry;
        log_utils::DecodeEntry(std::move(data), &log_entry);
        DCHECK_EQ(seqnum, log_entry.metadata.seqnum);
        return log_entry;
    } else {
        return std::nullopt;
    }
}

void ShardedLRUCache::PutAuxData(const AuxEntry& aux_entry) {
    uint64_t seqnum = aux_entry.metadata.seqnum;
    json aux_entry_data = json::parse(aux_entry.data);
    {
        absl::MutexLock cache_lk_(&cache_mu_);
        for (auto& [tag_str, aux_data] : aux_entry_data.items()) {
            DCHECK(!aux_data.empty());
            uint64_t tag = std::strtoul(tag_str.c_str(), nullptr, 16);
            // update index for addings
            aux_index_->Add(seqnum, tag);
            HVLOG_F(1, "ShardedLRUCache::PutAuxData from entry seqnum={:016X} tag={}",
                        seqnum, tag);
            // update data
            std::string idx_key_str = MakeAuxCacheKeyWithIndex(seqnum, tag);
            dbm_->Set(idx_key_str, aux_data.get<std::string>(), /* overwrite= */ true);
        }
        // update index for potential removes, add twice is ok
        UpdateCacheIndex();
    }
}

void ShardedLRUCache::PutAuxData(uint64_t seqnum, uint64_t tag,
                                 std::span<const char> aux_data) {
    absl::MutexLock cache_lk_(&cache_mu_);
    // update index for addings
    aux_index_->Add(seqnum, tag);
    HVLOG_F(1, "ShardedLRUCache::PutAuxData seqnum={:016X} tag={}", seqnum, tag);
    std::string idx_key_str = MakeAuxCacheKeyWithIndex(seqnum, tag);
    dbm_->Set(idx_key_str, SPAN_AS_STRING(aux_data), /* overwrite= */ true);
    // update index for potential removes, add twice is ok
    UpdateCacheIndex();
}

std::optional<AuxEntry> ShardedLRUCache::GetAuxData(uint64_t seqnum) {
    absl::ReaderMutexLock cache_rlk_(&cache_mu_);
    UserTagVec tags = aux_index_->GetTags(seqnum);
    json aux_entry;
    for (uint64_t tag : tags) {
        std::string key_str = MakeAuxCacheKeyWithIndex(seqnum, tag);
        std::string data;
        auto status = dbm_->Get(key_str, &data);
        if (status.IsOK()) {
            aux_entry[fmt::format("{}", tag)] = data;
        }
    }
    if (aux_entry.empty()) {
        return std::nullopt;
    } else {
        std::string aux_data_str = aux_entry.dump();
        AuxMetaData aux_metadata = {
            .seqnum = seqnum,
            .data_size = aux_data_str.size(),
        };
        return AuxEntry {
            .metadata = aux_metadata,
            .data = aux_data_str,
        };
    }
}

std::optional<AuxEntry> ShardedLRUCache::GetAuxDataChecked(uint64_t seqnum, uint64_t tag) const {
    if (!aux_index_->Contains(seqnum, tag)) {
        return std::nullopt;
    }
    bool contains_tag = false;
    UserTagVec tags = aux_index_->GetTags(seqnum);
    json aux_entry;
    for (uint64_t aux_tag : tags) {
        if (aux_tag == tag) {
            contains_tag = true;
        }
        std::string key_str = MakeAuxCacheKeyWithIndex(seqnum, tag);
        std::string data;
        auto status = dbm_->Get(key_str, &data);
        DCHECK(status.IsOK());
        aux_entry[fmt::format("{}", tag)] = data;
    }
    DCHECK(contains_tag);
    DCHECK(!aux_entry.empty());
    std::string aux_data_str = aux_entry.dump();
    HVLOG_F(1, "GetAuxData seqnum={:016X} aux_data={}", seqnum, aux_data_str);
    AuxMetaData aux_metadata = {
        .seqnum = seqnum,
        .data_size = aux_data_str.size(),
    };
    return AuxEntry {
        .metadata = aux_metadata,
        .data = aux_data_str,
    };
}

std::optional<AuxEntry> ShardedLRUCache::GetAuxDataPrev(uint64_t tag, uint64_t seqnum) {
    absl::ReaderMutexLock cache_rlk_(&cache_mu_);
    uint64_t result_seqnum;
    // DEBUG
    {
        bool found = aux_index_->FindPrev(seqnum, tag, &result_seqnum);
        HVLOG_F(1, "ShardedLRUCache::GetAuxDataPrev tag={}, seqnum={:016X}, found={}:{:016X}\n{}",
                    tag, seqnum, found, result_seqnum, aux_index_->Inspect());
    }
    if (aux_index_->FindPrev(seqnum, tag, &result_seqnum)) {
        std::optional<AuxEntry> aux_entry = GetAuxDataChecked(result_seqnum, tag);
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
    // DEBUG
    // bool found = aux_index_->FindNext(seqnum, tag, &result_seqnum);
    // HVLOG_F(1, "ShardedLRUCache::GetAuxDataNext tag={}, seqnum={:016X}, found={}",
    //             tag, seqnum, found) << aux_index_->Inspect();
    // if (found) {
    if (aux_index_->FindNext(seqnum, tag, &result_seqnum)) {
        std::optional<AuxEntry> aux_entry = GetAuxDataChecked(result_seqnum, tag);
        DCHECK(aux_entry.has_value());
        return aux_entry;
    } else {
        return std::nullopt;
    }
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
        if (!IsAuxCacheKeyWithIndex(seqnum_key)) {
            continue;
        }
        uint64_t seqnum, tag;
        GetMetaFromAuxCacheKeyWithIndex(seqnum_key, &seqnum, &tag);
        if (op_type == tkrzw::CacheDBMUpdateLogger::OpType::OP_SET) {
            // should have been added when putting new aux entries
            DCHECK(aux_index_->Contains(seqnum))
                << fmt::format("aux index not contains seqnum={:016X} to set", seqnum)
                << aux_index_->Inspect();
        } else if (op_type == tkrzw::CacheDBMUpdateLogger::OpType::OP_REMOVE) {
            HVLOG_F(1, "ShardedLRUCache::UpdateCacheIndex OP_REMOVE {}", seqnum_key);
            // removes must had been added
            DCHECK(aux_index_->Contains(seqnum))
                << fmt::format("aux index not contains seqnum={:016X} to remove", seqnum)
                << aux_index_->Inspect();
            aux_index_->Remove(seqnum, tag);
        } else if (op_type == tkrzw::CacheDBMUpdateLogger::OpType::OP_CLEAR) {
            NOT_IMPLEMENTED();
        } else {
            UNREACHABLE();
        }
    }
}

}  // namespace log
}  // namespace faas
