#pragma once

#include "log/common.h"
#include "log/view.h"
#include "log/view_watcher.h"
#include "utils/lockable_ptr.h"

namespace faas {
namespace log_utils {

uint16_t GetViewId(uint64_t value);

// Used for on-holding requests for future views
class FutureRequests {
public:
    FutureRequests();
    ~FutureRequests();

    // Both `OnNewView` and `OnHoldRequest` are thread safe

    // If `ready_requests` is nullptr, will panic if there are on-hold requests
    void OnNewView(const log::View* view,
                   std::vector<log::SharedLogRequest>* ready_requests);
    void OnHoldRequest(uint16_t view_id, log::SharedLogRequest request);

private:
    absl::Mutex mu_;

    uint16_t next_view_id_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* view_id */ uint16_t, std::vector<log::SharedLogRequest>>
        onhold_requests_ ABSL_GUARDED_BY(mu_);

    DISALLOW_COPY_AND_ASSIGN(FutureRequests);
};

template<class T>
class ThreadedMap {
public:
    ThreadedMap();
    ~ThreadedMap();

    // All these APIs are thread safe
    void Put(uint64_t key, T* value);         // Override if the given key exists
    bool Poll(uint64_t key, T** value);       // Remove the given key if it is found
    bool Peak(uint64_t key, T** value);       // Remove the given key if it is found
    void PutChecked(uint64_t key, T* value);  // Panic if key exists
    T*   PollChecked(uint64_t key);           // Panic if key does not exist
    T*   PeakChecked(uint64_t key);           // Panic if key does not exist
    void RemoveChecked(uint64_t key);         // Panic if key does not exist
    void PollAll(std::vector<std::pair<uint64_t, T*>>* values);
    void PollAllSorted(std::vector<std::pair<uint64_t, T*>>* values);

private:
    absl::Mutex mu_;
    absl::flat_hash_map<uint64_t, T*> rep_ ABSL_GUARDED_BY(mu_);

    DISALLOW_COPY_AND_ASSIGN(ThreadedMap);
};

log::MetaLogsProto MetaLogsFromPayload(std::span<const char> payload);

log::LogMetaData GetMetaDataFromMessage(const protocol::SharedLogMessage& message);
void SplitPayloadForMessage(const protocol::SharedLogMessage& message,
                            std::span<const char> payload,
                            std::span<const uint64_t>* user_tags,
                            std::span<const char>* log_data,
                            std::span<const char>* aux_data);

void PopulateMetaDataToMessage(const log::LogMetaData& metadata,
                               protocol::SharedLogMessage* message);
void PopulateMetaDataToMessage(const log::LogEntryProto& log_entry,
                               protocol::SharedLogMessage* message);

// encode/decode LogMetaData+LogEntry or AuxMetaData+AuxEntry
template <typename TEntryMeta>
inline std::string EncodeEntry(const TEntryMeta& log_metadata,
                               std::span<const uint64_t> user_tags,
                               std::span<const char> log_data) {
    DCHECK_EQ(log_metadata.num_tags, user_tags.size());
    DCHECK_EQ(log_metadata.data_size, log_data.size());
    size_t total_size = log_data.size()
                      + user_tags.size() * sizeof(uint64_t)
                      + sizeof(TEntryMeta);
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
    memcpy(ptr, &log_metadata, sizeof(TEntryMeta));
    return encoded;
}
template <typename TEntryMeta, typename TEntry>
inline void DecodeEntry(std::string encoded, TEntry* log_entry) {
    DCHECK_GT(encoded.size(), sizeof(TEntryMeta));
    TEntryMeta& metadata = log_entry->metadata;
    memcpy(&metadata,
           encoded.data() + encoded.size() - sizeof(TEntryMeta),
           sizeof(TEntryMeta));
    size_t total_size = metadata.data_size
                      + metadata.num_tags * sizeof(uint64_t)
                      + sizeof(TEntryMeta);
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

// Start implementation of ThreadedMap

template<class T>
ThreadedMap<T>::ThreadedMap() {}

template<class T>
ThreadedMap<T>::~ThreadedMap() {
#if DCHECK_IS_ON()
    if (!rep_.empty()) {
        LOG_F(WARNING, "There are {} elements left", rep_.size());
    }
#endif
}

template<class T>
void ThreadedMap<T>::Put(uint64_t key, T* value) {
    absl::MutexLock lk(&mu_);
    rep_[key] = value;
}

template<class T>
bool ThreadedMap<T>::Poll(uint64_t key, T** value) {
    absl::MutexLock lk(&mu_);
    if (rep_.contains(key)) {
        *value = rep_.at(key);
        rep_.erase(key);
        return true;
    } else {
        return false;
    }
}

template<class T>
bool ThreadedMap<T>::Peak(uint64_t key, T** value) {
    absl::MutexLock lk(&mu_);
    if (rep_.contains(key)) {
        *value = rep_.at(key);
        return true;
    } else {
        return false;
    }
}

template<class T>
void ThreadedMap<T>::PutChecked(uint64_t key, T* value) {
    absl::MutexLock lk(&mu_);
    DCHECK(!rep_.contains(key));
    rep_[key] = value;
}

template<class T>
T* ThreadedMap<T>::PollChecked(uint64_t key) {
    absl::MutexLock lk(&mu_);
    DCHECK(rep_.contains(key));
    T* value = rep_.at(key);
    rep_.erase(key);
    return value;
}

template<class T>
T* ThreadedMap<T>::PeakChecked(uint64_t key) {
    absl::MutexLock lk(&mu_);
    DCHECK(rep_.contains(key));
    T* value = rep_.at(key);
    return value;
}

template<class T>
void ThreadedMap<T>::RemoveChecked(uint64_t key) {
    absl::MutexLock lk(&mu_);
    DCHECK(rep_.contains(key));
    rep_.erase(key);
}

template<class T>
void ThreadedMap<T>::PollAll(std::vector<std::pair<uint64_t, T*>>* values) {
    absl::MutexLock lk(&mu_);
    values->resize(rep_.size());
    if (values->empty()) {
        return;
    }
    size_t i = 0;
    for (const auto& [key, value] : rep_) {
        (*values)[i++] = std::make_pair(key, value);
    }
    DCHECK_EQ(i, rep_.size());
    rep_.clear();
}

template<class T>
void ThreadedMap<T>::PollAllSorted(std::vector<std::pair<uint64_t, T*>>* values) {
    PollAll(values);
    if (values->empty()) {
        return;
    }
    std::sort(
        values->begin(), values->end(),
        [] (const std::pair<uint64_t, T*>& lhs, const std::pair<uint64_t, T*>& rhs) -> bool {
            return lhs.first < rhs.first;
        }
    );
}

template<class T>
void FinalizedLogSpace(LockablePtr<T> logspace_ptr,
                       const log::FinalizedView* finalized_view) {
    auto locked_logspace = logspace_ptr.Lock();
    uint32_t logspace_id = locked_logspace->identifier();
    bool success = locked_logspace->Finalize(
        finalized_view->final_metalog_position(logspace_id),
        finalized_view->tail_metalogs(logspace_id));
    if (!success) {
        LOG_F(FATAL, "Failed to finalize log space {}", bits::HexStr0x(logspace_id));
    }
}

}  // namespace log_utils
}  // namespace faas
