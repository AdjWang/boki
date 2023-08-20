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
    virtual ~ThreadedMap();

    // All these APIs are thread safe
    void Put(uint64_t key, T* value);         // Override if the given key exists
    bool Poll(uint64_t key, T** value);       // Remove the given key if it is found
    bool Peek(uint64_t key, T** value);       // Remove the given key if it is found
    void PutChecked(uint64_t key, T* value);  // Panic if key exists
    T*   PollChecked(uint64_t key);           // Panic if key does not exist
    T*   PeekChecked(uint64_t key);           // Panic if key does not exist
    void RemoveChecked(uint64_t key);         // Panic if key does not exist
    void PollAll(std::vector<std::pair<uint64_t, T*>>* values);
    void PollAllSorted(std::vector<std::pair<uint64_t, T*>>* values);

private:
    absl::Mutex mu_;
    absl::flat_hash_map<uint64_t, T*> rep_ ABSL_GUARDED_BY(mu_);

    DISALLOW_COPY_AND_ASSIGN(ThreadedMap);
};

template<class T>
class DebugThreadedMap : public ThreadedMap<T> {
public:
    DebugThreadedMap();
    virtual ~DebugThreadedMap();

    // All these APIs are thread safe
    void Put(uint64_t key, T* value);         // Override if the given key exists
    bool Poll(uint64_t key, T** value);       // Remove the given key if it is found
    bool Peek(uint64_t key, T** value);       // Get the given key without removing if it is found
    void PutChecked(uint64_t key, T* value);  // Panic if key exists
    T*   PollChecked(uint64_t key);           // Panic if key does not exist
    T*   PeekChecked(uint64_t key);           // Panic if key does not exist
    void RemoveChecked(uint64_t key);         // Panic if key does not exist
    void PollAll(std::vector<std::pair<uint64_t, T*>>* values);
    void PollAllSorted(std::vector<std::pair<uint64_t, T*>>* values);

private:
    absl::Mutex mu_;
    absl::flat_hash_map<std::string, std::string> debug_stack_trace_ ABSL_GUARDED_BY(mu_);

    DISALLOW_COPY_AND_ASSIGN(DebugThreadedMap);
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

// encode/decode AuxMetaData+AuxEntry
inline std::string EncodeAuxEntry(const log::AuxEntry& aux_entry) {
    uint64_t seqnum = aux_entry.metadata.seqnum;
    std::string aux_data = aux_entry.data;

    size_t total_size = aux_data.size() + sizeof(log::AuxMetaData);
    std::string encoded;
    encoded.resize(total_size);
    char* ptr = encoded.data();

    DCHECK_GT(aux_data.size(), 0U);
    memcpy(ptr, aux_data.data(), aux_data.size());
    ptr += aux_data.size();

    log::AuxMetaData aux_metadata = {
        .seqnum = seqnum,
        .data_size = aux_data.size(),
    };
    memcpy(ptr, &aux_metadata, sizeof(log::AuxMetaData));
    return encoded;
}
inline void DecodeAuxEntry(std::string encoded, log::AuxEntry* aux_entry) {
    DCHECK_GT(encoded.size(), sizeof(log::AuxMetaData));
    log::AuxMetaData& metadata = aux_entry->metadata;
    memcpy(&metadata,
           encoded.data() + encoded.size() - sizeof(log::AuxMetaData),
           sizeof(log::AuxMetaData));
    size_t total_size = metadata.data_size
                      + sizeof(log::AuxMetaData);
    DCHECK_EQ(total_size, encoded.size());
    encoded.resize(metadata.data_size);
    aux_entry->data = std::move(encoded);
}

// encode/decode LogMetaData+LogEntry 
inline std::string EncodeEntry(const log::LogMetaData& log_metadata,
                               std::span<const uint64_t> user_tags,
                               std::span<const char> log_data) {
    DCHECK_EQ(log_metadata.num_tags, user_tags.size());
    DCHECK_EQ(log_metadata.data_size, log_data.size());
    size_t total_size = log_data.size()
                      + user_tags.size() * sizeof(uint64_t)
                      + sizeof(log::LogMetaData);
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
    memcpy(ptr, &log_metadata, sizeof(log::LogMetaData));
    return encoded;
}
inline void DecodeEntry(std::string encoded, log::LogEntry* log_entry) {
    DCHECK_GT(encoded.size(), sizeof(log::LogMetaData));
    log::LogMetaData& metadata = log_entry->metadata;
    memcpy(&metadata,
           encoded.data() + encoded.size() - sizeof(log::LogMetaData),
           sizeof(log::LogMetaData));
    size_t total_size = metadata.data_size
                      + metadata.num_tags * sizeof(uint64_t)
                      + sizeof(log::LogMetaData);
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
bool ThreadedMap<T>::Peek(uint64_t key, T** value) {
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
    DCHECK(!rep_.contains(key)) << fmt::format("key={}", key);
    rep_[key] = value;
}

template<class T>
T* ThreadedMap<T>::PollChecked(uint64_t key) {
    absl::MutexLock lk(&mu_);
    DCHECK(rep_.contains(key)) << fmt::format("key={}", key);
    T* value = rep_.at(key);
    rep_.erase(key);
    return value;
}

template<class T>
T* ThreadedMap<T>::PeekChecked(uint64_t key) {
    absl::MutexLock lk(&mu_);
    DCHECK(rep_.contains(key)) << fmt::format("key={}", key);
    T* value = rep_.at(key);
    return value;
}

template<class T>
void ThreadedMap<T>::RemoveChecked(uint64_t key) {
    absl::MutexLock lk(&mu_);
    DCHECK(rep_.contains(key)) << fmt::format("key={}", key);
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
DebugThreadedMap<T>::DebugThreadedMap() {}

template<class T>
DebugThreadedMap<T>::~DebugThreadedMap() {}

template<class T>
void DebugThreadedMap<T>::Put(uint64_t key, T* value) {
    {
        absl::MutexLock lk(&mu_);
        std::string debug_key(fmt::format("Put-{}", key));
        if (debug_stack_trace_.contains(debug_key)) {
            LOG_F(FATAL, "key: {}, stacktrace:\ncurrent: {}\nprevious:{}",
                          debug_key, utils::DumpStackTrace(), debug_stack_trace_.at(debug_key));
        } else {
            debug_stack_trace_.emplace(debug_key, utils::DumpStackTrace());
        }
    }
    ThreadedMap<T>::Put(key, value);
}

template<class T>
bool DebugThreadedMap<T>::Poll(uint64_t key, T** value) {
    {
        absl::MutexLock lk(&mu_);
        std::string debug_key(fmt::format("Remove-{}", key));
        if (debug_stack_trace_.contains(debug_key)) {
            LOG_F(FATAL, "key: {}, stacktrace:\ncurrent: {}\nprevious:{}",
                          debug_key, utils::DumpStackTrace(), debug_stack_trace_.at(debug_key));
        } else {
            debug_stack_trace_.emplace(debug_key, utils::DumpStackTrace());
        }
    }
    return ThreadedMap<T>::Poll(key, value);
}

template<class T>
bool DebugThreadedMap<T>::Peek(uint64_t key, T** value) {
    {
        absl::MutexLock lk(&mu_);
        std::string debug_key(fmt::format("Remove-{}", key));
        if (debug_stack_trace_.contains(debug_key)) {
            LOG_F(FATAL, "key: {}, stacktrace:\ncurrent: {}\nprevious:{}",
                          debug_key, utils::DumpStackTrace(), debug_stack_trace_.at(debug_key));
        }
    }
    return ThreadedMap<T>::Peek(key, value);
}

template<class T>
void DebugThreadedMap<T>::PutChecked(uint64_t key, T* value) {
    {
        absl::MutexLock lk(&mu_);
        std::string debug_key(fmt::format("Put-{}", key));
        if (debug_stack_trace_.contains(debug_key)) {
            LOG_F(FATAL, "key: {}, stacktrace:\ncurrent: {}\nprevious:{}",
                          debug_key, utils::DumpStackTrace(), debug_stack_trace_.at(debug_key));
        } else {
            debug_stack_trace_.emplace(debug_key, utils::DumpStackTrace());
        }
    }
    ThreadedMap<T>::PutChecked(key, value);
}

template<class T>
T* DebugThreadedMap<T>::PollChecked(uint64_t key) {
    {
        absl::MutexLock lk(&mu_);
        std::string debug_key(fmt::format("Remove-{}", key));
        if (debug_stack_trace_.contains(debug_key)) {
            LOG_F(FATAL, "key: {}, stacktrace:\ncurrent: {}\nprevious:{}",
                          debug_key, utils::DumpStackTrace(), debug_stack_trace_.at(debug_key));
        } else {
            debug_stack_trace_.emplace(debug_key, utils::DumpStackTrace());
        }
    }
    return ThreadedMap<T>::PollChecked(key);
}

template<class T>
T* DebugThreadedMap<T>::PeekChecked(uint64_t key) {
    {
        absl::MutexLock lk(&mu_);
        std::string debug_key(fmt::format("Remove-{}", key));
        if (debug_stack_trace_.contains(debug_key)) {
            LOG_F(FATAL, "key: {}, stacktrace:\ncurrent: {}\nprevious:{}",
                          debug_key, utils::DumpStackTrace(), debug_stack_trace_.at(debug_key));
        }
    }
    return ThreadedMap<T>::PeekChecked(key);
}

template<class T>
void DebugThreadedMap<T>::RemoveChecked(uint64_t key) {
    {
        absl::MutexLock lk(&mu_);
        std::string debug_key(fmt::format("Remove-{}", key));
        if (debug_stack_trace_.contains(debug_key)) {
            LOG_F(FATAL, "key: {}, stacktrace:\ncurrent: {}\nprevious:{}",
                          debug_key, utils::DumpStackTrace(), debug_stack_trace_.at(debug_key));
        } else {
            debug_stack_trace_.emplace(debug_key, utils::DumpStackTrace());
        }
    }
    ThreadedMap<T>::RemoveChecked(key);
}

template<class T>
void DebugThreadedMap<T>::PollAll(std::vector<std::pair<uint64_t, T*>>* values) {
    ThreadedMap<T>::PollAll(values);
}

template<class T>
void DebugThreadedMap<T>::PollAllSorted(std::vector<std::pair<uint64_t, T*>>* values) {
    ThreadedMap<T>::PollAllSorted(values);
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

class ThreadSafeCounter {
public:
    ThreadSafeCounter() 
        : counter_(0), target_(std::numeric_limits<uint64_t>::max()) {}

    void Reset() {
        {
            absl::MutexLock lk(&mu_);
            counter_ = 0;
            target_ = std::numeric_limits<uint64_t>::max();
        }
        {
            absl::MutexLock lk(&buffer_mu_);
            seqnum_id_map_.clear();
        }
    }

    bool AddCountAndCheck(uint64_t n) {
        absl::MutexLock lk(&mu_);
        counter_ += n;
        // VLOG_F(1, "AddCountAndCheck n={}, check={}, bt={}", 
        //              n, counter_==target_, utils::DumpStackTrace());
        return counter_ == target_;
    }
    bool SetTargetAndCheck(uint64_t target) {
        absl::MutexLock lk(&mu_);
        DCHECK(target >= 0);
        DCHECK(target_ == std::numeric_limits<uint64_t>::max())
            << fmt::format("[{}] target_={}", (void*)this, target_);    // should set only once
        target_ = target;
        // VLOG_F(1, "SetTargetAndCheck[{}] target={}, check={}, bt={}", 
        //           (void*)this, target, counter_==target_, utils::DumpStackTrace());
        return counter_ == target_;
    }
    void BufferRequestId(uint64_t seqnum, uint64_t id) {
        absl::MutexLock lk(&buffer_mu_);
        DCHECK(!seqnum_id_map_.contains(seqnum));
        seqnum_id_map_.emplace(seqnum, id);
    }
    uint64_t GetBufferedRequestId(uint64_t seqnum) {
        absl::ReaderMutexLock lk(&buffer_mu_);
        DCHECK(seqnum_id_map_.contains(seqnum));
        return seqnum_id_map_.at(seqnum);
    }

    // DEBUG
    bool IsResolved() {
        absl::ReaderMutexLock lk(&mu_);
        return counter_ == target_;
    }
private:
    absl::Mutex mu_;
    uint64_t counter_ ABSL_GUARDED_BY(mu_);
    uint64_t target_ ABSL_GUARDED_BY(mu_);

    absl::Mutex buffer_mu_;
    absl::flat_hash_map<uint64_t, uint64_t> seqnum_id_map_ ABSL_GUARDED_BY(buffer_mu_);

    DISALLOW_COPY_AND_ASSIGN(ThreadSafeCounter);
};

class Profiler {
public:
    void TimeStamp(std::string_view tip) {
        int64_t timestamp = GetMonotonicNanoTimestamp();
        prof_log_.append(fmt::format("ts={} tip={};", timestamp, tip));
    }
    std::string Output() {
        return prof_log_;
    }
private:
    std::string prof_log_;
};

}  // namespace log_utils
}  // namespace faas
