#pragma once

#include "log/common.h"
#include "utils/fs.h"
#include "utils/lockable_ptr.h"
#include "ipc/base.h"
#ifdef __FAAS_SRC
#include "base/thread.h"
#endif

__BEGIN_THIRD_PARTY_HEADERS
#include <tkrzw_dbm_cache.h>
#include <tkrzw_dbm_hash.h>
#include <tkrzw_file_mmap.h>
__END_THIRD_PARTY_HEADERS

namespace boost {
namespace interprocess {
typedef allocator<void, managed_mapped_file::segment_manager> void_allocator_t;
typedef allocator<char, managed_mapped_file::segment_manager> char_allocator_t;
typedef basic_string<char, std::char_traits<char>, char_allocator_t> shm_string_t;

typedef allocator<shm_string_t, managed_mapped_file::segment_manager> list_allocator_t;
typedef list<shm_string_t, list_allocator_t> list_t;

typedef std::pair<shm_string_t, typename list_t::iterator> mapped_t;
typedef std::pair<const shm_string_t, mapped_t> value_t;
typedef allocator<value_t, managed_mapped_file::segment_manager> map_allocator_t;
typedef map<shm_string_t, mapped_t, std::less<shm_string_t>, map_allocator_t> map_t;

// DEBUG: shared list iterator SIGSEGV
// Raw map is just a hash map, without lru control.
typedef std::pair<const shm_string_t, shm_string_t> raw_value_t;
typedef allocator<raw_value_t, managed_mapped_file::segment_manager> raw_map_allocator_t;
// typedef map<shm_string_t, shm_string_t, std::less<shm_string_t>, raw_map_allocator_t> raw_map_t;
typedef unordered_map<shm_string_t, shm_string_t, boost::hash<shm_string_t>,
                      std::equal_to<shm_string_t>, raw_map_allocator_t>
    raw_map_t;

// class LRUCache {
// public:
//     LRUCache(size_t mem_capacity, std::string_view path)
//         : mem_capacity_(mem_capacity),
//           path_(path),
// #ifdef __COMPILE_AS_SHARED
//           segment_(open_only, path_.c_str()),
//         //   ca_(segment_.get_allocator<char>()),
//           alloc_inst_(segment_.get_segment_manager()),
//           lru_list_(segment_.find<list_t>("lru_list").first),
//           lru_map_(segment_.find<map_t>("lru_map").first),
// #else
//           segment_(create_only, path_.c_str(), mem_capacity),
//         //   ca_(segment_.get_allocator<char>()),
//           alloc_inst_(segment_.get_segment_manager()),
//           lru_list_(segment_.construct<list_t>("lru_list")(alloc_inst_)),
//         //   lru_list_(new list_t()),
//           lru_map_(segment_.construct<map_t>("lru_map")(std::less<shm_string_t>(),
//                                                         alloc_inst_)),
// #endif
//           mem_size_(0u)
//         {}

//     ~LRUCache() {
// #ifndef __COMPILE_AS_SHARED
//         file_mapping::remove(path_.c_str());
// #endif
//     }

//     size_t size() const { return lru_map_->size(); }

//     size_t capacity() const { return mem_capacity_; }

//     bool empty() const { return lru_map_->empty(); }

//     bool contains(const std::string& key) {
//         shm_string_t shm_key(key.c_str(), alloc_inst_);
//         return lru_map_->find(shm_key) != lru_map_->end();
//     }

//     void insert(const std::string& key, const std::string& value) {
//         shm_string_t shm_key(key.c_str(), alloc_inst_);
//         shm_string_t shm_value(value.begin(), value.end(), alloc_inst_);
//         typename map_t::iterator i = lru_map_->find(shm_key);
//         if (i == lru_map_->end()) {
//             // insert the new item
//             mem_size_ += (shm_key.size() + shm_value.size());
//             lru_list_->push_front(shm_key);
//             lru_map_->emplace(std::piecewise_construct,
//                 std::forward_as_tuple(shm_key),
//                 std::forward_as_tuple(shm_value, lru_list_->begin()));
//         } else {
//             // update existing item
//             const auto& original_value = lru_map_->at(shm_key);
//             mem_size_ += shm_value.size();
//             mem_size_ -= original_value.first.size();
//             lru_map_->at(shm_key).first = std::move(shm_value);
//         }
//         try_evict();
//     }

//     boost::optional<std::string> get(const std::string& key) {
// #ifdef __FAAS_SRC
//         // DEBUG
//         LOG_F(INFO, "LRUCache getting thread={}", faas::base::Thread::current()->name());
// #endif

//         // lookup value in the cache
//         shm_string_t shm_key(key.c_str(), alloc_inst_);
//         typename map_t::iterator i = lru_map_->find(shm_key);
//         if (i == lru_map_->end()) {
//             // value not in cache
//             return boost::none;
//         }

//         // return the value, but first update its place in the most
//         // recently used list
//         typename list_t::iterator j = i->second.second;
//         if (j != lru_list_->begin()) {
//             // move item to the front of the most recently used list
//             DCHECK(j != lru_list_->end());

//             // bool found = false;
//             // for (auto it = lru_list_->begin(); it != lru_list_->end(); it++) {
//             //     if (it == j) {
//             //         found = true;
//             //         break;
//             //     }
//             // }
//             // DCHECK(found);

//             list_t::const_iterator temp(j);
//             DCHECK(++(--temp) == j);
//             DCHECK(--(++temp) == j);

//             lru_list_->erase(j);
//             lru_list_->push_front(shm_key);

//             // update iterator in map
//             j = lru_list_->begin();
//             const shm_string_t& value = i->second.first;
//             lru_map_->emplace(std::piecewise_construct,
//                 std::forward_as_tuple(shm_key),
//                 std::forward_as_tuple(value, j));

//             // return the value
//             return std::string(value.begin(), value.end());
//         } else {
//             // the item is already at the front of the most recently
//             // used list so just return it
//             return std::string(i->second.first.begin(), i->second.first.end());
//         }
//     }

//     void clear() {
//         lru_map_->clear();
//         lru_list_->clear();
//         mem_size_ = 0u;
//     }

// private:
//     const size_t mem_capacity_;
//     std::string path_;

//     managed_mapped_file segment_;
//     void_allocator_t alloc_inst_;
// #ifndef __COMPILE_AS_SHARED
// #endif

//     list_t* lru_list_;
//     map_t* lru_map_;

//     size_t mem_size_;

//     void try_evict() {
//         while (mem_size_ > mem_capacity_) {
//             // evict item from the end of most recently used list
//             typename list_t::iterator i = --lru_list_->end();
//             const auto& value = lru_map_->at(*i);
//             mem_size_ -= (i->size() + value.first.size());
//             lru_map_->erase(*i);
//             lru_list_->erase(i);
//         }
//     }
// };


// DEBUG: A hash map implements the lru cache interface.
class LRUCache {
public:
    LRUCache(size_t mem_capacity, std::string_view path)
        : mem_capacity_(mem_capacity),
          path_(path),
#ifdef __COMPILE_AS_SHARED
          segment_(open_only, path_.c_str()),
          alloc_inst_(segment_.get_segment_manager()),
          raw_map_(segment_.find<raw_map_t>("raw_map").first)
#else
          segment_(create_only, path_.c_str(), mem_capacity),
          alloc_inst_(segment_.get_segment_manager()),
        //   raw_map_(segment_.construct<raw_map_t>("raw_map")(std::less<shm_string_t>(),
        //                                                     alloc_inst_))
          raw_map_(segment_.construct<raw_map_t>("raw_map")(0u, boost::hash<shm_string_t>(),
                                                            std::equal_to<shm_string_t>(),
                                                            alloc_inst_))
#endif
        {}

    ~LRUCache() {
#ifndef __COMPILE_AS_SHARED
        file_mapping::remove(path_.c_str());
#endif
    }

    size_t size() const { return raw_map_->size(); }

    size_t capacity() const { return mem_capacity_; }

    bool empty() const { return raw_map_->empty(); }

    bool contains(const std::string& key) {
        shm_string_t shm_key(key.c_str(), alloc_inst_);
        return raw_map_->find(shm_key) != raw_map_->end();
    }

    void insert(const std::string& key, const std::string& value) {
        shm_string_t shm_key(key.c_str(), alloc_inst_);
        shm_string_t shm_value(value.begin(), value.end(), alloc_inst_);
        typename raw_map_t::iterator i = raw_map_->find(shm_key);
        if (i == raw_map_->end()) {
            // insert the new item
            raw_map_->emplace(std::piecewise_construct,
                std::forward_as_tuple(shm_key),
                std::forward_as_tuple(shm_value));
        } else {
            // update existing item
            raw_map_->at(shm_key) = std::move(shm_value);
        }
    }

    boost::optional<std::string> get(const std::string& key) {
        // lookup value in the cache
        shm_string_t shm_key(key.c_str(), alloc_inst_);
        typename raw_map_t::iterator i = raw_map_->find(shm_key);
        if (i == raw_map_->end()) {
            // value not in cache
            return boost::none;
        } else {
            return std::string(i->second.begin(), i->second.end());
        }
    }

    void clear() {
        raw_map_->clear();
    }

private:
    const size_t mem_capacity_;
    std::string path_;

    managed_mapped_file segment_;
    void_allocator_t alloc_inst_;

    raw_map_t* raw_map_;
};
}  // namespace interprocess
}  // namespace boost

// Forward declarations
namespace tkrzw { class CacheDBM; }

namespace faas {
namespace log {

class LRUCache {
public:
    explicit LRUCache(uint32_t user_logspace, int mem_cap_mb);
    LRUCache(LRUCache&& other) {
        log_header_ = std::move(other.log_header_);
        dbm_ = std::move(other.dbm_);
    }
    ~LRUCache();

    void Put(const LogMetaData& log_metadata, std::span<const uint64_t> user_tags,
             std::span<const char> log_data);
    std::optional<LogEntry> Get(uint64_t seqnum);

    void PutAuxData(uint64_t seqnum, std::span<const char> data);
    std::optional<std::string> GetAuxData(uint64_t seqnum);

private:
    std::string log_header_;

    struct DeleteDBM {
        void operator()(tkrzw::CacheDBM* dbm) { dbm->Close(); }
    };
    std::unique_ptr<tkrzw::CacheDBM, DeleteDBM> dbm_;

    DISALLOW_COPY_AND_ASSIGN(LRUCache);
};

class SharedLRUCache {
public:
    explicit SharedLRUCache(uint32_t user_logspace, int mem_cap_mb,
                            std::string_view path);
    SharedLRUCache(SharedLRUCache&& other) {
        log_header_ = std::move(other.log_header_);
        lockable_dbm_ = std::move(other.lockable_dbm_);
    }

    void Put(const LogMetaData& log_metadata, std::span<const uint64_t> user_tags,
             std::span<const char> log_data);
    std::optional<LogEntry> Get(uint64_t seqnum);

    void PutAuxData(uint64_t seqnum, std::span<const char> data);
    std::optional<std::string> GetAuxData(uint64_t seqnum);

private:
    std::string log_header_;

    LockablePtr<boost::interprocess::LRUCache> lockable_dbm_;

    DISALLOW_COPY_AND_ASSIGN(SharedLRUCache);
};


template<class TCache>
class CacheManager {
public:
    CacheManager(bool enable, int capacity)
    : log_header_("CacheManager"),
      enable_cache_(enable),
      cap_per_user_(capacity) {}

    void Put(const LogMetaData& log_metadata,
             std::span<const uint64_t> user_tags,
             std::span<const char> log_data) {
        if (!enable_cache_) {
            return;
        }
        HVLOG_F(1, "Store cache for log entry seqnum={:016X}", log_metadata.seqnum);
        uint32_t user_logspace = log_metadata.user_logspace;
        auto& cache = GetOrCreateCache(user_logspace);
        cache.Put(log_metadata, user_tags, log_data);
    }

    std::optional<LogEntry> Get(uint32_t user_logspace, uint64_t seqnum) {
        if (!enable_cache_) {
            return std::nullopt;
        }
        const auto& cache = GetCache(user_logspace);
        if (cache.has_value()) {
            return cache.value().get().Get(seqnum);
        } else {
            return std::nullopt;
        }
    }
    void PutAuxData(uint32_t user_logspace, uint64_t seqnum, std::span<const char> data) {
        if (!enable_cache_) {
            return;
        }
        auto& cache = GetOrCreateCache(user_logspace);
        cache.PutAuxData(seqnum, data);
    }

    std::optional<std::string> GetAuxData(uint32_t user_logspace, uint64_t seqnum) {
        if (!enable_cache_) {
            return std::nullopt;
        }
        const auto& cache = GetCache(user_logspace);
        if (cache.has_value()) {
            return cache.value().get().GetAuxData(seqnum);
        } else {
            return std::nullopt;
        }
    }

private:
    std::string log_header_;
    bool enable_cache_;
    int cap_per_user_;

    absl::Mutex cache_map_mu_;
    absl::flat_hash_map<uint32_t /*user_logspace*/, TCache> log_caches_
        ABSL_GUARDED_BY(cache_map_mu_);

    std::optional<std::reference_wrapper<TCache>> GetCache(uint32_t user_logspace) {
        absl::ReaderMutexLock rlk(&cache_map_mu_);
        if (!log_caches_.contains(user_logspace)) {
            return std::nullopt;
        }
        return std::optional<std::reference_wrapper<TCache>>(log_caches_.at(user_logspace));
    }

    TCache& GetOrCreateCache(uint32_t user_logspace) {
        absl::MutexLock lk(&cache_map_mu_);
        // TODO: isolate by users
        if (log_caches_.contains(user_logspace)) {
            return log_caches_.at(user_logspace);
        } else {
            log_caches_.emplace(
                std::piecewise_construct, std::forward_as_tuple(user_logspace),
                std::forward_as_tuple(user_logspace, cap_per_user_,
                                      ipc::GetCacheShmFile(user_logspace).c_str()));
            return log_caches_.at(user_logspace);
        }
    }
};

// template<class SharedLRUCache>
// class CacheManager {
// public:
//     CacheManager(bool enable, int capacity)
//     : log_header_("CacheManager"),
//       enable_cache_(enable),
//       cap_per_user_(capacity),
//       cache_(/*user_logspace*/0u, cap_per_user_,
//              ipc::GetCacheShmFile(/*user_logspace*/0u).c_str()) {}

//     void Put(const LogMetaData& log_metadata,
//              std::span<const uint64_t> user_tags,
//              std::span<const char> log_data) {
//         if (!enable_cache_) {
//             return;
//         }
//         HVLOG_F(1, "Store cache for log entry seqnum={:016X}", log_metadata.seqnum);
//         uint32_t user_logspace = log_metadata.user_logspace;
//         DCHECK_EQ(user_logspace, 0u);
//         cache_.Put(log_metadata, user_tags, log_data);
//     }

//     std::optional<LogEntry> Get(uint32_t user_logspace, uint64_t seqnum) {
//         if (!enable_cache_) {
//             return std::nullopt;
//         }
//         DCHECK_EQ(user_logspace, 0u);
//         return cache_.Get(seqnum);
//     }
//     void PutAuxData(uint32_t user_logspace, uint64_t seqnum, std::span<const char> data) {
//         if (!enable_cache_) {
//             return;
//         }
//         DCHECK_EQ(user_logspace, 0u);
//         cache_.PutAuxData(seqnum, data);
//     }

//     std::optional<std::string> GetAuxData(uint32_t user_logspace, uint64_t seqnum) {
//         if (!enable_cache_) {
//             return std::nullopt;
//         }
//         DCHECK_EQ(user_logspace, 0u);
//         return cache_.GetAuxData(seqnum);
//     }

// private:
//     std::string log_header_;
//     bool enable_cache_;
//     int cap_per_user_;

//     SharedLRUCache cache_;
// };

}  // namespace log
}  // namespace faas
