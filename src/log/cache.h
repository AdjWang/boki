#pragma once

#include "log/common.h"
#include "utils/fs.h"
#include "utils/lockable_ptr.h"
#include "ipc/base.h"

__BEGIN_THIRD_PARTY_HEADERS
#include <tkrzw_dbm_cache.h>
#include <tkrzw_dbm_hash.h>
#include <tkrzw_file_mmap.h>
__END_THIRD_PARTY_HEADERS

namespace boost {
namespace interprocess {
class LRUCache {
public:
    typedef managed_mapped_file::allocator<char>::type char_allocator_t;
    typedef basic_string<char, std::char_traits<char>, char_allocator_t> shm_string_t;

    typedef allocator<shm_string_t, managed_mapped_file::segment_manager> list_allocator_t;
    typedef list<shm_string_t, list_allocator_t> list_t;

    typedef std::pair<shm_string_t, typename list_t::iterator> mapped_t;
    typedef std::pair<const shm_string_t, mapped_t> value_t;
    typedef allocator<value_t, managed_mapped_file::segment_manager> map_allocator_t;
    typedef map<shm_string_t, mapped_t, std::less<shm_string_t>, map_allocator_t> map_t;

    LRUCache(size_t capacity, const char* path)
        : capacity_(capacity),
          path_(path),
#ifdef __COMPILE_AS_SHARED
          segment_(open_only, path),
          ca_(segment_.get_allocator<char>()),
          lru_list_(segment_.find<list_t>("lru_list").first),
          lru_map_(segment_.find<map_t>("lru_map").first)
#else
          segment_(create_only, path, capacity),
          ca_(segment_.get_allocator<char>()),
          list_alloc_inst_(segment_.get_segment_manager()),
          map_alloc_inst_(segment_.get_segment_manager()),
          lru_list_(segment_.construct<list_t>("lru_list")(list_alloc_inst_)),
          lru_map_(segment_.construct<map_t>("lru_map")(std::less<shm_string_t>(),
                                                        map_alloc_inst_))
#endif
        {}

    ~LRUCache() {
#ifndef __COMPILE_AS_SHARED
        file_mapping::remove(path_);
#endif
    }

    size_t size() const { return lru_map_->size(); }

    size_t capacity() const { return capacity_; }

    bool empty() const { return lru_map_->empty(); }

    bool contains(const std::string& key) {
        shm_string_t shm_key(key.c_str(), ca_);
        return lru_map_->find(shm_key) != lru_map_->end();
    }

    void insert(const std::string& key, const std::string& value) {
        shm_string_t shm_key(key.c_str(), ca_);
        typename map_t::iterator i = lru_map_->find(shm_key);
        if (i == lru_map_->end()) {
            // insert item into the cache, but first check if it is full
            if (size() >= capacity_) {
                // cache is full, evict the least recently used item
                evict();
            }

            // insert the new item
            lru_list_->push_front(shm_key);
            shm_string_t shm_value(value.begin(), value.end(), ca_);
            lru_map_->emplace(shm_key, std::make_pair(shm_value, lru_list_->begin()));
        }
    }

    boost::optional<std::string> get(const std::string& key) {
        // lookup value in the cache
        shm_string_t shm_key(key.c_str(), ca_);
        typename map_t::iterator i = lru_map_->find(shm_key);
        if (i == lru_map_->end()) {
            // value not in cache
            return boost::none;
        }

        // return the value, but first update its place in the most
        // recently used list
        typename list_t::iterator j = i->second.second;
        if (j != lru_list_->begin()) {
            // move item to the front of the most recently used list
            lru_list_->erase(j);
            lru_list_->push_front(shm_key);

            // update iterator in map
            j = lru_list_->begin();
            const shm_string_t& value = i->second.first;
            lru_map_->at(shm_key) = std::make_pair(value, j);

            // return the value
            return std::string(value.begin(), value.end());
        } else {
            // the item is already at the front of the most recently
            // used list so just return it
            return std::string(i->second.first.begin(), i->second.first.end());
        }
    }

    void clear() {
        lru_map_->clear();
        lru_list_->clear();
    }

private:
    size_t capacity_;
    const char* path_;

    managed_mapped_file segment_;
    char_allocator_t ca_;
#ifndef __COMPILE_AS_SHARED
    list_allocator_t list_alloc_inst_;
    map_allocator_t map_alloc_inst_;
#endif

    list_t* lru_list_;
    map_t* lru_map_;

    void evict() {
        // evict item from the end of most recently used list
        typename list_t::iterator i = --lru_list_->end();
        lru_map_->erase(*i);
        lru_list_->erase(i);
    }
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
                            const char* path);
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

}  // namespace log
}  // namespace faas
