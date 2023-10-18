#include <index_data_c.h>
#include "base/init.h"
#include "log/index_data.h"
#include "log/cache.h"
#include "utils/lockable_ptr.h"
#include "utils/hash.h"
#include "common/stat.h"

static absl::Mutex g_cache_mu;
static std::atomic<faas::log::SharedLRUCache*> g_cache = NULL;

static absl::Mutex g_hash_meta_cache_mu;
static absl::flat_hash_map<
    uint16_t /*view_id*/,
    std::pair<uint64_t /*log_space_hash_seed*/,
              absl::FixedArray<uint16_t> /*log_space_hash_tokens*/>>
    g_hash_meta_cache;

// STAT
static absl::Mutex stat_mu_;
static faas::stat::StatisticsCollector<int32_t> index_read_delay_stat_(
        faas::stat::StatisticsCollector<int32_t>::StandardReportCallback(
            "index_read_delay"));
static faas::stat::StatisticsCollector<int32_t> cache_read_delay_stat_(
        faas::stat::StatisticsCollector<int32_t>::StandardReportCallback(
            "cache_read_delay"));

static faas::stat::StatisticsCollector<int32_t> index_lock_stat_(
        faas::stat::StatisticsCollector<int32_t>::StandardReportCallback(
            "index_lock"));

static faas::log::SharedLRUCache* GetOrCreateCache(uint32_t user_logspace) {
    if (g_cache.load() != NULL) {
        return g_cache.load();
    } else {
        absl::MutexLock lk(&g_cache_mu);
        if (g_cache.load() != NULL) {
            return g_cache.load();
        }
        std::string shared_cache_path = faas::ipc::GetCacheShmFile(user_logspace);
        if (!faas::fs_utils::Exists(shared_cache_path)) {
            return NULL;
        }
        g_cache.store(new faas::log::SharedLRUCache(
            user_logspace, /*mem_cap_mb*/ -1, shared_cache_path.c_str()));
        return g_cache.load();
    }
}

static std::optional<faas::protocol::Message> TryGetCache(uint32_t user_logspace,
                                                          uint64_t metalog_progress,
                                                          uint64_t seqnum) {
    faas::log::SharedLRUCache* log_cache = GetOrCreateCache(user_logspace);
    if (__FAAS_PREDICT_FALSE(log_cache == NULL)) {
        return std::nullopt;
    }
    auto cached_log_entry = log_cache->Get(seqnum);
    if (!cached_log_entry.has_value()) {
        return std::nullopt;
    }
    // Cache hits
    VLOG_F(1, "Cache hits for log entry seqnum={:016X}", seqnum);
    const faas::log::LogEntry& log_entry = cached_log_entry.value();
    std::optional<std::string> cached_aux_data = log_cache->GetAuxData(seqnum);
    std::span<const char> aux_data;
    if (cached_aux_data.has_value()) {
        size_t full_size = log_entry.data.size()
                            + log_entry.user_tags.size() * sizeof(uint64_t)
                            + cached_aux_data->size();
        if (full_size <= MESSAGE_INLINE_DATA_SIZE) {
            aux_data = STRING_AS_SPAN(*cached_aux_data);
        } else {
            LOG_F(WARNING, "Inline buffer of message not large enough "
                           "for auxiliary data of log seqnum={:016X}: "
                           "log_size={}, num_tags={} aux_data_size={}",
                    seqnum, log_entry.data.size(),
                    log_entry.user_tags.size(), cached_aux_data->size());
        }
    }
    // DEBUG
    VLOG_F(1, "aux data size={}", aux_data.size());
    faas::protocol::Message response =
        faas::protocol::MessageHelper::BuildLocalReadOKResponse(
            log_entry.metadata.seqnum, VECTOR_AS_SPAN(log_entry.user_tags),
            STRING_AS_SPAN(log_entry.data));
    response.log_aux_data_size = gsl::narrow_cast<uint16_t>(aux_data.size());
    faas::protocol::MessageHelper::AppendInlineData(&response, aux_data);
    response.metalog_progress = metalog_progress;
    return response;
}


enum APIReturnValue {
    ReadOK = faas::log::IndexQueryResult::kFound,   // always be 0

    // enum State { kFound, kEmpty, kContinue, kPending };
    IndexReadEmpty = faas::log::IndexQueryResult::kEmpty,
    IndexReadContinue = faas::log::IndexQueryResult::kContinue,
    IndexReadPending = faas::log::IndexQueryResult::kPending,

    IndexReadInitFutureViewBail = -1,
    IndexReadInitCurrentViewPending = -2,
    IndexReadContinueOK = -3,

    LogReadCacheMiss = -4,
    
    AuxDataSetOK = 0,
    // The specified user not existing.
    // User function does not have the user table, so the first setting
    // must through engine.
    AuxDataInvalidUser = -5,
};

typedef faas::LockablePtr<faas::log::IndexDataManager> shared_index_t;
#define SHARED_INDEX_CAST(index_data_ptr) \
    reinterpret_cast<shared_index_t*>(index_data_ptr)

// DEBUG
int test_func(uint32_t var_in, uint64_t* var_in_out, uint64_t* var_out) {
    fprintf(stderr, "test func var_in=%d var_in_out=%ld\n", var_in, *var_in_out);
    // auto index_data = faas::log::IndexDataManager(1u);
    // index_data.set_indexed_metalog_position(4u);
    *var_in_out = var_in + 1;
    *var_out = var_in + 2;
    fprintf(stderr, "test func create index_data var_out=%ld\n", *var_out);
    return -1;
}
// DEBUG
void Inspect(void* index_data) {
    auto locked_index_data = SHARED_INDEX_CAST(index_data)->Lock();
    locked_index_data->Inspect();
}

uint32_t GetLogSpaceIdentifier(uint32_t user_logspace) {
    // get the last view
    constexpr const char* view_dir_prefix = "view_";
    constexpr const size_t prefix_size = 5u;
    uint16_t max_view_id = 0;

    struct dirent* dp;
    std::string root_path_for_shm(faas::ipc::GetRootPathForShm());
    DIR* dir = opendir(root_path_for_shm.c_str());
    while ((dp = readdir(dir)) != NULL) {
        if (strncmp(view_dir_prefix, dp->d_name, prefix_size) == 0) {
            uint16_t view_id = static_cast<uint16_t>(atoi(&dp->d_name[prefix_size]));
            max_view_id = std::max(max_view_id, view_id);
        }
    }
    closedir(dir);
    // deserialize log space hash meta
    // metadata serialization see Engine::SetupViewIPCMeta()
    {
        absl::ReaderMutexLock rlk(&g_hash_meta_cache_mu);
        if (g_hash_meta_cache.contains(max_view_id)) {
            const auto& [log_space_hash_seed, log_space_hash_tokens] =
                g_hash_meta_cache.at(max_view_id);
            // get logspace_id by user_logspace hash
            // the same calculation as View::LogSpaceIdentifier()
            uint64_t h = faas::hash::xxHash64(user_logspace, /* seed= */ log_space_hash_seed);
            uint16_t node_id = log_space_hash_tokens[h % log_space_hash_tokens.size()];
            return faas::bits::JoinTwo16(max_view_id, node_id);
        }
    }
    const std::string viewshm_path(faas::ipc::GetViewShmPath(max_view_id));
    const std::string viewmeta_path(faas::ipc::GetLogSpaceHashMetaPath(viewshm_path));
    std::string log_space_hash_meta_data;
    bool success = faas::fs_utils::ReadContents(viewmeta_path, &log_space_hash_meta_data);
    DCHECK(success) << viewmeta_path;
    DCHECK_GE(log_space_hash_meta_data.size(),
              sizeof(uint64_t) + sizeof(uint16_t));
    uint64_t log_space_hash_seed =
        *(reinterpret_cast<uint64_t*>(log_space_hash_meta_data.data()));
    size_t n_hash_tokens =
        (log_space_hash_meta_data.size() - sizeof(uint64_t)) / sizeof(uint16_t);
    absl::FixedArray<uint16_t> log_space_hash_tokens(n_hash_tokens);
    memcpy(log_space_hash_tokens.data(),
           log_space_hash_meta_data.data() + sizeof(uint64_t),
           n_hash_tokens * sizeof(uint16_t));
    {
        absl::MutexLock lk(&g_hash_meta_cache_mu);
        g_hash_meta_cache.emplace(
            max_view_id,
            std::make_pair(log_space_hash_seed, log_space_hash_tokens));
    }
    // get logspace_id by user_logspace hash
    // the same calculation as View::LogSpaceIdentifier()
    uint64_t h = faas::hash::xxHash64(user_logspace, /* seed= */ log_space_hash_seed);
    uint16_t node_id = log_space_hash_tokens[h % log_space_hash_tokens.size()];
    return faas::bits::JoinTwo16(max_view_id, node_id);
}

// Initialize explicitly to avoid manually setting priority for all global variables,
// required by __attribute__ ((constructor)). Details see:
// https://stackoverflow.com/questions/43941159/global-static-variables-initialization-issue-with-attribute-constructor-i
void Init(const char* ipc_root_path, int vlog_level) {
    faas::logging::Init(vlog_level);
    faas::base::SetupSignalHandler();
    LOG_F(INFO, "Init set ipc_root_path={}", ipc_root_path);
    faas::ipc::SetRootPathForIpc(ipc_root_path, /* create= */ false);
#if defined(DEBUG)
    LOG(INFO) << "Running DEBUG built version";
    absl::SetMutexDeadlockDetectionMode(absl::OnDeadlockCycle::kAbort);
#endif
#if defined(NDEBUG)
    LOG(INFO) << "Running RELEASE built version";
#endif
}

void* ConstructIndexData(uint64_t metalog_progress, uint32_t logspace_id,
                         uint32_t user_logspace) {
    if (!faas::ipc::CheckIndexMetaFile(user_logspace, logspace_id)) {
        // uint16_t view_id = faas::bits::HighHalf32(logspace_id);
        // std::string viewshm_path(faas::ipc::GetViewShmPath(view_id));
        // // should have been created when installing the view by engine
        // bool view_exists = faas::fs_utils::Exists(viewshm_path);
        // VLOG_F(1, "view_path={}|{}", viewshm_path, view_exists);
        // std::string indexshm_path(
        //     faas::fs_utils::JoinPath(viewshm_path, fmt::format("index_{}", logspace_id),
        //                              faas::ipc::GetIndexSegmentName(user_logspace, logspace_id)));
        // bool index_exists = faas::fs_utils::Exists(indexshm_path);
        // VLOG_F(1, "index_path={}|{}", indexshm_path, index_exists);
        VLOG_F(1, "ConstructIndexData IndexMetaPath check failed "
                  "index user_logspace={:08X} logspace_id={:08X} ",
                  user_logspace, logspace_id);
        return NULL;
    }
    auto index_data = std::unique_ptr<faas::log::IndexDataManager>(
        new faas::log::IndexDataManager(logspace_id));
    uint64_t index_metalog_progress = index_data->index_metalog_progress();
    if (metalog_progress > index_metalog_progress) {
        VLOG_F(1, "ConstructIndexData metalog_progress={:016X} not satisify future "
                  "index metalog_progress={:016X}",
                  index_metalog_progress, metalog_progress);
        return NULL;
    }
    index_data->LoadIndexData(user_logspace);
    std::string mu_name = faas::ipc::GetIndexMutexFile(logspace_id);
    shared_index_t* lockable_index_data =
        new faas::LockablePtr(std::move(index_data), mu_name.c_str());
    return lockable_index_data;
}

void DestructIndexData(void* index_data) {
    delete SHARED_INDEX_CAST(index_data);
}

int IndexReadLocalId(void* index_data, /*InOut*/ uint64_t* metalog_progress,
                     uint32_t user_logspace, uint64_t localid,
                     /*Out*/ uint64_t* seqnum, /*Out*/ uint16_t* engine_id) {
    // STAT
    int64_t ts = faas::GetMonotonicMicroTimestamp();
    auto locked_index_data = SHARED_INDEX_CAST(index_data)->Lock();
    index_lock_stat_.AddSample(gsl::narrow_cast<int32_t>(faas::GetMonotonicMicroTimestamp()-ts));

    faas::log::IndexQuery query = faas::log::IndexQuery {
        .direction = faas::log::IndexQuery::kReadLocalId,
        .initial = true,
        .user_logspace = user_logspace,
        .query_seqnum = localid,
        .metalog_progress = *metalog_progress,
    };
    faas::log::IndexDataManager::QueryConsistencyType consistency_type =
        locked_index_data->CheckConsistency(query);
    switch (consistency_type) {
        case faas::log::IndexDataManager::QueryConsistencyType::kInitFutureViewBail:
            return APIReturnValue::IndexReadInitFutureViewBail;
        case faas::log::IndexDataManager::QueryConsistencyType::kInitCurrentViewPending:
            return APIReturnValue::IndexReadInitCurrentViewPending;
        case faas::log::IndexDataManager::QueryConsistencyType::kInitCurrentViewOK:
        case faas::log::IndexDataManager::QueryConsistencyType::kInitPastViewOK: {
            faas::log::IndexQueryResult result =
                locked_index_data->ProcessLocalIdQuery(query);
            if (result.state == faas::log::IndexQueryResult::kFound) {
                *metalog_progress = result.metalog_progress;
                *seqnum = result.found_result.seqnum;
                *engine_id = result.found_result.engine_id;
            }
            return static_cast<int>(result.state);
        }
        case faas::log::IndexDataManager::QueryConsistencyType::kContOK:
        default:
            return APIReturnValue::IndexReadContinueOK;
    }
}

int IndexReadNext(void* index_data, /*InOut*/ uint64_t* metalog_progress,
                  uint32_t user_logspace, uint64_t query_seqnum,
                  uint64_t query_tag, /*Out*/ uint64_t* seqnum, /*Out*/ uint16_t* engine_id) {
    // STAT
    int64_t ts = faas::GetMonotonicMicroTimestamp();
    auto locked_index_data = SHARED_INDEX_CAST(index_data)->Lock();
    index_lock_stat_.AddSample(gsl::narrow_cast<int32_t>(faas::GetMonotonicMicroTimestamp()-ts));

    faas::log::IndexQuery query = faas::log::IndexQuery {
        .direction = faas::log::IndexQuery::kReadNext,
        .initial = true,
        .user_logspace = user_logspace,
        .user_tag = query_tag,
        .query_seqnum = query_seqnum,
        .metalog_progress = *metalog_progress,
    };
    faas::log::IndexDataManager::QueryConsistencyType consistency_type =
        locked_index_data->CheckConsistency(query);
    switch (consistency_type) {
        case faas::log::IndexDataManager::QueryConsistencyType::kInitFutureViewBail:
            return APIReturnValue::IndexReadInitFutureViewBail;
        case faas::log::IndexDataManager::QueryConsistencyType::kInitCurrentViewPending:
            return APIReturnValue::IndexReadInitCurrentViewPending;
        case faas::log::IndexDataManager::QueryConsistencyType::kInitCurrentViewOK:
        case faas::log::IndexDataManager::QueryConsistencyType::kInitPastViewOK: {
            faas::log::IndexQueryResult result = locked_index_data->ProcessReadNext(query);
            if (result.state == faas::log::IndexQueryResult::kFound) {
                *metalog_progress = result.metalog_progress;
                *seqnum = result.found_result.seqnum;
                *engine_id = result.found_result.engine_id;
            }
            return static_cast<int>(result.state);
        }
        case faas::log::IndexDataManager::QueryConsistencyType::kContOK:
        default:
            return APIReturnValue::IndexReadContinueOK;
    }
}

int IndexReadPrev(void* index_data, /*InOut*/ uint64_t* metalog_progress,
                  uint32_t user_logspace, uint64_t query_seqnum,
                  uint64_t query_tag, /*Out*/ uint64_t* seqnum, /*Out*/ uint16_t* engine_id) {
    // STAT
    int64_t ts = faas::GetMonotonicMicroTimestamp();
    auto locked_index_data = SHARED_INDEX_CAST(index_data)->Lock();
    index_lock_stat_.AddSample(gsl::narrow_cast<int32_t>(faas::GetMonotonicMicroTimestamp()-ts));

    faas::log::IndexQuery query = faas::log::IndexQuery {
        .direction = faas::log::IndexQuery::kReadPrev,
        .initial = true,
        .user_logspace = user_logspace,
        .user_tag = query_tag,
        .query_seqnum = query_seqnum,
        .metalog_progress = *metalog_progress,
    };
    faas::log::IndexDataManager::QueryConsistencyType consistency_type =
        locked_index_data->CheckConsistency(query);
    switch (consistency_type) {
        case faas::log::IndexDataManager::QueryConsistencyType::kInitFutureViewBail:
            return APIReturnValue::IndexReadInitFutureViewBail;
        case faas::log::IndexDataManager::QueryConsistencyType::kInitCurrentViewPending:
            return APIReturnValue::IndexReadInitCurrentViewPending;
        case faas::log::IndexDataManager::QueryConsistencyType::kInitCurrentViewOK:
        case faas::log::IndexDataManager::QueryConsistencyType::kInitPastViewOK: {
            faas::log::IndexQueryResult result = locked_index_data->ProcessReadPrev(query);
            if (result.state == faas::log::IndexQueryResult::kFound) {
                *metalog_progress = result.metalog_progress;
                *seqnum = result.found_result.seqnum;
                *engine_id = result.found_result.engine_id;
            }
            return static_cast<int>(result.state);
        }
        case faas::log::IndexDataManager::QueryConsistencyType::kContOK:
        default:
            return APIReturnValue::IndexReadContinueOK;
    }
}
#undef SHARED_INDEX_CAST


int LogReadLocalId(void* index_data, uint64_t metalog_progress,
                   uint32_t user_logspace, uint64_t localid,
                   /*Out*/ void* response) {
    uint64_t _InOut_metalog_progress = metalog_progress;
    uint64_t _Out_seqnum = 0u;
    uint16_t _Out_engine_id = 0u;
    int ret = IndexReadLocalId(index_data, &_InOut_metalog_progress,
                               user_logspace, localid, &_Out_seqnum, &_Out_engine_id);
    if (ret != APIReturnValue::ReadOK) {
        return ret;
    }
    std::optional<faas::protocol::Message> message =
        TryGetCache(user_logspace, _InOut_metalog_progress, _Out_seqnum);
    if (message.has_value()) {
        memcpy(response, static_cast<void*>(&message.value()),
            sizeof(faas::protocol::Message));
        return APIReturnValue::ReadOK;
    } else {
        faas::protocol::Message response_without_data =
            faas::protocol::MessageHelper::BuildIndexReadOKResponse(_Out_seqnum, _Out_engine_id);
        response_without_data.metalog_progress = _InOut_metalog_progress;
        memcpy(response, static_cast<void*>(&response_without_data),
               sizeof(faas::protocol::Message));
        return APIReturnValue::LogReadCacheMiss;
    }
}

int LogReadNext(void* index_data, uint64_t metalog_progress,
                uint32_t user_logspace, uint64_t query_seqnum,
                uint64_t query_tag, /*Out*/ void* response) {
    uint64_t _InOut_metalog_progress = metalog_progress;
    uint64_t _Out_seqnum = 0u;
    uint16_t _Out_engine_id = 0u;
    // STAT
    uint64_t ts = faas::GetMonotonicMicroTimestamp();
    int ret = IndexReadNext(index_data, &_InOut_metalog_progress, user_logspace,
                            query_seqnum, query_tag, &_Out_seqnum, &_Out_engine_id);
    {
        absl::MutexLock lk(&stat_mu_);
        index_read_delay_stat_.AddSample(gsl::narrow_cast<int32_t>(faas::GetMonotonicMicroTimestamp()-ts));
    }
    if (ret != APIReturnValue::ReadOK) {
        return ret;
    }
    // STAT
    ts = faas::GetMonotonicMicroTimestamp();
    std::optional<faas::protocol::Message> message =
        TryGetCache(user_logspace, _InOut_metalog_progress, _Out_seqnum);
    {
        absl::MutexLock lk(&stat_mu_);
        cache_read_delay_stat_.AddSample(gsl::narrow_cast<int32_t>(faas::GetMonotonicMicroTimestamp()-ts));
    }
    if (message.has_value()) {
        memcpy(response, static_cast<void*>(&message.value()),
               sizeof(faas::protocol::Message));
        return APIReturnValue::ReadOK;
    } else {
        faas::protocol::Message response_without_data =
            faas::protocol::MessageHelper::BuildIndexReadOKResponse(_Out_seqnum, _Out_engine_id);
        response_without_data.metalog_progress = _InOut_metalog_progress;
        memcpy(response, static_cast<void*>(&response_without_data),
               sizeof(faas::protocol::Message));
        return APIReturnValue::LogReadCacheMiss;
    }
}

int LogReadPrev(void* index_data, uint64_t metalog_progress,
                uint32_t user_logspace, uint64_t query_seqnum,
                uint64_t query_tag, /*Out*/ void* response) {
    uint64_t _InOut_metalog_progress = metalog_progress;
    uint64_t _Out_seqnum = 0u;
    uint16_t _Out_engine_id = 0u;
    // STAT
    uint64_t ts = faas::GetMonotonicMicroTimestamp();
    int ret = IndexReadPrev(index_data, &_InOut_metalog_progress, user_logspace,
                            query_seqnum, query_tag, &_Out_seqnum, &_Out_engine_id);
    {
        absl::MutexLock lk(&stat_mu_);
        index_read_delay_stat_.AddSample(gsl::narrow_cast<int32_t>(faas::GetMonotonicMicroTimestamp()-ts));
    }
    if (ret != APIReturnValue::ReadOK) {
        return ret;
    }
    // STAT
    ts = faas::GetMonotonicMicroTimestamp();
    std::optional<faas::protocol::Message> message =
        TryGetCache(user_logspace, _InOut_metalog_progress, _Out_seqnum);
    {
        absl::MutexLock lk(&stat_mu_);
        cache_read_delay_stat_.AddSample(gsl::narrow_cast<int32_t>(faas::GetMonotonicMicroTimestamp()-ts));
    }
    if (message.has_value()) {
        memcpy(response, static_cast<void*>(&message.value()),
               sizeof(faas::protocol::Message));
        return APIReturnValue::ReadOK;
    } else {
        faas::protocol::Message response_without_data =
            faas::protocol::MessageHelper::BuildIndexReadOKResponse(_Out_seqnum, _Out_engine_id);
        response_without_data.metalog_progress = _InOut_metalog_progress;
        memcpy(response, static_cast<void*>(&response_without_data),
               sizeof(faas::protocol::Message));
        return APIReturnValue::LogReadCacheMiss;
    }
}

int SetAuxData(uint32_t user_logspace, uint64_t seqnum, void* data, size_t len) {
    DCHECK(data != nullptr);
    DCHECK_GT(len, 0u);
    faas::log::SharedLRUCache* log_cache = GetOrCreateCache(user_logspace);
    if (log_cache != NULL) {
        log_cache->PutAuxData(
            seqnum,
            std::span<const char>(reinterpret_cast<const char*>(data), len));
        return APIReturnValue::AuxDataSetOK;
    } else {
        return APIReturnValue::AuxDataInvalidUser;
    }
}
