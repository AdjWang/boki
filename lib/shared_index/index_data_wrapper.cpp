#include <index_data_c.h>
#include "log/index_data.h"
#include "utils/lockable_ptr.h"
#include "base/init.h"

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

// Initialize explicitly to avoid manually setting priority for all global variables,
// required by __attribute__ ((constructor)). Details see:
// https://stackoverflow.com/questions/43941159/global-static-variables-initialization-issue-with-attribute-constructor-i
void Init(const char* ipc_root_path) {
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

void* ConstructIndexData(uint32_t logspace_id, uint32_t user_logspace) {
    auto index_data = std::unique_ptr<faas::log::IndexDataManager>(
        new faas::log::IndexDataManager(logspace_id));
    index_data->LoadIndexData(user_logspace);
    shared_index_t* lockable_index_data =
        new faas::LockablePtr(std::move(index_data), logspace_id);
    return lockable_index_data;
}

void DestructIndexData(void* index_data) {
    delete SHARED_INDEX_CAST(index_data);
}

int ProcessLocalIdQuery(void* index_data, /*InOut*/ uint64_t* metalog_progress,
                        uint64_t localid, /*Out*/ uint64_t* seqnum) {
    auto locked_index_data = SHARED_INDEX_CAST(index_data)->Lock();
    faas::log::IndexQuery query = faas::log::IndexQuery {
        .direction = faas::log::IndexQuery::kReadLocalId,
        .initial = true,
        .query_seqnum = localid,
        .metalog_progress = *metalog_progress,
    };
    faas::log::IndexDataManager::QueryConsistencyType consistency_type =
        locked_index_data->CheckConsistency(query);
    switch (consistency_type) {
        case faas::log::IndexDataManager::QueryConsistencyType::kInitFutureViewBail:
            return -1;
        case faas::log::IndexDataManager::QueryConsistencyType::kInitCurrentViewPending:
            return -2;
        case faas::log::IndexDataManager::QueryConsistencyType::kInitCurrentViewOK:
        case faas::log::IndexDataManager::QueryConsistencyType::kInitPastViewOK: {
            faas::log::IndexQueryResult result =
                locked_index_data->ProcessLocalIdQuery(query);
            if (result.state == faas::log::IndexQueryResult::kFound) {
                *metalog_progress = result.metalog_progress;
                *seqnum = result.found_result.seqnum;
            }
            return static_cast<int>(result.state);
        }
        case faas::log::IndexDataManager::QueryConsistencyType::kContOK:
        default:
            return -3;
    }
}

int ProcessReadNext(void* index_data, /*InOut*/ uint64_t* metalog_progress,
                    uint32_t user_logspace, uint64_t query_seqnum,
                    uint64_t query_tag, /*Out*/ uint64_t* seqnum) {
    auto locked_index_data = SHARED_INDEX_CAST(index_data)->Lock();
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
            return -1;
        case faas::log::IndexDataManager::QueryConsistencyType::kInitCurrentViewPending:
            return -2;
        case faas::log::IndexDataManager::QueryConsistencyType::kInitCurrentViewOK:
        case faas::log::IndexDataManager::QueryConsistencyType::kInitPastViewOK: {
            faas::log::IndexQueryResult result = locked_index_data->ProcessReadNext(query);
            if (result.state == faas::log::IndexQueryResult::kFound) {
                *metalog_progress = result.metalog_progress;
                *seqnum = result.found_result.seqnum;
            }
            return static_cast<int>(result.state);
        }
        case faas::log::IndexDataManager::QueryConsistencyType::kContOK:
        default:
            return -3;
    }
}

int ProcessReadPrev(void* index_data, /*InOut*/ uint64_t* metalog_progress,
                    uint32_t user_logspace, uint64_t query_seqnum,
                    uint64_t query_tag, /*Out*/ uint64_t* seqnum) {
    auto locked_index_data = SHARED_INDEX_CAST(index_data)->Lock();
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
            return -1;
        case faas::log::IndexDataManager::QueryConsistencyType::kInitCurrentViewPending:
            return -2;
        case faas::log::IndexDataManager::QueryConsistencyType::kInitCurrentViewOK:
        case faas::log::IndexDataManager::QueryConsistencyType::kInitPastViewOK: {
            faas::log::IndexQueryResult result = locked_index_data->ProcessReadPrev(query);
            if (result.state == faas::log::IndexQueryResult::kFound) {
                *metalog_progress = result.metalog_progress;
                *seqnum = result.found_result.seqnum;
            }
            return static_cast<int>(result.state);
        }
        case faas::log::IndexDataManager::QueryConsistencyType::kContOK:
        default:
            return -3;
    }
}
