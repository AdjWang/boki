#define __FAAS_USED_IN_BINDING
#include "ipc/base.h"
#include "utils/fs.h"
#include "utils/bits.h"

namespace faas {
namespace ipc {

namespace {
std::string root_path_for_ipc    = "NOT_SET";
std::string engine_unix_ipc_path = "NOT_SET";
std::string root_path_for_shm    = "NOT_SET";
std::string root_path_for_fifo   = "NOT_SET";

static constexpr const char* kEngineUnixSocket = "engine.sock";
static constexpr const char* kShmSubPath = "shm";
static constexpr const char* kFifoSubPath = "fifo";
}

void SetRootPathForIpc(std::string_view path, bool create) {
    root_path_for_ipc = std::string(path);
    engine_unix_ipc_path = fs_utils::JoinPath(root_path_for_ipc, kEngineUnixSocket);
    root_path_for_shm = fs_utils::JoinPath(root_path_for_ipc, kShmSubPath);
    root_path_for_fifo = fs_utils::JoinPath(root_path_for_ipc, kFifoSubPath);
    if (create) {
        if (fs_utils::IsDirectory(root_path_for_ipc)) {
            PCHECK(fs_utils::RemoveDirectoryRecursively(root_path_for_ipc));
        } else if (fs_utils::Exists(root_path_for_ipc)) {
            PCHECK(fs_utils::Remove(root_path_for_ipc));
        }
        PCHECK(fs_utils::MakeDirectory(root_path_for_ipc));
        PCHECK(fs_utils::MakeDirectory(root_path_for_shm));
        PCHECK(fs_utils::MakeDirectory(root_path_for_fifo));
    } else {
        CHECK(fs_utils::IsDirectory(root_path_for_ipc)) << root_path_for_ipc << " does not exist";
        CHECK(fs_utils::IsDirectory(root_path_for_shm)) << root_path_for_shm << " does not exist";
        CHECK(fs_utils::IsDirectory(root_path_for_fifo)) << root_path_for_fifo << " does not exist";
    }
}

std::string_view GetRootPathForIpc() {
    CHECK_NE(root_path_for_ipc, "NOT_SET");
    return root_path_for_ipc;
}

std::string_view GetEngineUnixSocketPath() {
    CHECK_NE(engine_unix_ipc_path, "NOT_SET");
    return engine_unix_ipc_path;
}

std::string_view GetRootPathForShm() {
    CHECK_NE(root_path_for_shm, "NOT_SET");
    return root_path_for_shm;
}

std::string_view GetRootPathForFifo() {
    CHECK_NE(root_path_for_fifo, "NOT_SET");
    return root_path_for_fifo;
}

std::string GetFuncWorkerInputFifoName(uint16_t client_id) {
    return fmt::format("worker_{}_input", client_id);
}

std::string GetFuncWorkerOutputFifoName(uint16_t client_id) {
    return fmt::format("worker_{}_output", client_id);
}

std::string GetFuncCallInputShmName(uint64_t full_call_id) {
    return fmt::format("{}.i", full_call_id);
}

std::string GetFuncCallOutputShmName(uint64_t full_call_id) {
    return fmt::format("{}.o", full_call_id);
}

std::string GetFuncCallOutputFifoName(uint64_t full_call_id) {
    return fmt::format("{}.o", full_call_id);
}

std::string GetViewShmPath(uint16_t view_id) {
    return fs_utils::JoinPath(GetRootPathForShm(),
                              fmt::format("view_{}", view_id));
}

std::string GetLogSpaceHashMetaPath(std::string_view view_shm_path) {
    return fs_utils::JoinPath(view_shm_path, "log_space_hash_meta");
}

std::string GetOrCreateCacheShmPath() {
    DCHECK(fs_utils::Exists(GetRootPathForShm()));
    std::string cacheshm_path(
        fs_utils::JoinPath(GetRootPathForShm(), "cache"));
    if (!fs_utils::Exists(cacheshm_path)) {
        PCHECK(fs_utils::MakeDirectory(cacheshm_path));
    }
    return cacheshm_path;
}

bool CheckIndexMetaPath(uint32_t logspace_id) {
    uint16_t view_id = bits::HighHalf32(logspace_id);
    std::string viewshm_path(ipc::GetViewShmPath(view_id));
    // should have been created when installing the view by engine
    if (!fs_utils::Exists(viewshm_path)) {
        return false;
    }
    std::string indexshm_path(
        fs_utils::JoinPath(viewshm_path, fmt::format("index_{}", logspace_id)));
    return fs_utils::Exists(indexshm_path);
}

std::string GetOrCreateIndexMetaPath(uint32_t logspace_id) {
    uint16_t view_id = bits::HighHalf32(logspace_id);
    std::string viewshm_path(ipc::GetViewShmPath(view_id));
    // should have been created when installing the view by engine
    DCHECK(fs_utils::Exists(viewshm_path)) << viewshm_path;
    std::string indexshm_path(
        fs_utils::JoinPath(viewshm_path, fmt::format("index_{}", logspace_id)));
#if defined(__COMPILE_AS_SHARED)
    DCHECK(fs_utils::Exists(indexshm_path)) << indexshm_path;
#else
    if (!fs_utils::Exists(indexshm_path)) {
        PCHECK(fs_utils::MakeDirectory(indexshm_path));
    }
#endif
    return indexshm_path;
}

std::string GetIndexSegmentPath(std::string_view obj_name,
                                uint32_t user_logspace, uint32_t logspace_id) {
    return fs_utils::JoinPath(
        GetOrCreateIndexMetaPath(logspace_id),
        fmt::format("{}_{}_{}", obj_name, user_logspace, logspace_id));
}

std::string GetIndexSegmentObjectName(std::string_view obj_name,
                                      uint32_t user_logspace,
                                      uint32_t logspace_id) {
    return fmt::format("{}_{}_{}", obj_name, user_logspace, logspace_id);
}

std::string GetCacheShmFile(uint32_t user_logspace) {
    return fs_utils::JoinPath(GetOrCreateCacheShmPath(),
                              fmt::format("user_{}", user_logspace));
}

std::string GetIndexMutexName(uint32_t logspace_id) {
    return fs_utils::JoinPath(GetOrCreateIndexMetaPath(logspace_id),
                              fmt::format("index_mu_{}", logspace_id));
    // return fmt::format("index_mu_{}", logspace_id);
}

std::string GetCacheMutexName(uint32_t user_logspace) {
    return fs_utils::JoinPath(
        GetOrCreateCacheShmPath(),
        fmt::format("SharedLRUCache_mu_{}", user_logspace));
    // return fmt::format("SharedLRUCache_mu_{}", user_logspace);
}

}  // namespace ipc
}  // namespace faas
