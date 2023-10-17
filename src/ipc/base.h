#pragma once

#include "base/common.h"

namespace faas {
namespace ipc {

void SetRootPathForIpc(std::string_view path, bool create = false);

std::string_view GetRootPathForIpc();
std::string_view GetEngineUnixSocketPath();
std::string_view GetRootPathForShm();
std::string_view GetRootPathForFifo();

std::string GetFuncWorkerInputFifoName(uint16_t client_id);
std::string GetFuncWorkerOutputFifoName(uint16_t client_id);

std::string GetFuncCallInputShmName(uint64_t full_call_id);
std::string GetFuncCallOutputShmName(uint64_t full_call_id);
std::string GetFuncCallOutputFifoName(uint64_t full_call_id);

std::string GetViewShmPath(uint16_t view_id);
std::string GetLogSpaceHashMetaPath(std::string_view view_shm_path);
std::string GetOrCreateCacheShmPath();
std::string GetIndexSegmentName(uint32_t user_logspace, uint32_t logspace_id);
bool CheckIndexMeta(uint32_t user_logspace, uint32_t logspace_id);
std::string GetOrCreateIndexMetaPath(uint32_t logspace_id);

std::string GetIndexSegmentPath(std::string_view obj_name,
                                uint32_t user_logspace, uint32_t logspace_id);
std::string GetIndexSegmentObjectName(std::string_view obj_name,
                                      uint32_t user_logspace,
                                      uint32_t logspace_id);

std::string GetCacheShmFile(uint32_t user_logspace);

std::string GetIndexMutexName(uint32_t logspace_id);
std::string GetCacheMutexName(uint32_t user_logspace);

}  // namespace ipc
}  // namespace faas
