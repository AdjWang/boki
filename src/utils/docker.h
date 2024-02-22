#pragma once

#ifndef __FAAS_SRC
#error utils/docker.h cannot be included outside
#endif

#include "base/common.h"

namespace faas {
namespace docker_utils {

// cgroup_fs root by default is /sys/fs/cgroup
void SetCgroupFsRoot(std::string_view path);

constexpr size_t kContainerIdLength = 64;
extern const std::string kInvalidContainerId;

// Get container ID of the running process
// Will return kInvalidContainerId if failed
std::string GetSelfContainerId();

struct ContainerStat {
    int64_t timestamp;      // in ns
    int64_t cpu_usage;      // in ns, from cpuacct.usage
    int32_t cpu_stat_user;  // in tick, from cpuacct.stat
    int32_t cpu_stat_sys;   // in tick, from cpuacct.stat
};

struct ProcStat {
    // 10 metrics from /proc/stat
    int32_t cpu_user;
    int32_t cpu_nice;
    int32_t cpu_system;
    int32_t cpu_idle;
    int32_t cpu_iowait;
    int32_t cpu_irq;
    int32_t cpu_softirq;
    int32_t cpu_steal;
    int32_t cpu_guest;
    int32_t cpu_guest_nice;
};

bool ReadContainerStat(std::string_view container_id, ContainerStat* stat);
bool ReadProcStat(ProcStat* stat);

}  // namespace docker_utils
}  // namespace faas
