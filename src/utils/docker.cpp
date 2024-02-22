#include "utils/docker.h"

#include "common/time.h"
#include "utils/fs.h"

namespace faas {
namespace docker_utils {

namespace {
std::string cgroupfs_root = "/sys/fs/cgroup";

template<class T>
bool ReadIntegerFromFile(std::string_view path, T* value) {
    std::string contents;
    if (!fs_utils::ReadContents(path, &contents)) {
        return false;
    }
    return absl::SimpleAtoi(contents, value);
}

bool ReadCpuAcctStat(std::string_view container_id, int32_t* user, int32_t* system) {
    std::string full_path(fmt::format(
        "{}/cpuacct/docker/{}/cpuacct.stat", cgroupfs_root, container_id));
    std::string contents;
    if (!fs_utils::ReadContents(full_path, &contents)) {
        return false;
    }
    for (const auto& line : absl::StrSplit(contents, '\n', absl::SkipWhitespace())) {
        if (absl::StartsWith(line, "user ")) {
            if (!absl::SimpleAtoi(absl::StripPrefix(line, "user "), user)) {
                return false;
            }
        }
        if (absl::StartsWith(line, "system ")) {
            if (!absl::SimpleAtoi(absl::StripPrefix(line, "system "), system)) {
                return false;
            }
        }
    }
    return true;
}

// /proc/stat
bool ReadCpuProcStat(int32_t* user,
                     int32_t* nice,
                     int32_t* system,
                     int32_t* idle,
                     int32_t* iowait,
                     int32_t* irq,
                     int32_t* softirq,
                     int32_t* steal,
                     int32_t* guest,
                     int32_t* guest_nice) {
    std::string full_path("/proc/stat");
    std::string contents;
    if (!fs_utils::ReadContents(full_path, &contents)) {
        return false;
    }
    for (const auto& line : absl::StrSplit(contents, '\n', absl::SkipWhitespace())) {
        if (absl::StartsWith(line, "cpu ")) {
            std::vector<std::string> items = absl::StrSplit(line, ' ');
            // if (!absl::SimpleAtoi(absl::StripPrefix(line, "user "), user)) {
            //     return false;
            // }
            DCHECK_EQ(items.size(), 12u);
            if (!absl::SimpleAtoi(items[2], user)) {
                return false;
            }
            if (!absl::SimpleAtoi(items[3], nice)) {
                return false;
            }
            if (!absl::SimpleAtoi(items[4], system)) {
                return false;
            }
            if (!absl::SimpleAtoi(items[5], idle)) {
                return false;
            }
            if (!absl::SimpleAtoi(items[6], iowait)) {
                return false;
            }
            if (!absl::SimpleAtoi(items[7], irq)) {
                return false;
            }
            if (!absl::SimpleAtoi(items[8], softirq)) {
                return false;
            }
            if (!absl::SimpleAtoi(items[9], steal)) {
                return false;
            }
            if (!absl::SimpleAtoi(items[10], guest)) {
                return false;
            }
            if (!absl::SimpleAtoi(items[11], guest_nice)) {
                return false;
            }
        }
    }
    return true;
}
}

void SetCgroupFsRoot(std::string_view path) {
    cgroupfs_root = std::string(path);
}

const std::string kInvalidContainerId(kContainerIdLength, '0');

std::string GetSelfContainerId() {
    std::string contents;
    if (!fs_utils::ReadContents("/proc/self/cgroup", &contents)) {
        LOG(ERROR) << "Failed to read /proc/self/cgroup";
        return kInvalidContainerId;
    }
    size_t pos = contents.find("/docker/");
    if (pos == std::string::npos
          || pos + strlen("/docker/") + kContainerIdLength >= contents.length()) {
        LOG(ERROR) << "Cannot find docker's cgroup in /proc/self/cgroup";
        return kInvalidContainerId;
    }
    return contents.substr(pos + strlen("/docker/"), kContainerIdLength);
}

bool ReadContainerStat(std::string_view container_id, ContainerStat* stat) {
    stat->timestamp = GetMonotonicNanoTimestamp();
    if (!ReadIntegerFromFile(fmt::format("{}/cpuacct/docker/{}/cpuacct.usage",
                                         cgroupfs_root, container_id),
                             &stat->cpu_usage)) {
        return false;
    }
    if (!ReadCpuAcctStat(container_id, &stat->cpu_stat_user, &stat->cpu_stat_sys)) {
        return false;
    }
    return true;
}

bool ReadProcStat(ProcStat* stat) {
    if (!ReadCpuProcStat(&stat->cpu_user,
                         &stat->cpu_nice,
                         &stat->cpu_system,
                         &stat->cpu_idle,
                         &stat->cpu_iowait,
                         &stat->cpu_irq,
                         &stat->cpu_softirq,
                         &stat->cpu_steal,
                         &stat->cpu_guest,
                         &stat->cpu_guest_nice)) {
        return false;
    }
    return true;
}

}  // namespace docker_utils
}  // namespace faas
