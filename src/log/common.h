#pragma once

#include "base/common.h"
#include "common/protocol.h"

namespace faas {
namespace log {

inline uint16_t GetViewId(uint64_t value) {
    return bits::HighHalf32(bits::HighHalf64(value));
}

constexpr uint64_t kEmptyLogTag      = 0;
constexpr uint64_t kMaxLogSeqNum     = 0xffff000000000000ULL;
constexpr uint64_t kInvalidLogSeqNum = protocol::kInvalidLogSeqNum;
constexpr uint64_t kInvalidLogTag    = protocol::kInvalidLogTag;

struct SharedLogRequest {
    protocol::SharedLogMessage message;
    std::string                payload;
    void*                      local_op;

    explicit SharedLogRequest(void* local_op)
        : local_op(local_op) {}

    explicit SharedLogRequest(const protocol::SharedLogMessage& message,
                              std::span<const char> payload = EMPTY_CHAR_SPAN)
        : message(message),
          payload(),
          local_op(nullptr) {
        if (payload.size() > 0) {
            this->payload.assign(payload.data(), payload.size());
        }
    }
};

using UserTagVec = absl::InlinedVector<uint64_t, 4>;

struct LogMetaData {
    uint32_t user_logspace;
    uint64_t seqnum;
    uint64_t localid;
    size_t   num_tags;
    size_t   data_size;
};

struct LogEntry {
    LogMetaData metadata;
    UserTagVec  user_tags;
    std::string data;
};

}  // namespace log
}  // namespace faas
