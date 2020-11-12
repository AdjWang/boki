#pragma once

#include "base/common.h"
#include "common/time.h"

namespace faas {
namespace protocol {

constexpr int kFuncIdBits   = 8;
constexpr int kMethodIdBits = 6;
constexpr int kClientIdBits = 14;

constexpr int kMaxFuncId   = (1 << kFuncIdBits) - 1;
constexpr int kMaxMethodId = (1 << kMethodIdBits) - 1;
constexpr int kMaxClientId = (1 << kClientIdBits) - 1;

union FuncCall {
    struct {
        uint16_t func_id   : 8;
        uint16_t method_id : 6;
        uint16_t client_id : 14;
        uint32_t call_id   : 32;
        uint16_t padding   : 4;
    } __attribute__ ((packed));
    uint64_t full_call_id;
};
static_assert(sizeof(FuncCall) == 8, "Unexpected FuncCall size");

constexpr FuncCall kInvalidFuncCall = { .full_call_id = 0 };

#define NEW_EMPTY_FUNC_CALL(FC_VAR) \
    FuncCall FC_VAR; FC_VAR.full_call_id = 0

class FuncCallHelper {
public:
    static FuncCall New(uint16_t func_id, uint16_t client_id, uint32_t call_id) {
        NEW_EMPTY_FUNC_CALL(func_call);
        func_call.func_id = func_id;
        func_call.client_id = client_id;
        func_call.call_id = call_id;
        return func_call;
    }

    static FuncCall NewWithMethod(uint16_t func_id, uint16_t method_id,
                                  uint16_t client_id, uint32_t call_id) {
        NEW_EMPTY_FUNC_CALL(func_call);
        func_call.func_id = func_id;
        func_call.method_id = method_id;
        func_call.client_id = client_id;
        func_call.call_id = call_id;
        return func_call;
    }

    static std::string DebugString(const FuncCall& func_call) {
        if (func_call.method_id == 0) {
            return fmt::format("func_id={}, client_id={}, call_id={}",
                            func_call.func_id, func_call.client_id, func_call.call_id);
        } else {
            return fmt::format("func_id={}, method_id={}, client_id={}, call_id={}",
                            func_call.func_id, func_call.method_id,
                            func_call.client_id, func_call.call_id);
        }
    }

private:
    DISALLOW_IMPLICIT_CONSTRUCTORS(FuncCallHelper);
};

#undef NEW_EMPTY_FUNC_CALL

enum class MessageType : uint16_t {
    INVALID               = 0,
    ENGINE_HANDSHAKE      = 1,
    LAUNCHER_HANDSHAKE    = 2,
    FUNC_WORKER_HANDSHAKE = 3,
    HANDSHAKE_RESPONSE    = 4,
    CREATE_FUNC_WORKER    = 5,
    INVOKE_FUNC           = 6,
    DISPATCH_FUNC_CALL    = 7,
    FUNC_CALL_COMPLETE    = 8,
    FUNC_CALL_FAILED      = 9,
    SHARED_LOG_OP         = 10
};

enum class SharedLogOpType : uint16_t {
    APPEND     = 0,
    REPLICATED = 1,
    DISCARDED  = 2,
    READ_AT    = 3,
    READ_NEXT  = 4,
    TRIM       = 5
};

constexpr uint32_t kDefaultLogTag     = std::numeric_limits<uint32_t>::max();
constexpr uint64_t kInvalidLogLocalId = std::numeric_limits<uint64_t>::max();
constexpr uint64_t kInvalidLogSeqNum  = std::numeric_limits<uint64_t>::max();

constexpr uint32_t kFuncWorkerUseEngineSocketFlag = 1;
constexpr uint32_t kUseFifoForNestedCallFlag = 2;

struct Message {
    struct {
        uint16_t message_type : 4;
        uint16_t func_id      : 8;
        uint16_t method_id    : 6;
        uint16_t client_id    : 14;
        uint32_t call_id;
    } __attribute__ ((packed));
    union {
        uint64_t parent_call_id;  // Used in INVOKE_FUNC, saved as full_call_id
        struct {
            int32_t dispatch_delay;   // Used in FUNC_CALL_COMPLETE, FUNC_CALL_FAILED
            int32_t processing_time;  // Used in FUNC_CALL_COMPLETE
        } __attribute__ ((packed));
        uint64_t log_seqnum;  // Used in SHARED_LOG_OP
    };
    int64_t send_timestamp;
    int32_t payload_size;  // Used in HANDSHAKE_RESPONSE, INVOKE_FUNC, FUNC_CALL_COMPLETE, SHARED_LOG_OP
    uint32_t flags;

    struct {
        uint16_t log_op;
        uint16_t padding1;
    } __attribute__ ((packed));

    uint32_t log_tag;
    uint64_t log_localid;

    char padding2[__FAAS_CACHE_LINE_SIZE - 48];
    char inline_data[__FAAS_MESSAGE_SIZE - __FAAS_CACHE_LINE_SIZE]
        __attribute__ ((aligned (__FAAS_CACHE_LINE_SIZE)));
};

#define MESSAGE_INLINE_DATA_SIZE (__FAAS_MESSAGE_SIZE - __FAAS_CACHE_LINE_SIZE)
static_assert(sizeof(Message) == __FAAS_MESSAGE_SIZE, "Unexpected Message size");

struct GatewayMessage {
    struct {
        uint16_t message_type : 4;
        uint16_t func_id      : 8;
        uint16_t method_id    : 6;
        uint16_t client_id    : 14;
        uint32_t call_id;
    } __attribute__ ((packed));
    union {
        // Used in ENGINE_HANDSHAKE
        struct {
            uint16_t node_id;
            uint16_t conn_id;
            char     shared_log_addr[32];
        } __attribute__ ((packed));
        int32_t processing_time; // Used in FUNC_CALL_COMPLETE
        int32_t status_code;     // Used in FUNC_CALL_FAILED
    };
    int32_t payload_size;        // Used in INVOKE_FUNC, FUNC_CALL_COMPLETE, SHARED_LOG_OP
} __attribute__ ((packed));

static_assert(sizeof(GatewayMessage) == 48, "Unexpected GatewayMessage size");

class MessageHelper {
public:
    static bool IsLauncherHandshake(const Message& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::LAUNCHER_HANDSHAKE;
    }

    static bool IsFuncWorkerHandshake(const Message& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::FUNC_WORKER_HANDSHAKE;
    }

    static bool IsHandshakeResponse(const Message& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::HANDSHAKE_RESPONSE;
    }

    static bool IsCreateFuncWorker(const Message& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::CREATE_FUNC_WORKER;
    }

    static bool IsInvokeFunc(const Message& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::INVOKE_FUNC;
    }

    static bool IsDispatchFuncCall(const Message& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::DISPATCH_FUNC_CALL;
    }

    static bool IsFuncCallComplete(const Message& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::FUNC_CALL_COMPLETE;
    }

    static bool IsFuncCallFailed(const Message& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::FUNC_CALL_FAILED;
    }

    static bool IsSharedLogOp(const Message& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::SHARED_LOG_OP;
    }

    static void SetFuncCall(Message* message, const FuncCall& func_call) {
        message->func_id = func_call.func_id;
        message->method_id = func_call.method_id;
        message->client_id = func_call.client_id;
        message->call_id = func_call.call_id;
    }

    static FuncCall GetFuncCall(const Message& message) {
        DCHECK(IsInvokeFunc(message) || IsDispatchFuncCall(message)
                || IsFuncCallComplete(message) || IsFuncCallFailed(message));
        FuncCall func_call;
        func_call.func_id = message.func_id;
        func_call.method_id = message.method_id;
        func_call.client_id = message.client_id;
        func_call.call_id = message.call_id;
        func_call.padding = 0;
        return func_call;
    }

    static void SetInlineData(Message* message, std::span<const char> data) {
        message->payload_size = gsl::narrow_cast<int32_t>(data.size());
        DCHECK(data.size() <= MESSAGE_INLINE_DATA_SIZE);
        if (data.size() > 0) {
            memcpy(message->inline_data, data.data(), data.size());
        }
    }

    static std::span<const char> GetInlineData(const Message& message) {
        if (IsInvokeFunc(message) || IsDispatchFuncCall(message)
              || IsFuncCallComplete(message) || IsLauncherHandshake(message)
              || IsSharedLogOp(message)) {
            if (message.payload_size > 0) {
                return std::span<const char>(
                    message.inline_data, gsl::narrow_cast<size_t>(message.payload_size));
            }
        }
        return std::span<const char>();
    }

    static SharedLogOpType GetSharedLogOpType(const Message& message) {
        return static_cast<SharedLogOpType>(message.log_op);
    }

    static int32_t ComputeMessageDelay(const Message& message) {
        if (message.send_timestamp > 0) {
            return gsl::narrow_cast<int32_t>(GetMonotonicMicroTimestamp() - message.send_timestamp);
        } else {
            return -1;
        }
    }

#define NEW_EMPTY_MESSAGE(MSG_VAR) \
    Message MSG_VAR; memset(&MSG_VAR, 0, sizeof(Message))

    static Message NewLauncherHandshake(uint16_t func_id) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::LAUNCHER_HANDSHAKE);
        message.func_id = func_id;
        return message;
    }

    static Message NewFuncWorkerHandshake(uint16_t func_id, uint16_t client_id) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::FUNC_WORKER_HANDSHAKE);
        message.func_id = func_id;
        message.client_id = client_id;
        return message;
    }

    static Message NewHandshakeResponse(uint32_t payload_size) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::HANDSHAKE_RESPONSE);
        message.payload_size = payload_size;
        return message;
    }

    static Message NewCreateFuncWorker(uint16_t client_id) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::CREATE_FUNC_WORKER);
        message.client_id = client_id;
        return message;
    }

    static Message NewInvokeFunc(const FuncCall& func_call, uint64_t parent_call_id) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::INVOKE_FUNC);
        SetFuncCall(&message, func_call);
        message.parent_call_id = parent_call_id;
        return message;
    }

    static Message NewDispatchFuncCall(const FuncCall& func_call) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::DISPATCH_FUNC_CALL);
        SetFuncCall(&message, func_call);
        return message;
    }

    static Message NewFuncCallComplete(const FuncCall& func_call, int32_t processing_time) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_COMPLETE);
        SetFuncCall(&message, func_call);
        message.processing_time = processing_time;
        return message;
    }

    static Message NewFuncCallFailed(const FuncCall& func_call) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_FAILED);
        SetFuncCall(&message, func_call);
        return message;
    }

    static Message NewSharedLogAppend(uint32_t log_tag,
                                      uint64_t log_localid = kInvalidLogLocalId) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::SHARED_LOG_OP);
        message.log_op = static_cast<uint16_t>(SharedLogOpType::APPEND);
        message.log_tag = log_tag;
        message.log_localid = log_localid;
        return message;
    }

    static Message NewSharedLogReadAt(uint64_t log_seqnum) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::SHARED_LOG_OP);
        message.log_op = static_cast<uint16_t>(SharedLogOpType::READ_AT);
        message.log_seqnum = log_seqnum;
        return message;
    }

#undef NEW_EMPTY_MESSAGE

private:
    DISALLOW_IMPLICIT_CONSTRUCTORS(MessageHelper);
};

class GatewayMessageHelper {
public:
    static bool IsEngineHandshake(const GatewayMessage& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::ENGINE_HANDSHAKE;
    }

    static bool IsDispatchFuncCall(const GatewayMessage& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::DISPATCH_FUNC_CALL;
    }

    static bool IsFuncCallComplete(const GatewayMessage& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::FUNC_CALL_COMPLETE;
    }

    static bool IsFuncCallFailed(const GatewayMessage& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::FUNC_CALL_FAILED;
    }

    static bool IsSharedLogOp(const GatewayMessage& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::SHARED_LOG_OP;
    }

    static void SetFuncCall(GatewayMessage* message, const FuncCall& func_call) {
        message->func_id = func_call.func_id;
        message->method_id = func_call.method_id;
        message->client_id = func_call.client_id;
        message->call_id = func_call.call_id;
    }

    static FuncCall GetFuncCall(const GatewayMessage& message) {
        DCHECK(IsDispatchFuncCall(message) || IsFuncCallComplete(message)
                  || IsFuncCallFailed(message));
        FuncCall func_call;
        func_call.func_id = message.func_id;
        func_call.method_id = message.method_id;
        func_call.client_id = message.client_id;
        func_call.call_id = message.call_id;
        func_call.padding = 0;
        return func_call;
    }

#define NEW_EMPTY_GATEWAY_MESSAGE(MSG_VAR) \
    GatewayMessage MSG_VAR; memset(&MSG_VAR, 0, sizeof(GatewayMessage))

    static GatewayMessage NewEngineHandshake(uint16_t node_id, uint16_t conn_id) {
        NEW_EMPTY_GATEWAY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::ENGINE_HANDSHAKE);
        message.node_id = node_id;
        message.conn_id = conn_id;
        return message;
    }

    static GatewayMessage NewDispatchFuncCall(const FuncCall& func_call) {
        NEW_EMPTY_GATEWAY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::DISPATCH_FUNC_CALL);
        SetFuncCall(&message, func_call);
        return message;
    }

    static GatewayMessage NewFuncCallComplete(const FuncCall& func_call, int32_t processing_time) {
        NEW_EMPTY_GATEWAY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_COMPLETE);
        SetFuncCall(&message, func_call);
        message.processing_time = processing_time;
        return message;
    }

    static GatewayMessage NewFuncCallFailed(const FuncCall& func_call, int32_t status_code = 0) {
        NEW_EMPTY_GATEWAY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_FAILED);
        SetFuncCall(&message, func_call);
        message.status_code = status_code;
        return message;
    }

    static GatewayMessage NewSharedLogOp() {
        NEW_EMPTY_GATEWAY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::SHARED_LOG_OP);
        return message;
    }

#undef NEW_EMPTY_GATEWAY_MESSAGE

private:
    DISALLOW_IMPLICIT_CONSTRUCTORS(GatewayMessageHelper);
};

}  // namespace protocol
}  // namespace faas
