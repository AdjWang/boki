#pragma once

#include "common/zk.h"
#include "log/common.h"
#include "log/view.h"
#include "log/view_watcher.h"
#include "log/index.h"
#include "log/cache.h"
#include "log/utils.h"
#include "server/io_worker.h"
#include "utils/object_pool.h"
#include "utils/appendable_buffer.h"

namespace faas {

// Forward declaration
namespace engine { class Engine; }

namespace log {

class EngineBase {
public:
    explicit EngineBase(engine::Engine* engine);
    virtual ~EngineBase();

    void Start();
    void Stop();

    void OnRecvSharedLogMessage(int conn_type, uint16_t src_node_id,
                                const protocol::SharedLogMessage& message,
                                std::span<const char> payload);

    void OnNewExternalFuncCall(const protocol::FuncCall& func_call, uint32_t log_space);
    void OnNewInternalFuncCall(const protocol::FuncCall& func_call,
                               const protocol::FuncCall& parent_func_call);
    void OnFuncCallCompleted(const protocol::FuncCall& func_call);
    void OnMessageFromFuncWorker(const protocol::Message& message);

protected:
    uint16_t my_node_id() const { return node_id_; }
    zk::ZKSession* zk_session();

    virtual void OnViewCreated(const View* view) = 0;
    virtual void OnViewFrozen(const View* view) = 0;
    virtual void OnViewFinalized(const FinalizedView* finalized_view) = 0;

    virtual void HandleRemoteRead(const protocol::SharedLogMessage& request) = 0;
    virtual void OnRecvNewMetaLogs(const protocol::SharedLogMessage& message,
                                   std::span<const char> payload) = 0;
    virtual void OnRecvNewIndexData(const protocol::SharedLogMessage& message,
                                    std::span<const char> payload) = 0;
    virtual void OnRecvResponse(const protocol::SharedLogMessage& message,
                                std::span<const char> payload) = 0;

    void MessageHandler(const protocol::SharedLogMessage& message,
                        std::span<const char> payload);

    struct LocalOp {
        protocol::SharedLogOpType type;
        uint16_t client_id;
        uint32_t user_logspace;
        uint64_t id;
        uint64_t client_data;
        uint64_t metalog_progress;
        uint64_t query_tag;
        uint64_t query_start_seqnum;
        union {
            uint64_t localid;
            uint64_t seqnum;
        };
        uint64_t func_call_id;
        int64_t start_timestamp;
        UserTagVec user_tags;
        utils::AppendableBuffer data;
        uint16_t flags;
        // Used by READ_SYNCTO to reorder results
        log_utils::ThreadSafeCounter response_counter;

        static constexpr uint16_t kReadLocalIdFlag    = (1 << 1);     // Indicates localid/seqnum
        static constexpr uint16_t kReadFromCachedFlag = (1 << 2);     // Indicates whether skip previous cached logs
        std::string InspectRead() const {
            if ((flags & kReadLocalIdFlag) != 0) {
                return fmt::format("op_id={} localid={:016X}", id, localid);
            } else {
                return fmt::format("op_id={} seqnum={:016X}", id, seqnum);
            }
        }
    };

    virtual void HandleLocalAppend(LocalOp* op) = 0;
    virtual void HandleLocalTrim(LocalOp* op) = 0;
    virtual void HandleLocalRead(LocalOp* op) = 0;
    virtual void HandleLocalSetAuxData(LocalOp* op) = 0;

    void LocalOpHandler(LocalOp* op);

    void ReplicateLogEntry(const View* view, const LogMetaData& log_metadata,
                           std::span<const uint64_t> user_tags,
                           std::span<const char> log_data);
    void PropagateAuxData(const View* view, const LogMetaData& log_metadata, 
                          std::span<const char> aux_data);

    void SendLocalOpWithResponse(LocalOp* op, protocol::Message* response,
                                 uint64_t metalog_progress,
                                 std::function<void()> on_finished=nullptr);

    bool SendIndexReadRequest(const View::Sequencer* sequencer_node,
                              protocol::SharedLogMessage* request);
    bool SendStorageReadRequest(const IndexQueryResult& result,
                                const View::Engine* engine_node);
    void SendReadResponse(const IndexQuery& query,
                          protocol::SharedLogMessage* response,
                          std::span<const char> user_tags_payload = EMPTY_CHAR_SPAN,
                          std::span<const char> data_payload = EMPTY_CHAR_SPAN,
                          std::span<const char> aux_data_payload = EMPTY_CHAR_SPAN);
    void SendReadResponseWithoutData(const IndexQuery& query,
                                     protocol::SharedLogResultType result_type,
                                     uint64_t metalog_progress = 0);
    bool SendSequencerMessage(uint16_t sequencer_id,
                              protocol::SharedLogMessage* message,
                              std::span<const char> payload = EMPTY_CHAR_SPAN);

    server::IOWorker* SomeIOWorker();

private:
    const uint16_t node_id_;
    engine::Engine* engine_;

    ViewWatcher view_watcher_;

    utils::ThreadSafeObjectPool<LocalOp> log_op_pool_;
    std::atomic<uint64_t> next_local_op_id_;

    struct FnCallContext {
        uint32_t user_logspace;
        uint64_t metalog_progress;
        uint64_t parent_call_id;
    };

    absl::Mutex fn_ctx_mu_;
    absl::flat_hash_map</* full_call_id */ uint64_t, FnCallContext>
        fn_call_ctx_ ABSL_GUARDED_BY(fn_ctx_mu_);
    std::string DebugListExistingFnCall(
        const absl::flat_hash_map</* full_call_id */ uint64_t, FnCallContext>&
            fn_call_ctx);

    void SetupZKWatchers();
    void SetupTimers();

    void PopulateLogTagsAndData(const protocol::Message& message, LocalOp* op);

    DISALLOW_COPY_AND_ASSIGN(EngineBase);
};

}  // namespace log
}  // namespace faas

template <>
struct fmt::formatter<faas::protocol::SharedLogOpType>: formatter<std::string_view> {
    auto format(faas::protocol::SharedLogOpType dir, format_context& ctx) const {
        std::string_view result = "unknown";
        switch (dir) {
            case faas::protocol::SharedLogOpType::INVALID:
                result = "INVALID";
                break;
            case faas::protocol::SharedLogOpType::APPEND:
                result = "APPEND";
                break;
            case faas::protocol::SharedLogOpType::READ_NEXT:
                result = "READ_NEXT";
                break;
            case faas::protocol::SharedLogOpType::READ_PREV:
                result = "READ_PREV";
                break;
            case faas::protocol::SharedLogOpType::TRIM:
                result = "TRIM";
                break;
            case faas::protocol::SharedLogOpType::SET_AUXDATA:
                result = "SET_AUXDATA";
                break;
            case faas::protocol::SharedLogOpType::READ_NEXT_B:
                result = "READ_NEXT_B";
                break;
            case faas::protocol::SharedLogOpType::READ_SYNCTO:
                result = "READ_SYNCTO";
                break;
            case faas::protocol::SharedLogOpType::READ_AT:
                result = "READ_AT";
                break;
            case faas::protocol::SharedLogOpType::REPLICATE:
                result = "REPLICATE";
                break;
            case faas::protocol::SharedLogOpType::INDEX_DATA:
                result = "INDEX_DATA";
                break;
            case faas::protocol::SharedLogOpType::SHARD_PROG:
                result = "SHARD_PROG";
                break;
            case faas::protocol::SharedLogOpType::METALOGS:
                result = "METALOGS";
                break;
            case faas::protocol::SharedLogOpType::META_PROG:
                result = "META_PROG";
                break;
            case faas::protocol::SharedLogOpType::ASYNC_APPEND:
                result = "ASYNC_APPEND";
                break;
            case faas::protocol::SharedLogOpType::ASYNC_READ_NEXT:
                result = "ASYNC_READ_NEXT";
                break;
            case faas::protocol::SharedLogOpType::ASYNC_READ_NEXT_B:
                result = "ASYNC_READ_NEXT_B";
                break;
            case faas::protocol::SharedLogOpType::ASYNC_READ_PREV:
                result = "ASYNC_READ_PREV";
                break;
            case faas::protocol::SharedLogOpType::ASYNC_READ_PREV_AUX:
                result = "ASYNC_READ_PREV_AUX";
                break;
            case faas::protocol::SharedLogOpType::ASYNC_READ_LOCALID:
                result = "ASYNC_READ_LOCALID";
                break;
            case faas::protocol::SharedLogOpType::RESPONSE:
                result = "RESPONSE";
                break;
        }
        return formatter<string_view>::format(result, ctx);
    }
};
