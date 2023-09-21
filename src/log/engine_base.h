#pragma once

#include "common/zk.h"
#include "log/common.h"
#include "log/view.h"
#include "log/view_watcher.h"
#include "log/index.h"
#include "log/cache.h"
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
    virtual void OnRecvCheckTailResponse(const protocol::SharedLogMessage& message) = 0;
    virtual void OnRecvResponse(const protocol::SharedLogMessage& message,
                                std::span<const char> payload) = 0;

    void MessageHandler(const protocol::SharedLogMessage& message,
                        std::span<const char> payload);

    struct LocalOp {
        constexpr static uint32_t kCheckTailReadPrev = (1 << 0);

        protocol::SharedLogOpType type;
        uint16_t client_id;
        uint32_t user_logspace;
        uint64_t id;
        uint64_t client_data;
        bool     query_index_only;
        uint64_t metalog_progress;
        uint64_t query_tag;
        union {
            uint64_t localid;
            uint64_t seqnum;
        };
        uint64_t full_call_id;
        int64_t start_timestamp;
        UserTagVec user_tags;
        utils::AppendableBuffer data;
        uint32_t flags;
    };

    virtual void HandleLocalAppend(LocalOp* op) = 0;
    virtual void HandleLocalTrim(LocalOp* op) = 0;
    virtual void HandleLocalRead(LocalOp* op) = 0;
    virtual void HandleLocalSetAuxData(LocalOp* op) = 0;
    virtual void HandleLocalCheckTail(LocalOp* op) = 0;

    void LocalOpHandler(LocalOp* op);

    void ReplicateLogEntry(const View* view, const LogMetaData& log_metadata,
                           std::span<const uint64_t> user_tags,
                           std::span<const char> log_data);
    void PropagateAuxData(const View* view, const LogMetaData& log_metadata, 
                          std::span<const char> aux_data);

    void IntermediateLocalOpWithResponse(LocalOp* op, protocol::Message* response, 
                                         uint64_t metalog_progress);
    void FinishLocalOpWithResponse(LocalOp* op, protocol::Message* response,
                                   uint64_t metalog_progress);
    void FinishLocalOpWithFailure(LocalOp* op, protocol::SharedLogResultType result,
                                  uint64_t metalog_progress = 0);

    void LogCachePut(const LogMetaData& log_metadata, std::span<const uint64_t> user_tags,
                     std::span<const char> log_data);
    std::optional<LogEntry> LogCacheGet(uint64_t seqnum);
    void LogCachePutAuxData(uint64_t seqnum, std::span<const char> data);
    std::optional<std::string> LogCacheGetAuxData(uint64_t seqnum);

    bool SendIndexReadRequest(const View::Sequencer* sequencer_node,
                              protocol::SharedLogMessage* request);
    bool SendStorageReadRequest(const IndexQueryResult& result,
                                const View::Engine* engine_node);
    void SendReadResponse(const IndexQuery& query,
                          protocol::SharedLogMessage* response,
                          std::span<const char> user_tags_payload = EMPTY_CHAR_SPAN,
                          std::span<const char> data_payload = EMPTY_CHAR_SPAN,
                          std::span<const char> aux_data_payload = EMPTY_CHAR_SPAN);
    void SendReadFailureResponse(const IndexQuery& query,
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
    std::string DebugListExistingFnCall(const absl::flat_hash_map</* full_call_id */ uint64_t, FnCallContext>& fn_call_ctx);

    std::optional<LRUCache> log_cache_;

#ifndef __FAAS_DISABLE_STAT
    absl::Mutex stat_mu_;
    stat::StatisticsCollector<int32_t> append_delay_stat_ ABSL_GUARDED_BY(stat_mu_);
    stat::StatisticsCollector<int32_t> check_tail_delay_ ABSL_GUARDED_BY(stat_mu_);
#endif

    void SetupZKWatchers();
    void SetupTimers();

    void PopulateLogTagsAndData(const protocol::Message& message, LocalOp* op);

    void SetLogReadRespTypeFlag(LocalOp* op, protocol::Message* response);
    void RecordOpDelay(const LocalOp* op);

    DISALLOW_COPY_AND_ASSIGN(EngineBase);
};

}  // namespace log
}  // namespace faas
