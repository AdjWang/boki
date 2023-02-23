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

#include "common/otel_trace.h"

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

    // bind to engine::Engine::CreateSharedLogIngressConn, creating shared log IngressConnection
    // check types, then invoke MessageHandler(...)
    void OnRecvSharedLogMessage(int conn_type, uint16_t src_node_id,
                                otel::context& ctx,
                                const protocol::SharedLogMessage& message,
                                std::span<const char> payload);

    // bind to engine::Engine::OnExternalFuncCall
    void OnNewExternalFuncCall(const protocol::FuncCall& func_call, uint32_t log_space);

    // bind to engine::Engine::HandleInvokeFuncMessage
    void OnNewInternalFuncCall(const protocol::FuncCall& func_call,
                               const protocol::FuncCall& parent_func_call);
    
    // bind to engine::Engine::HandleInvokeFuncMessage
    // bind to engine::Engine::HandleFuncCallCompleteMessage
    // bind to engine::Engine::HandleFuncCallFailedMessage
    // bind to engine::Engine::OnExternalFuncCall
    void OnFuncCallCompleted(const protocol::FuncCall& func_call);

    // bind to engine::Engine::HandleSharedLogOpMessage
    // must be invoked after the ctx created
    // build LocalOp by message, then invoke LocalOpHandler(...)
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

    // handle SharedLogMessage, not Message; invoked by OnRecvSharedLogMessage(...)
    void MessageHandler(const protocol::SharedLogMessage& message,
                        std::span<const char> payload);

    // combine necessary infos from protocol::Message and ctx
    struct LocalOp {
        protocol::SharedLogOpType type; // from protocol::Message
        uint16_t client_id;             // from protocol::Message
        uint32_t user_logspace;         // from ctx
        uint64_t id;                    // generated
        uint64_t client_data;           // from protocol::Message
        uint64_t metalog_progress;      // from ctx
        uint64_t query_tag;             // initialized as kInvalidLogTag； used by SharedLogOpType::READ_NEXT,READ_PREV,READ_NEXT_B
        uint64_t seqnum;                // initialized as kInvalidLogSeqNum； used by SharedLogOpType::READ_NEXT,READ_PREV,READ_NEXT_B,TRIM,SET_AUXDATA
        uint64_t func_call_id;          // from protocol::Message::func_call
        int64_t start_timestamp;        // generated
        UserTagVec user_tags;           // initialized as empty vec; used by SharedLogOpType::APPEND
        utils::AppendableBuffer data;   // initialized as empty buffer; used by SharedLogOpType::APPEND,SET_AUXDATA

        nostd::shared_ptr<trace::Span> trace_span;  // for distributed tracing
        nostd::unique_ptr<trace::Scope> trace_scope;  // for distributed tracing
    };

    virtual void HandleLocalAppend(LocalOp* op) = 0;
    virtual void HandleLocalTrim(LocalOp* op) = 0;
    virtual void HandleLocalRead(LocalOp* op) = 0;
    virtual void HandleLocalSetAuxData(LocalOp* op) = 0;

    void LocalOpHandler(LocalOp* op);

    // send log data to all storage nodes from this engine node
    // invoked by Engine::HandleLocalAppend(...)
    void ReplicateLogEntry(const View* view, const LogMetaData& log_metadata,
                           std::span<const uint64_t> user_tags,
                           std::span<const char> log_data);
    // invoked by Engine::HandleLocalSetAuxData(...)
    void PropagateAuxData(const View* view, const LogMetaData& log_metadata, 
                          std::span<const char> aux_data);

    void FinishLocalOpWithResponse(LocalOp* op, protocol::Message* response,
                                   uint64_t metalog_progress);
    void FinishLocalOpWithFailure(LocalOp* op, protocol::SharedLogResultType result,
                                  uint64_t metalog_progress = 0);
    
    // invoked by Engine::OnRecvResponse(...)
    // invoked by Engine::ProcessAppendResults(...)
    void LogCachePut(const LogMetaData& log_metadata, std::span<const uint64_t> user_tags,
                     std::span<const char> log_data);
    // invoked by Engine::HandleLocalSetAuxData(...)
    // invoked by Engine::ProcessIndexFoundResult(...)
    std::optional<LogEntry> LogCacheGet(uint64_t seqnum);
    // invoked by Engine::OnRecvResponse(...)
    // invoked by Engine::HandleLocalSetAuxData(...)
    void LogCachePutAuxData(uint64_t seqnum, std::span<const char> data);
    // invoked by Engine::ProcessIndexFoundResult(...)
    // invoked by Engine::HandleLocalSetAuxData(...)
    std::optional<std::string> LogCacheGetAuxData(uint64_t seqnum);

    // invoked by Engine::HandleLocalRead(...)
    // invoked by Engine::ProcessIndexContinueResult(...)
    // send index read request to a remote engine node. pick the proper engine
    // node by the sequencer_node id.
    bool SendIndexReadRequest(const View::Sequencer* sequencer_node,
                              protocol::SharedLogMessage* request);
    // invoked by Engine::ProcessIndexFoundResult(...)
    bool SendStorageReadRequest(const IndexQueryResult& result,
                                const View::Engine* engine_node);
    // invoked by EngineBase::SendReadFailureResponse(...)
    // invoked by Engine::ProcessIndexFoundResult(...)
    void SendReadResponse(const IndexQuery& query,
                          protocol::SharedLogMessage* response,
                          std::span<const char> user_tags_payload = EMPTY_CHAR_SPAN,
                          std::span<const char> data_payload = EMPTY_CHAR_SPAN,
                          std::span<const char> aux_data_payload = EMPTY_CHAR_SPAN);
    // invoked by Engine::ProcessIndexFoundResult(...)
    // invoked by Engine::ProcessIndexQueryResults(...)
    void SendReadFailureResponse(const IndexQuery& query,
                                 protocol::SharedLogResultType result_type,
                                 uint64_t metalog_progress = 0);
    bool SendSequencerMessage(uint16_t sequencer_id,
                              protocol::SharedLogMessage* message,
                              std::span<const char> payload = EMPTY_CHAR_SPAN);

    // invoked by Engine::OnViewCreated(...)
    // invoked by Engine::OnViewFinalized(...)
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

    std::optional<LRUCache> log_cache_;

    void SetupZKWatchers();
    void SetupTimers();

    void PopulateLogTagsAndData(const protocol::Message& message, LocalOp* op);

    DISALLOW_COPY_AND_ASSIGN(EngineBase);
};

}  // namespace log
}  // namespace faas
