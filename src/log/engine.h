#pragma once

#include "log/engine_base.h"
#include "log/log_space.h"
#include "log/index.h"
#include "log/utils.h"

namespace faas {

// Forward declaration
namespace engine { class Engine; }

namespace log {

class Engine final : public EngineBase {
public:
    explicit Engine(engine::Engine* engine);
    ~Engine();

private:
    std::string log_header_;

    absl::Mutex view_mu_;
    const View* current_view_        ABSL_GUARDED_BY(view_mu_);
    bool current_view_active_        ABSL_GUARDED_BY(view_mu_);
    std::vector<const View*> views_  ABSL_GUARDED_BY(view_mu_);
    LogSpaceCollection<LogProducer>
        producer_collection_         ABSL_GUARDED_BY(view_mu_);
    LogSpaceCollection<Index>
        index_collection_            ABSL_GUARDED_BY(view_mu_);

    bool log_cache_enabled_;
    std::unique_ptr<ShardedLRUCache> log_cache_;

    log_utils::FutureRequests       future_requests_;
    log_utils::ThreadedMap<LocalOp> onging_local_reads_;

    void OnViewCreated(const View* view) override;
    void OnViewFrozen(const View* view) override;
    void OnViewFinalized(const FinalizedView* finalized_view) override;

    void HandleLocalAppend(LocalOp* op) override;
    void HandleLocalTrim(LocalOp* op) override;
    void HandleLocalRead(LocalOp* op) override;
    void HandleLocalSetAuxData(LocalOp* op) override;

    void HandleRemoteRead(const protocol::SharedLogMessage& request) override;
    void OnRecvNewMetaLogs(const protocol::SharedLogMessage& message,
                           std::span<const char> payload) override;
    void OnRecvNewIndexData(const protocol::SharedLogMessage& message,
                            std::span<const char> payload) override;
    void OnRecvResponse(const protocol::SharedLogMessage& message,
                        std::span<const char> payload) override;

    void ProcessAppendResults(const LogProducer::AppendResultVec& results);
    void ProcessIndexQueryResults(const Index::QueryResultVec& results);
    void ProcessRequests(const std::vector<SharedLogRequest>& requests);

    void ProcessIndexFoundResult(const IndexQueryResult& query_result);
    void ProcessIndexContinueResult(const IndexQueryResult& query_result,
                                    Index::QueryResultVec* more_results);

    void LogCachePut(const LogMetaData& log_metadata,
                     std::span<const uint64_t> user_tags,
                     std::span<const char> log_data);
    std::optional<LogEntry> LogCacheGet(uint64_t seqnum);
    void LogCachePutAuxData(const AuxEntry& aux_entry);
    void LogCachePutAuxData(const AuxMetaData& aux_metadata,
                            std::span<const uint64_t> user_tags,
                            std::span<const char> aux_data);
    std::optional<AuxEntry> LogCacheGetAuxData(uint64_t seqnum);
    std::optional<AuxEntry> LogCacheGetAuxDataPrev(uint64_t tag, uint64_t seqnum);
    std::optional<AuxEntry> LogCacheGetAuxDataNext(uint64_t tag, uint64_t seqnum);

    inline AuxMetaData AuxMetaDataFromOp(LocalOp* op) {
        DCHECK(op->type == protocol::SharedLogOpType::SET_AUXDATA);
        return AuxMetaData {
            .seqnum = op->seqnum,
            .num_tags = op->user_tags.size(),
            .data_size = op->data.length()
        };
    }

    inline LogMetaData MetaDataFromAppendOp(LocalOp* op) {
        DCHECK(op->type == protocol::SharedLogOpType::APPEND
            || op->type == protocol::SharedLogOpType::ASYNC_APPEND);
        return LogMetaData {
            .user_logspace = op->user_logspace,
            .seqnum = kInvalidLogSeqNum,
            .localid = 0,
            .num_tags = op->user_tags.size(),
            .data_size = op->data.length()
        };
    }

    protocol::SharedLogMessage BuildReadRequestMessage(LocalOp* op);
    protocol::SharedLogMessage BuildReadRequestMessage(const IndexQueryResult& result);

    IndexQuery UpdateQueryWithAux(/*ref*/ IndexQuery& query);
    IndexQuery BuildIndexQuery(LocalOp* op);
    IndexQuery BuildIndexQuery(const protocol::SharedLogMessage& message);
    IndexQuery BuildIndexQuery(const IndexQueryResult& result);

    DISALLOW_COPY_AND_ASSIGN(Engine);
};

}  // namespace log
}  // namespace faas
