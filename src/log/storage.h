#pragma once

#include "log/storage_base.h"
#include "log/log_space.h"
#include "log/utils.h"

namespace faas {
namespace log {

class Storage final : public StorageBase {
public:
    explicit Storage(uint16_t node_id);
    ~Storage();

private:
    std::string log_header_;

    absl::Mutex view_mu_;
    const View* current_view_      ABSL_GUARDED_BY(view_mu_);
    bool view_finalized_           ABSL_GUARDED_BY(view_mu_);
    LogSpaceCollection<LogStorage>
        storage_collection_        ABSL_GUARDED_BY(view_mu_);

    log_utils::FutureRequests future_requests_;

    void OnViewCreated(const View* view) override;
    void OnViewFinalized(const FinalizedView* finalized_view) override;

    // 4 types of requests
    void HandleReadAtRequest(const protocol::SharedLogMessage& request) override;
    void HandleReplicateRequest(const protocol::SharedLogMessage& message,
                                std::span<const char> payload) override;
    void OnRecvNewMetaLogs(const protocol::SharedLogMessage& message,
                           std::span<const char> payload) override;
    void OnRecvLogAuxData(const protocol::SharedLogMessage& message,
                          std::span<const char> payload) override;

    // helpers
    void ProcessReadResults(const LogStorage::ReadResultVec& results);
    void ProcessReadFromDB(const protocol::SharedLogMessage& request);

    // process 4 types of requests
    void ProcessRequests(const std::vector<SharedLogRequest>& requests);

    void SendEngineLogResult(const protocol::SharedLogMessage& request,
                             protocol::SharedLogMessage* response,
                             std::span<const char> tags_data,
                             std::span<const char> log_data);

    // set up flush timer
    void BackgroundThreadMain() override;
    
    // period at pace of FLAGS_slog_local_cut_interval_us which is applied in StorageBase::SetupTimers()
    void SendShardProgressIfNeeded() override;

    // period at pace of FLAGS_slog_storage_bgthread_interval_ms which is applied in BackgroundThreadMain()
    // flush to db
    void FlushLogEntries();

    DISALLOW_COPY_AND_ASSIGN(Storage);
};

}  // namespace log
}  // namespace faas
