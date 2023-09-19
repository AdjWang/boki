#pragma once

#include "log/sequencer_base.h"
#include "log/log_space.h"
#include "log/utils.h"

namespace faas {
namespace log {

class Sequencer final : public SequencerBase {
public:
    explicit Sequencer(uint16_t node_id);
    ~Sequencer();

private:
    std::string log_header_;

    absl::Mutex view_mu_;
    const View* current_view_          ABSL_GUARDED_BY(view_mu_);
    LockablePtr<MetaLogPrimary>
        current_primary_               ABSL_GUARDED_BY(view_mu_);
    LogSpaceCollection<MetaLogPrimary>
        primary_collection_            ABSL_GUARDED_BY(view_mu_);
    LogSpaceCollection<MetaLogBackup>
        backup_collection_             ABSL_GUARDED_BY(view_mu_);

    log_utils::FutureRequests future_requests_;

    absl::Mutex metalog_tail_mu_;
    absl::flat_hash_map<uint32_t /*logspace_id*/, uint32_t /*metalog_position*/>
        last_propagated_metalog_pos_ ABSL_GUARDED_BY(metalog_tail_mu_);

    void OnViewCreated(const View* view) override;
    void OnViewFrozen(const View* view) override;
    void OnViewFinalized(const FinalizedView* finalized_view) override;

    void HandleTrimRequest(const protocol::SharedLogMessage& request) override;
    void HandleCheckTailRequest(const protocol::SharedLogMessage& request) override;
    void OnRecvMetaLogProgress(const protocol::SharedLogMessage& message) override;
    void OnRecvShardProgress(const protocol::SharedLogMessage& message,
                             std::span<const char> payload) override;
    void OnRecvNewMetaLogs(const protocol::SharedLogMessage& message,
                           std::span<const char> payload) override;

    void ProcessRequests(const std::vector<SharedLogRequest>& requests);

    void MarkNextCutIfDoable() override;

    DISALLOW_COPY_AND_ASSIGN(Sequencer);
};

}  // namespace log
}  // namespace faas
