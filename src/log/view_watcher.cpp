#include "log/view_watcher.h"

#define log_header_ "ViewWatcher: "

namespace faas {
namespace log {

ViewWatcher::ViewWatcher() {}

ViewWatcher::~ViewWatcher() {}

void ViewWatcher::StartWatching(zk::ZKSession* session) {
    watcher_.reset(new zk_utils::DirWatcher(
        session, "view", /* sequential_znodes= */ true));
    watcher_->SetNodeCreatedCallback(
        absl::bind_front(&ViewWatcher::OnZNodeCreated, this));
    watcher_->Start();
}

void ViewWatcher::SetViewCreatedCallback(ViewCallback cb) {
    view_created_cb_ = cb;
}

void ViewWatcher::SetViewFrozenCallback(ViewCallback cb) {
    view_frozen_cb_ = cb;
}

void ViewWatcher::SetViewFinalizedCallback(ViewFinalizedCallback cb) {
    view_finalized_cb_ = cb;
}

void ViewWatcher::InstallNextView(const ViewProto& view_proto) {
    if (view_proto.view_id() != next_view_id()) {
        HLOG(FATAL) << fmt::format("Non-consecutive view_id {}", view_proto.view_id());
    }
    View* view = new View(view_proto);
    views_.emplace_back(view);
    if (view_created_cb_) {
        view_created_cb_(view);
    }
}

void ViewWatcher::FinalizeCurrentView(const FinalizedViewProto& finalized_view_proto) {
    const View* view = current_view();
    if (view == nullptr || finalized_view_proto.view_id() != view->id()) {
        LOG(FATAL) << fmt::format("Cannot finalized current view: view_id {}",
                                  finalized_view_proto.view_id());
    }
    FinalizedView* finalized_view = new FinalizedView(view, finalized_view_proto);
    finalized_views_.emplace_back(finalized_view);
    if (view_finalized_cb_) {
        view_finalized_cb_(finalized_view);
    }
}

void ViewWatcher::OnZNodeCreated(std::string_view path, std::span<const char> contents) {
    if (absl::StartsWith(path, "new")) {
        ViewProto view_proto;
        if (!view_proto.ParseFromArray(contents.data(), contents.size())) {
            HLOG(FATAL) << "Failed to parse ViewProto";
        }
        InstallNextView(view_proto);
    } else if (absl::StartsWith(path, "freeze")) {
        int parsed;
        if (!absl::SimpleAtoi(std::string_view(contents.data(), contents.size()), &parsed)) {
            HLOG(FATAL) << "Failed to parse view ID";
        }
        const View* view = view_with_id(gsl::narrow_cast<uint16_t>(parsed));
        if (view == nullptr) {
            HLOG(FATAL) << fmt::format("Invalid view_id {}", parsed);
        }
        if (view_frozen_cb_) {
            view_frozen_cb_(view);
        }
    } else if (absl::StartsWith(path, "finalize")) {
        FinalizedViewProto finalized_view_proto;
        if (!finalized_view_proto.ParseFromArray(contents.data(), contents.size())) {
            HLOG(FATAL) << "Failed to parse FinalizedViewProto";
        }
        FinalizeCurrentView(finalized_view_proto);
    } else {
        HLOG(FATAL) << fmt::format("Unknown znode path {}", path);
    }
}

FinalizedView::FinalizedView(const View* view,
                             const FinalizedViewProto& finalized_view_proto)
    : view_(view) {
    const View::NodeIdVec& sequencer_node_ids = view_->GetSequencerNodes();
    DCHECK_EQ(gsl::narrow_cast<size_t>(finalized_view_proto.metalog_positions_size()),
              sequencer_node_ids.size());
    DCHECK_EQ(gsl::narrow_cast<size_t>(finalized_view_proto.tail_metalogs_size()),
              sequencer_node_ids.size());
    for (size_t i = 0; i < sequencer_node_ids.size(); i++) {
        uint16_t node_id = sequencer_node_ids[i];
        final_metalog_positions_[node_id] = finalized_view_proto.metalog_positions(i);
        std::vector<MetaLogProto> metalogs;
        const auto& tail_metalog_proto = finalized_view_proto.tail_metalogs(i);
        for (const MetaLogProto& metalog : tail_metalog_proto.metalogs()) {
            metalogs.push_back(metalog);
        }
        tail_metalogs_[node_id] = std::move(metalogs);
    }
}

}  // namespace log
}  // namespace faas