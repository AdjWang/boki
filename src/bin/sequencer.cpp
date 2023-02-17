#include "base/init.h"
#include "base/common.h"
#include "utils/env_variables.h"
#include "log/sequencer.h"
#include "common/otel_trace.h"

ABSL_FLAG(int, node_id, -1,
          "My node ID. Also settable through environment variable FAAS_NODE_ID.");
ABSL_FLAG(std::string, tracer_exporter_endpoint, "", "Endpoint of opentelemetry exporter.");

namespace faas {

static std::atomic<server::ServerBase*> server_ptr{nullptr};
static void StopServerHandler() {
    server::ServerBase* server = server_ptr.exchange(nullptr);
    if (server != nullptr) {
        server->ScheduleStop();
    }
}

void SequencerMain(int argc, char* argv[]) {
    base::InitMain(argc, argv);
    base::SetInterruptHandler(StopServerHandler);

    int node_id = absl::GetFlag(FLAGS_node_id);
    if (node_id == -1) {
        node_id = utils::GetEnvVariableAsInt("FAAS_NODE_ID", -1);
    }
    if (node_id == -1) {
        LOG(FATAL) << "Node ID not set!";
    }
    // setup tracer
    otel::InitTracer(absl::GetFlag(FLAGS_tracer_exporter_endpoint),
                     /* service_name= */ fmt::format("sequencer_{}", node_id));
    {
        auto scoped_span = trace::Scope(otel::get_tracer()->StartSpan("SequencerMain tracer init test"));
    }

    auto sequencer = std::make_unique<log::Sequencer>(node_id);

    sequencer->Start();
    server_ptr.store(sequencer.get());
    sequencer->WaitForFinish();
}

}  // namespace faas

int main(int argc, char* argv[]) {
    faas::SequencerMain(argc, argv);
    return 0;
}
