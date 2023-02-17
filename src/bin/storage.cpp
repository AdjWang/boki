#include "base/init.h"
#include "base/common.h"
#include "utils/env_variables.h"
#include "log/storage.h"
#include "common/otel_trace.h"

ABSL_FLAG(int, node_id, -1,
          "My node ID. Also settable through environment variable FAAS_NODE_ID.");
ABSL_FLAG(std::string, db_path, "", "Path for RocksDB database storage.");
ABSL_FLAG(std::string, tracer_exporter_endpoint, "", "Endpoint of opentelemetry exporter.");

namespace faas {

static std::atomic<server::ServerBase*> server_ptr{nullptr};
static void StopServerHandler() {
    server::ServerBase* server = server_ptr.exchange(nullptr);
    if (server != nullptr) {
        server->ScheduleStop();
    }
}

void StorageMain(int argc, char* argv[]) {
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
                     /* service_name= */ fmt::format("storage_{}", node_id));
    {
        auto scoped_span = trace::Scope(otel::get_tracer()->StartSpan("StorageMain tracer init test"));
    }

    auto storage = std::make_unique<log::Storage>(node_id);

    storage->set_db_path(absl::GetFlag(FLAGS_db_path));

    storage->Start();
    server_ptr.store(storage.get());
    storage->WaitForFinish();
}

}  // namespace faas

int main(int argc, char* argv[]) {
    faas::StorageMain(argc, argv);
    return 0;
}
