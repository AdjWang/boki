#include "base/init.h"
#include "base/common.h"
#include "base/thread.h"
#include "ipc/base.h"
#include "utils/docker.h"
#include "utils/fs.h"
#include "utils/procfs.h"
#include "utils/env_variables.h"
#include "engine/engine.h"
#include "common/otel_trace.h"
#include <iostream>
#include <absl/debugging/stacktrace.h>
#include <absl/debugging/symbolize.h>

using namespace faas;

// // Print a program counter and its symbol name.
// // https://github.com/abseil/abseil-cpp/blob/master/absl/debugging/symbolize.h
// static void DumpPCAndSymbol(void *pc) {
//     char tmp[1024];
//     const char *symbol = "(unknown)";
//     if (absl::Symbolize(pc, tmp, sizeof(tmp))) {
//         symbol = tmp;
//     }
//     fprintf(stderr, "%p  %s\n", pc, symbol);
// }
// static void ShowStackframe() {
//     void *trace[100];
//     int i, trace_size = 0;
//     trace_size = absl::GetStackTrace(trace, 100, 0);
//     fprintf(stderr, "[bt] Execution depth: %d\n", trace_size);
//     for (i=0; i<trace_size; ++i) {
//         DumpPCAndSymbol(trace[i]);
//     }
// }

std::function<void()> get_closure(const std::string& some_str) {
    return [&some_str](){
        std::cout << fmt::format("str={}", some_str);
    };
}

int main(int argc, char* argv[]) {
    absl::InitializeSymbolizer(argv[0]);
    faas::base::Thread::RegisterMainThread();

    std::cout << "test func start\n";
    std::function<void()> func;
    {
        func = get_closure("string to print");
    }
    func();
    std::cout << "test func end\n";
    return 0;
}

//     // setup tracer
//     faas::otel::InitTracer("http://localhost:9411/api/v2/spans", "test tracer");

//     auto trace_span = otel::get_tracer()->StartSpan("test span");
//     auto trace_scope = otel::get_tracer()->WithActiveSpan(trace_span);

//     otel::context ctx(otel::get_context());

//     std::cout << "new ctx\n";
//     otel::PrintSpanContextFromContext(ctx);

//     std::span<const char> ctx_data;
//     {
//         auto propagator = trace::propagation::HttpTraceContext();
//         otel::StringTextMapCarrier carrier;
//         propagator.Inject(carrier, ctx);
//         ctx_data = gsl::make_span(carrier.Serialize());
    
//         std::cout << "current ctx\n";
//         otel::PrintSpanContextFromContext(ctx);
//     }

//     {
//         auto propagator = trace::propagation::HttpTraceContext();
//         std::string ctx_str(std::string(ctx_data.data(), ctx_data.size()));
//         std::cout << "ctx string: " << ctx_str << "\n";
//         auto carrier = otel::StringTextMapCarrier::Deserialize(ctx_str);
//         otel::context ctx(otel::get_context());

//         std::cout << "propagated ctx before extract\n";
//         otel::PrintSpanContextFromContext(ctx);

//         ctx = propagator.Extract(carrier, ctx);

//         std::cout << "propagated ctx before sub span\n";
//         otel::PrintSpanContextFromContext(ctx);

//         trace::StartSpanOptions options;
//         options.parent = trace::GetSpan(ctx)->GetContext();
//         auto trace_span = otel::get_tracer()->StartSpan("sub test span", options);
//         auto trace_scope = otel::get_tracer()->WithActiveSpan(trace_span);
//         trace_span->End();

//         std::cout << "propagated ctx after sub span\n";
//         otel::PrintSpanContextFromContext(ctx);
//     }
    
//     trace_span->End();
//     std::cout << "ok\n";

//     return 0;
// }
