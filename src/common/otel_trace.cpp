#include "common/otel_trace.h"
#include <absl/debugging/stacktrace.h>
#include <absl/debugging/symbolize.h>

// defined in header file
// namespace trace     = opentelemetry::trace;
// namespace nostd     = opentelemetry::nostd;

namespace trace_sdk = opentelemetry::sdk::trace;
namespace zipkin    = opentelemetry::exporter::zipkin;
namespace resource  = opentelemetry::sdk::resource;


namespace faas {
namespace otel {

#pragma region debug_utils

// Print a program counter and its symbol name.
// https://github.com/abseil/abseil-cpp/blob/master/absl/debugging/symbolize.h
static void DumpPCAndSymbol(void *pc) {
    char tmp[1024];
    const char *symbol = "(unknown)";
    if (absl::Symbolize(pc, tmp, sizeof(tmp))) {
        symbol = tmp;
    }
    fprintf(stderr, "%p  %s\n", pc, symbol);
}
static void ShowStackframe() {
    void *trace[100];
    int i, trace_size = 0;
    trace_size = absl::GetStackTrace(trace, 100, 0);
    fprintf(stderr, "[bt] Execution depth: %d\n", trace_size);
    for (i=0; i<trace_size; ++i) {
        DumpPCAndSymbol(trace[i]);
    }
}

void PrintSpanContextFromContext(context& ctx) {
    ShowStackframe();

    trace::SpanContext span_ctx = trace::GetSpan(ctx)->GetContext();
    if(!span_ctx.IsValid()) {
        LOG(INFO) << "invalid ctx!!";
        return;
    }

    std::string trace_id_str;
    std::string span_id_str;
    {
        char buffer[32];
        auto span_buffer = gsl::make_span(buffer, 32);
        span_ctx.trace_id().ToLowerBase16(span_buffer);
        trace_id_str = std::string(span_buffer.data(), span_buffer.size());
    }
    {
        char buffer[16];
        auto span_buffer = gsl::make_span(buffer, 16);
        span_ctx.span_id().ToLowerBase16(span_buffer);
        span_id_str = std::string(span_buffer.data(), span_buffer.size());
    }

    LOG(INFO) << fmt::format("span context: is_valid={}, trace_id={}, span_id={}",
            span_ctx.IsValid(), trace_id_str, span_id_str).c_str();
}

#pragma endregion

context get_context() {
    return opentelemetry::context::RuntimeContext::GetCurrent();
}

// One tracer per service is enough for now.
nostd::shared_ptr<trace::Tracer> get_tracer() {
    auto provider = trace::Provider::GetTracerProvider();
    return provider->GetTracer("global_tracer", OPENTELEMETRY_SDK_VERSION);
}

// e.g.: InitTracer("http://localhost:9411/api/v2/spans");
void InitTracer(std::string endpoint, std::string service_name) {
    // LOG(INFO) << "tracer endpoint: " << endpoint << "\n";

    zipkin::ZipkinExporterOptions opts;
    if(endpoint != "") {
        opts.endpoint = endpoint;
    }
    // else use the default endpoint, which is already set in
    // zipkin::ZipkinExporterOptions constructor

    // Create zipkin exporter instance
    resource::ResourceAttributes attributes = {{"service.name", service_name}};
    auto resource                           = resource::Resource::Create(attributes);
    auto exporter                           = zipkin::ZipkinExporterFactory::Create(opts);
    auto processor = trace_sdk::SimpleSpanProcessorFactory::Create(std::move(exporter));
    std::shared_ptr<opentelemetry::trace::TracerProvider> provider =
        trace_sdk::TracerProviderFactory::Create(std::move(processor), resource);
    // Set the global trace provider
    trace::Provider::SetTracerProvider(provider);
}

void CleanupTracer() {
    std::shared_ptr<opentelemetry::trace::TracerProvider> none;
    trace::Provider::SetTracerProvider(none);
}

} // namespace otel
} // namespace faas
