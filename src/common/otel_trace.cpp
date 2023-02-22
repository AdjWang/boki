#include "common/otel_trace.h"

// defined in header file
// namespace trace     = opentelemetry::trace;
// namespace nostd     = opentelemetry::nostd;

namespace trace_sdk = opentelemetry::sdk::trace;
namespace zipkin    = opentelemetry::exporter::zipkin;
namespace resource  = opentelemetry::sdk::resource;


namespace faas {
namespace otel {

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
    LOG(INFO) << "tracer endpoint: " << endpoint << "\n";

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
