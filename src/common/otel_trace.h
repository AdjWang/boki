// usage:
// docker:
// docker run -d -p 9411:9411 openzipkin/zipkin
// 
// main.cpp:
// #include "common/otel_trace.h"
// 
// void f1() {
//   auto scoped_span = trace::Scope(faas::otel::get_tracer()->StartSpan("f1"));
// }
// 
// void f2() {
//   auto scoped_span = trace::Scope(faas::otel::get_tracer()->StartSpan("f2"));
// 
//   f1();
//   f1();
// }
// 
// void foo_library() {
//   auto scoped_span = trace::Scope(otel::get_tracer()->StartSpan(
//     "library",
//     {{"key", "val"}}
//   ));
// 
//   f2();
// }
// 
// int main(int argc, char* argv[]) {
//     faas::otel::InitTracer("http://localhost:9411/api/v2/spans");
// 
//     foo_library();
// 
//     faas::otel::CleanupTracer();
// 
//     return 0;
// }

#pragma once

#include "base/common.h"

namespace trace     = opentelemetry::trace;
namespace nostd     = opentelemetry::nostd;

namespace faas {
namespace otel {

extern nostd::shared_ptr<trace::Tracer> get_tracer();
extern void InitTracer(std::string endpoint, std::string service_name);
extern void CleanupTracer();

} // namespace otel
} // namespace faas
