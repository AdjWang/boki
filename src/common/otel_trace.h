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

using context = opentelemetry::context::Context;
extern context get_context();

extern nostd::shared_ptr<trace::Tracer> get_tracer();
extern void InitTracer(std::string endpoint, std::string service_name);
extern void CleanupTracer();

// boki/deps/opentelemetry-cpp/examples/http/tracer_common.h
// template <typename T>
// class HttpTextMapCarrier : public opentelemetry::context::propagation::TextMapCarrier {
// public:
//     HttpTextMapCarrier<T>(T &headers) : headers_(headers) {}
//     HttpTextMapCarrier() = default;
//     virtual opentelemetry::nostd::string_view Get(
//         opentelemetry::nostd::string_view key) const noexcept override {
//         std::string key_to_compare = key.data();
//         // Header's first letter seems to be  automatically capitaliazed by our test http-server, so
//         // compare accordingly.
//         if (key == opentelemetry::trace::propagation::kTraceParent) {
//             key_to_compare = "Traceparent";
//         } else if (key == opentelemetry::trace::propagation::kTraceState) {
//             key_to_compare = "Tracestate";
//         }
//         auto it = headers_.find(key_to_compare);
//         if (it != headers_.end()) {
//             return it->second;
//         }
//         return "";
//     }
// 
//     virtual void Set(opentelemetry::nostd::string_view key,
//                      opentelemetry::nostd::string_view value) noexcept override {
//         headers_.insert(std::pair<std::string, std::string>(std::string(key), std::string(value)));
//     }
// 
//     T headers_;
// };
using json = nlohmann::json;

class StringTextMapCarrier : public opentelemetry::context::propagation::TextMapCarrier {
public:
    StringTextMapCarrier(std::multimap<std::string, std::string>&& headers) : headers_(headers) {}
    StringTextMapCarrier(StringTextMapCarrier&&) = default;
    StringTextMapCarrier() = default;
    virtual opentelemetry::nostd::string_view Get(
        opentelemetry::nostd::string_view key) const noexcept override {
        std::string key_to_compare = key.data();
        // Header's first letter seems to be  automatically capitaliazed by our test http-server, so
        // compare accordingly.
        if (key == opentelemetry::trace::propagation::kTraceParent) {
            key_to_compare = "Traceparent";
        } else if (key == opentelemetry::trace::propagation::kTraceState) {
            key_to_compare = "Tracestate";
        }
        auto it = headers_.find(key_to_compare);
        if (it != headers_.end()) {
            return it->second;
        }
        return "";
    }

    virtual void Set(opentelemetry::nostd::string_view key,
                     opentelemetry::nostd::string_view value) noexcept override {
        headers_.insert(std::pair<std::string, std::string>(std::string(key), std::string(value)));
    }

    std::string Serialize() {
        return json(headers_).dump();
    }
    static StringTextMapCarrier Deserialize(const std::string& ctx_str) {
        json ctx = json::parse(ctx_str, /*parser_callback*/nullptr,
                               /*allow_exceptions*/false,
                               /*ignore_comments*/true);
        DCHECK(!ctx.is_discarded()) << "json parse failed: " << ctx_str;
        return StringTextMapCarrier(ctx.get<std::multimap<std::string, std::string>>());
    }

private:
    std::multimap<std::string, std::string> headers_;
};

// struct cmp_ic
// {
//   bool operator()(const std::string &s1, const std::string &s2) const
//   {
//     return std::lexicographical_compare(
//         s1.begin(), s1.end(), s2.begin(), s2.end(),
//         [](char c1, char c2) { return ::tolower(c1) < ::tolower(c2); });
//   }
// };
// using Headers = std::multimap<std::string, std::string, cmp_ic>;

} // namespace otel
} // namespace faas
