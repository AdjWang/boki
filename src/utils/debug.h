#pragma once

#include "base/common.h"

namespace faas {
namespace utils {

std::string DumpStackTrace();

}  // namespace utils
}  // namespace faas

template <typename T>
struct fmt::formatter<gsl::span<T>>: formatter<std::string_view> {
    auto format(gsl::span<T> s, format_context& ctx) const {
        std::string result;
        for (const auto data : s) {
            result.append(fmt::format("{} ", data));
        }
        return formatter<string_view>::format(result, ctx);
    }
};
