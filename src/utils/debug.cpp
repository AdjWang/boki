#include "debug.h"

namespace faas {
namespace utils {

// Print a program counter and its symbol name.
static std::string DumpPCAndSymbol(void *pc) {
    char tmp[1024];
    const char *symbol = "(unknown)";
    if (absl::Symbolize(pc, tmp, sizeof(tmp))) {
        symbol = tmp;
    }
    return fmt::format("{}  {}\n", pc, symbol);
}

std::string DumpStackTrace() {
    constexpr int kSize = 32;
    void *stack[kSize];
    // int frames[kSize];
    int depth = absl::GetStackTrace(stack, kSize, 0);
    // absl::GetStackFrames(stack, frames, kSize, 0);

    std::string res("[DEBUG] Stack Trace:\n");
    for (int i = 0; i < depth; i++) {
        res.append(DumpPCAndSymbol(stack[i]));
    }
    return res;
}

}  // namespace utils
}  // namespace faas

