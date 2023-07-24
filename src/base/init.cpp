#include "base/diagnostic.h"
#include "base/init.h"
#include "base/logging.h"
#include "base/thread.h"

#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <execinfo.h>
#include <ucontext.h>

__BEGIN_THIRD_PARTY_HEADERS

#include <absl/flags/flag.h>
#include <absl/flags/parse.h>
#include <absl/debugging/symbolize.h>
#include <absl/debugging/failure_signal_handler.h>

__END_THIRD_PARTY_HEADERS

ABSL_FLAG(int, v, 0, "Show all VLOG(m) messages for m <= this.");

#define RAW_CHECK(EXPR, MSG)             \
    do {                                 \
        if (!(EXPR)) {                   \
            fprintf(stderr, MSG "\n");   \
            exit(EXIT_FAILURE);          \
        }                                \
    } while (0)

namespace faas {
namespace base {

namespace {
static constexpr size_t      kNumCleanupFns = 64;
static std::function<void()> cleanup_fns[kNumCleanupFns];
static std::atomic<size_t>   next_cleanup_fn{0};

static std::function<void()> sigint_handler;

static std::string DumpPCAndSymbol(void *pc) {
    char tmp[1024];
    const char *symbol = "(unknown)";
    if (absl::Symbolize(pc, tmp, sizeof(tmp))) {
        symbol = tmp;
    }
    return fmt::format("{}  {}\n", pc, symbol);
}

static void RaiseToDefaultHandler(int signo) {
    signal(signo, SIG_DFL);
    raise(signo);
}

static void PrintStackTrace(ucontext_t* uc) {
    constexpr int kSize = 16;
    void *trace[kSize];
    int trace_size = backtrace(trace, kSize);
    /* overwrite sigaction with caller's address */
    trace[1] = (void *)uc->uc_mcontext.gregs[REG_RIP];

    std::string res("Stack Trace:\n");
    for (int i = 0; i < trace_size; i++) {
        res.append(DumpPCAndSymbol(trace[i]));
    }
    fprintf(stderr, "%s\n", res.c_str());
}

static void BTSignalHandler(int signo, siginfo_t *info, void *secret) {
    ucontext_t *uc = (ucontext_t *)secret;
    if (signo == SIGTERM || signo == SIGABRT) {
        // dump stacktrace
        PrintStackTrace(uc);
        // clean up
        size_t n = next_cleanup_fn.load();
        fprintf(stderr, "Invoke %d clean-up functions\n", (int) n);
        for (size_t i = 0; i < n; i++) {
            cleanup_fns[i]();
        }
        fprintf(stderr, "Exit with failure\n");
        exit(EXIT_FAILURE);
    } else if (signo == SIGINT) {
        if (sigint_handler) {
            sigint_handler();
        } else {
            RaiseToDefaultHandler(SIGINT);
        }
    } else {
        if (signo == SIGSEGV) {
            fprintf(stderr,
                "SIGSEGV, faulty address is %p, from %p\n",
                info->si_addr, (void*)uc->uc_mcontext.gregs[REG_RIP]);
        }
        PrintStackTrace(uc);
        RaiseToDefaultHandler(signo);
    }
}
}  // namespace

void InitMain(int argc, char* argv[],
              std::vector<char*>* positional_args) {
    absl::InitializeSymbolizer(argv[0]);

    struct sigaction act;
    memset(&act, 0, sizeof(struct sigaction));
    act.sa_sigaction = BTSignalHandler;
    act.sa_flags |= SA_SIGINFO;
    RAW_CHECK(sigaction(SIGABRT, &act, nullptr) == 0,
              "Failed to set SIGABRT handler");
    RAW_CHECK(sigaction(SIGTERM, &act, nullptr) == 0,
              "Failed to set SIGTERM handler");
    RAW_CHECK(sigaction(SIGINT, &act, nullptr) == 0,
              "Failed to set SIGINT handler");

    absl::FailureSignalHandlerOptions options;
    options.call_previous_handler = true;
    absl::InstallFailureSignalHandler(options);

    std::vector<char*> unparsed_args = absl::ParseCommandLine(argc, argv);
    logging::Init(absl::GetFlag(FLAGS_v));

    if (positional_args == nullptr && unparsed_args.size() > 1) {
        LOG(FATAL) << "This program does not accept positional arguments";
    }
    if (positional_args != nullptr) {
        positional_args->clear();
        for (size_t i = 1; i < unparsed_args.size(); i++) {
            positional_args->push_back(unparsed_args[i]);
        }
    }

    Thread::RegisterMainThread();
    #if defined(DEBUG)
    LOG(INFO) << "Running DEBUG built version";
    #endif
    #if defined(NDEBUG)
    LOG(INFO) << "Running RELEASE built version";
    #endif
}

void ChainCleanupFn(std::function<void()> fn) {
    size_t idx = next_cleanup_fn.fetch_add(1);
    if (idx >= kNumCleanupFns) {
        LOG(FATAL) << "Not enough statically allocated clean-up function slots, "
                      "consider enlarge kNumCleanupFns";
    }
    cleanup_fns[idx] = fn;
}

void SetInterruptHandler(std::function<void()> fn) {
    if (sigint_handler) {
        LOG(FATAL) << "Interrupt handler can only be set once!";
    }
    sigint_handler = fn;
}

}  // namespace base
}  // namespace faas
