// largecopy - main.cpp - Entry point


#include "common.h"
#include "args.h"
#include "engine.h"
#include "console.h"

// Global abort flag - checked by engine progress loop
volatile long g_abort = 0;

static BOOL WINAPI ctrl_handler(DWORD event) {
    if (event == CTRL_C_EVENT || event == CTRL_BREAK_EVENT) {
        if (InterlockedIncrement(&g_abort) == 1) {
            // First Ctrl+C: request graceful shutdown
            lc_warn(L"Interrupted - shutting down (press Ctrl+C again to force)...");
            return TRUE;  // handled, don't kill yet
        } else {
            // Second Ctrl+C: force exit immediately
            lc_error(L"Force exit.");
            ExitProcess(130);
        }
    }
    return FALSE;
}

int wmain(int argc, wchar_t* argv[]) {
    SetConsoleCtrlHandler(ctrl_handler, TRUE);
    console_init();

    Config cfg;
    if (!parse_args(argc, argv, cfg)) {
        return 1;
    }

    if (cfg.command == Command::Help) {
        print_usage();
        return 0;
    }

    CopyEngine engine;

    switch (cfg.command) {
        case Command::Copy:
            return engine.run_copy(cfg);

        case Command::Resume:
            return engine.run_resume(cfg);

        case Command::Verify:
            return engine.run_verify(cfg);

        case Command::Status:
            return engine.run_status(cfg);

        case Command::Bench:
            return engine.run_bench(cfg);

        case Command::Compare:
            return engine.run_compare(cfg);

        case Command::Hash:
            return engine.run_hash(cfg);

        default:
            print_usage();
            return 1;
    }
}
