// largecopy - args.cpp - Command-line argument parsing


#include "args.h"
#include <cwchar>

static bool parse_uint(const wchar_t* str, uint32_t& out) {
    wchar_t* end = nullptr;
    unsigned long val = wcstoul(str, &end, 10);
    if (end == str || *end != L'\0' || val == 0) return false;
    out = static_cast<uint32_t>(val);
    return true;
}

static bool parse_int(const wchar_t* str, int& out) {
    wchar_t* end = nullptr;
    long val = wcstol(str, &end, 10);
    if (end == str || *end != L'\0' || val <= 0) return false;
    out = static_cast<int>(val);
    return true;
}

void print_usage() {
    wchar_t buf[4096];
    int len = swprintf(buf, 4096,
        L"largecopy v%s - High Performance File Copy Engine\n"
        L"\n"
        L"Usage: largecopy [command] [options] <source> <destination>\n"
        L"\n"
        L"Commands (optional - 'copy' is the default):\n"
        L"  copy      Copy file from source to destination (default)\n"
        L"  resume    Resume an interrupted transfer (provide .lcledger path)\n"
        L"  verify    Verify destination against source using stored hashes\n"
        L"  status    Show status of a transfer ledger\n"
        L"  compare   Compare two files (random order, Ctrl+C to stop)\n"
        L"  hash      Compute xxHash3-128 of a file\n"
        L"  bench     Benchmark sequential I/O to a path\n"
        L"  help      Show this help message\n"
        L"\n"
        L"Options:\n"
        L"  --chunk-size <MB>   Chunk size in megabytes (default: 50)\n"
        L"  --threads <n>       I/O thread count (default: auto)\n"
        L"  --inflight <n>      Max chunks in flight (default: 8)\n"
        L"  --retries <n>       Retry count per chunk (default: 5)\n"
        L"  --no-checksum       Skip checksumming (faster, less safe)\n"
        L"  --compress          Request SMB compression\n"
        L"  --verify-after      Full verification pass after copy\n"
        L"  --force             Overwrite existing destination\n"
        L"  --verbose           Detailed per-chunk output\n"
        L"  --dry-run           Show what would be done\n"
        L"  --quiet             Minimal output\n"
        L"  --ssd              Treat destination as SSD/NVMe (force async, skip HDD throttles)\n"
        L"  --safe-net         Force synchronous writes for network destinations (slower, safer)\n"
        L"  --buffered         Use Windows system cache (slower, but plays better with antivirus)\n"
        L"\n"
        L"WAN Optimization:\n"
        L"  --wan               Enable WAN mode (auto-tune for high latency)\n"
        L"  --connections <n>   Parallel SMB sessions (default: 4 in WAN mode)\n"
        L"  --adaptive          Auto-tune inflight based on throughput\n"
        L"  --no-adaptive       Disable adaptive tuning (fixed inflight)\n"
        L"  --sparse            Skip zero-filled regions (sparse files)\n"
        L"  --delta             Skip chunks dest already has (hash compare)\n"
        L"\n"
        L"  -h, --help          Show this help message\n"
        L"  --version           Show version and exit\n"
        L"\n"
        L"Examples:\n"
        L"  largecopy D:\\backup.vhdx \\\\server\\share\\backup.vhdx\n"
        L"  largecopy \\\\server\\data\\big.zip E:\\local\\big.zip\n"
        L"  largecopy resume E:\\local\\big.zip.lcledger\n"
        L"  largecopy verify D:\\backup.vhdx \\\\server\\share\\backup.vhdx\n"
        L"  largecopy --wan D:\\big.vhdx \\\\remote\\share\\big.vhdx\n"
        L"  largecopy --wan --connections 8 --sparse D:\\vm.vhdx \\\\nas\\bak\\vm.vhdx\n"
        L"  largecopy compare D:\\backup.vhdx \\\\server\\share\\backup.vhdx\n"
        L"  largecopy hash D:\\backup.vhdx\n"
        L"  largecopy bench \\\\server\\share\\\n",
        LC_VERSION);
    if (len > 0) {
        lc_write_stderr(buf, len);
    }
}

bool parse_args(int argc, wchar_t* argv[], Config& cfg) {
    if (argc < 2) {
        print_usage();
        return false;
    }

    // Parse command (or infer 'copy' when first arg looks like a path)
    const wchar_t* cmd = argv[1];
    int args_start = 2;  // index where options/positional args begin

    if (_wcsicmp(cmd, L"copy") == 0)        cfg.command = Command::Copy;
    else if (_wcsicmp(cmd, L"resume") == 0)  cfg.command = Command::Resume;
    else if (_wcsicmp(cmd, L"verify") == 0)  cfg.command = Command::Verify;
    else if (_wcsicmp(cmd, L"status") == 0)  cfg.command = Command::Status;
    else if (_wcsicmp(cmd, L"bench") == 0)   cfg.command = Command::Bench;
    else if (_wcsicmp(cmd, L"compare") == 0) cfg.command = Command::Compare;
    else if (_wcsicmp(cmd, L"hash") == 0)    cfg.command = Command::Hash;
    else if (_wcsicmp(cmd, L"help") == 0 || _wcsicmp(cmd, L"-h") == 0 ||
             _wcsicmp(cmd, L"--help") == 0) {
        cfg.command = Command::Help;
        return true;
    } else if (_wcsicmp(cmd, L"version") == 0 || _wcsicmp(cmd, L"--version") == 0) {
        cfg.command = Command::Version;
        return true;
    } else if (cmd[0] == L'-') {
        // Starts with dash: treat as option for implicit copy
        cfg.command = Command::Copy;
        args_start = 1;
    } else if (wcschr(cmd, L'\\') || wcschr(cmd, L'/') ||
               wcschr(cmd, L':')  || wcschr(cmd, L'.')) {
        // Looks like a path: implicit copy, argv[1] is the source
        cfg.command = Command::Copy;
        args_start = 1;
    } else {
        lc_error(L"Unknown command: %s\n"
                 L"Hint: you can omit 'copy' when providing paths directly:\n"
                 L"  largecopy <source> <destination>", cmd);
        return false;
    }

    // Collect positional args (source, dest)
    const wchar_t* positional[2] = {};
    int pos_count = 0;

    // Walk remaining args
    for (int i = args_start; i < argc; i++) {
        const wchar_t* arg = argv[i];

        if (wcscmp(arg, L"--chunk-size") == 0) {
            if (++i >= argc) { lc_error(L"--chunk-size requires a value"); return false; }
            uint32_t mb = 0;
            if (!parse_uint(argv[i], mb)) { lc_error(L"Invalid chunk size: %s", argv[i]); return false; }
            cfg.chunk_size = mb * 1024u * 1024u;
            if (cfg.chunk_size < SECTOR_SIZE) {
                lc_error(L"Chunk size must be at least %u bytes", SECTOR_SIZE);
                return false;
            }
            // Ensure chunk size is sector-aligned
            cfg.chunk_size = align_up(cfg.chunk_size, SECTOR_SIZE);
        }
        else if (wcscmp(arg, L"--threads") == 0) {
            if (++i >= argc) { lc_error(L"--threads requires a value"); return false; }
            if (!parse_int(argv[i], cfg.io_threads)) { lc_error(L"Invalid thread count: %s", argv[i]); return false; }
        }
        else if (wcscmp(arg, L"--inflight") == 0) {
            if (++i >= argc) { lc_error(L"--inflight requires a value"); return false; }
            if (!parse_int(argv[i], cfg.inflight)) { lc_error(L"Invalid inflight count: %s", argv[i]); return false; }
            cfg.inflight_user_set = true;
        }
        else if (wcscmp(arg, L"--retries") == 0) {
            if (++i >= argc) { lc_error(L"--retries requires a value"); return false; }
            if (!parse_int(argv[i], cfg.retries)) { lc_error(L"Invalid retry count: %s", argv[i]); return false; }
        }
        else if (wcscmp(arg, L"--ssd") == 0)            cfg.force_ssd      = true;
        else if (wcscmp(arg, L"--safe-net") == 0)       cfg.force_safe_net = true;
        else if (wcscmp(arg, L"--buffered") == 0)       cfg.buffered       = true;
        else if (wcscmp(arg, L"--no-checksum") == 0)    cfg.no_checksum    = true;
        else if (wcscmp(arg, L"--compress") == 0)        cfg.compress     = true;
        else if (wcscmp(arg, L"--verify-after") == 0)    cfg.verify_after = true;
        else if (wcscmp(arg, L"--force") == 0)           cfg.force        = true;
        else if (wcscmp(arg, L"--verbose") == 0)         cfg.verbose      = true;
        else if (wcscmp(arg, L"--dry-run") == 0)         cfg.dry_run      = true;
        else if (wcscmp(arg, L"--quiet") == 0)           cfg.quiet        = true;
        else if (wcscmp(arg, L"--wan") == 0)             cfg.wan_mode     = true;
        else if (wcscmp(arg, L"--adaptive") == 0)        { cfg.adaptive = true;  cfg.adaptive_user_set = true; }
        else if (wcscmp(arg, L"--no-adaptive") == 0)     { cfg.adaptive = false; cfg.adaptive_user_set = true; }
        else if (wcscmp(arg, L"--sparse") == 0)          cfg.sparse       = true;
        else if (wcscmp(arg, L"--delta") == 0)           cfg.delta        = true;
        else if (wcscmp(arg, L"--connections") == 0) {
            if (++i >= argc) { lc_error(L"--connections requires a value"); return false; }
            if (!parse_int(argv[i], cfg.connections)) { lc_error(L"Invalid connection count: %s", argv[i]); return false; }
            if (cfg.connections > MAX_CONNECTIONS) {
                lc_error(L"Max connections is %d", MAX_CONNECTIONS);
                return false;
            }
            cfg.connections_user_set = true;
        }
        else if (wcscmp(arg, L"-h") == 0 || wcscmp(arg, L"--help") == 0) {
            cfg.command = Command::Help;
            return true;
        }
        else if (wcscmp(arg, L"--version") == 0) {
            cfg.command = Command::Version;
            return true;
        }
        else if (arg[0] == L'-') {
            lc_error(L"Unknown option: %s", arg);
            return false;
        }
        else {
            if (pos_count >= 2) {
                lc_error(L"Too many positional arguments");
                return false;
            }
            positional[pos_count++] = arg;
        }
    }

    // Validate positional args based on command
    switch (cfg.command) {
        case Command::Copy:
        case Command::Verify:
        case Command::Compare:
            if (pos_count < 2) {
                lc_error(L"'%s' requires <source> and <destination>", cmd);
                return false;
            }
            {
                const wchar_t* src = positional[0];
                if (src[0] == L'@') src++;
                wcsncpy(cfg.source, src, MAX_PATH_EXTENDED - 1);

                const wchar_t* dst = positional[1];
                if (dst[0] == L'@') dst++;
                wcsncpy(cfg.dest, dst, MAX_PATH_EXTENDED - 1);
            }
            break;

        case Command::Resume:
        case Command::Status:
            if (pos_count < 1) {
                lc_error(L"'%s' requires <ledger-path> or <destination>", cmd);
                return false;
            }
            wcsncpy(cfg.source, positional[0], MAX_PATH_EXTENDED - 1);
            if (pos_count > 1)
                wcsncpy(cfg.dest, positional[1], MAX_PATH_EXTENDED - 1);
            break;

        case Command::Hash:
            if (pos_count < 1) {
                lc_error(L"'hash' requires a <file>");
                return false;
            }
            wcsncpy(cfg.source, positional[0], MAX_PATH_EXTENDED - 1);
            break;

        case Command::Bench:
            if (pos_count < 1) {
                lc_error(L"'bench' requires a <path>");
                return false;
            }
            wcsncpy(cfg.dest, positional[0], MAX_PATH_EXTENDED - 1);
            break;

        default:
            break;
    }

    // WAN mode implies adaptive + higher connections
    if (cfg.wan_mode) {
        if (cfg.connections == 1)
            cfg.connections = DEFAULT_CONNECTIONS;
        if (!cfg.adaptive_user_set)
            cfg.adaptive = true;
    }

    return true;
}
