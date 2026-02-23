// largecopy - console.cpp - VT100 progress display


#include "console.h"
#include <cstdarg>

static HANDLE g_console = INVALID_HANDLE_VALUE;
static LARGE_INTEGER g_qpc_freq = {};
static int g_last_lines = 0;
static bool g_is_console = false;

// ── Message queue for verbose/warn output during progress ──
static constexpr int MSG_QUEUE_MAX = 64;
static constexpr int MSG_MAX_LEN   = 512;
static wchar_t  g_msg_queue[MSG_QUEUE_MAX][MSG_MAX_LEN];
static int      g_msg_count = 0;
static SRWLOCK  g_msg_lock  = SRWLOCK_INIT;

// Write a wide string to stderr - uses WriteConsoleW if attached to console,
// otherwise converts to UTF-8 for pipes/redirects.
static void con_write(const wchar_t* str) {
    if (!str || !str[0]) return;
    DWORD len = static_cast<DWORD>(wcslen(str));

    if (g_is_console && g_console != INVALID_HANDLE_VALUE) {
        DWORD written = 0;
        WriteConsoleW(g_console, str, len, &written, nullptr);
    } else {
        // Convert to UTF-8 for pipe output
        int utf8_len = WideCharToMultiByte(CP_UTF8, 0, str, len, nullptr, 0, nullptr, nullptr);
        if (utf8_len > 0) {
            char* buf = static_cast<char*>(_alloca(utf8_len));
            WideCharToMultiByte(CP_UTF8, 0, str, len, buf, utf8_len, nullptr, nullptr);
            HANDLE h = GetStdHandle(STD_ERROR_HANDLE);
            DWORD written = 0;
            WriteFile(h, buf, utf8_len, &written, nullptr);
        }
    }
}

// Printf-style wrapper
static void con_printf(const wchar_t* fmt, ...) {
    wchar_t buf[2048];
    va_list args;
    va_start(args, fmt);
    int len = vswprintf(buf, 2048, fmt, args);
    va_end(args);
    if (len > 0) {
        buf[len] = L'\0';
        con_write(buf);
    }
}

void console_init() {
    g_console = GetStdHandle(STD_ERROR_HANDLE);

    // Check if stderr is actually a console (not a pipe/file)
    DWORD mode = 0;
    g_is_console = (GetConsoleMode(g_console, &mode) != 0);

    if (g_is_console) {
        mode |= ENABLE_VIRTUAL_TERMINAL_PROCESSING | ENABLE_PROCESSED_OUTPUT;
        SetConsoleMode(g_console, mode);
    }

    QueryPerformanceFrequency(&g_qpc_freq);
}

void console_queue_msg(const wchar_t* fmt, ...) {
    wchar_t buf[MSG_MAX_LEN];
    va_list args;
    va_start(args, fmt);
    int len = vswprintf(buf, MSG_MAX_LEN - 1, fmt, args);
    va_end(args);
    if (len <= 0) return;
    buf[len] = L'\0';

    AcquireSRWLockExclusive(&g_msg_lock);
    if (g_msg_count < MSG_QUEUE_MAX) {
        wcsncpy(g_msg_queue[g_msg_count], buf, MSG_MAX_LEN - 1);
        g_msg_queue[g_msg_count][MSG_MAX_LEN - 1] = L'\0';
        g_msg_count++;
    }
    ReleaseSRWLockExclusive(&g_msg_lock);
}

// Flush queued messages above the progress bar.
// Called from print_progress under the main thread.
static void flush_queued_msgs() {
    AcquireSRWLockExclusive(&g_msg_lock);
    int count = g_msg_count;
    // Copy locally so we can release the lock quickly
    wchar_t local[MSG_QUEUE_MAX][MSG_MAX_LEN];
    for (int i = 0; i < count; i++) {
        wcsncpy(local[i], g_msg_queue[i], MSG_MAX_LEN - 1);
        local[i][MSG_MAX_LEN - 1] = L'\0';
    }
    g_msg_count = 0;
    ReleaseSRWLockExclusive(&g_msg_lock);

    for (int i = 0; i < count; i++) {
        con_printf(L"%s\n", local[i]);
    }
}

void print_banner() {
    con_printf(L"\x1b[96mlargecopy\x1b[0m v%s \x1b[90m\u2014 High Performance File Copy Engine\x1b[0m\n\n",
               LC_VERSION);
}

void print_config(const Config& cfg, uint64_t file_size, uint32_t chunk_count,
                  bool have_privilege) {
    wchar_t size_buf[64];
    format_bytes(file_size, size_buf, 64);

    con_printf(L"Source:  \x1b[97m%s\x1b[0m (%s)\n", cfg.source, size_buf);
    con_printf(L"Dest:    \x1b[97m%s\x1b[0m\n", cfg.dest);

    SYSTEM_INFO si;
    GetSystemInfo(&si);
    int hash_threads = si.dwNumberOfProcessors / 2;
    if (hash_threads < 2) hash_threads = 2;

    int io_threads = cfg.io_threads;
    if (io_threads == 0) {
        io_threads = si.dwNumberOfProcessors;
        if (io_threads > 16) io_threads = 16;
    }

    wchar_t chunk_buf[64];
    format_bytes(cfg.chunk_size, chunk_buf, 64);

    con_printf(L"Engine:  IOCP | %d I/O threads | %d hash threads | %d inflight | %s chunks",
               io_threads, hash_threads, cfg.inflight, chunk_buf);
    if (!cfg.no_checksum) {
        con_printf(L" | xxHash3");
    }
    if (cfg.connections > 1) {
        con_printf(L" | %d connections", cfg.connections);
    }
    if (cfg.max_writes > 0) {
        con_printf(L" | max writes: %d", cfg.max_writes);
    }
    if (cfg.adaptive) {
        con_printf(L" | adaptive");
    }
    con_printf(L"\n");

    con_printf(L"Priv:    SetFileValidData %s\n",
               have_privilege ? L"\x1b[92m[OK]\x1b[0m" : L"\x1b[93m[NO]\x1b[0m (fallback to SetEndOfFile)");

    con_printf(L"Chunks:  %u total\n\n", chunk_count);
}

void print_progress(const TransferStats& stats) {
    if (g_qpc_freq.QuadPart == 0) return;

    uint64_t transferred = stats.bytes_transferred.load(std::memory_order_relaxed);
    uint32_t verified    = stats.chunks_verified.load(std::memory_order_relaxed);
    uint32_t failed      = stats.chunks_failed.load(std::memory_order_relaxed);
    uint32_t skipped     = stats.chunks_skipped.load(std::memory_order_relaxed);
    uint32_t retries     = stats.retry_count.load(std::memory_order_relaxed);
    int      inflight    = stats.current_inflight.load(std::memory_order_relaxed);

    LARGE_INTEGER now;
    QueryPerformanceCounter(&now);
    double elapsed = static_cast<double>(now.QuadPart - stats.start_time.QuadPart) /
                     static_cast<double>(g_qpc_freq.QuadPart);
    if (elapsed < 0.001) elapsed = 0.001;

    // Rate based on actual bytes transferred (exclude skipped sparse/delta)
    uint64_t actual = (transferred > stats.bytes_skipped)
                      ? transferred - stats.bytes_skipped : 0;
    double rate = static_cast<double>(actual) / elapsed;

    // Progress includes all bytes (transferred + skipped)
    double pct = (stats.total_bytes > 0)
                 ? (static_cast<double>(transferred) / static_cast<double>(stats.total_bytes)) * 100.0
                 : 0.0;
    if (pct > 100.0) pct = 100.0;

    // ETA based on actual transfer rate applied to remaining actual work
    uint64_t remaining_total = (stats.total_bytes > transferred)
                               ? stats.total_bytes - transferred : 0;
    double eta = (rate > 0 && remaining_total > 0)
                 ? static_cast<double>(remaining_total) / rate
                 : 0.0;

    // Move cursor up to overwrite previous progress + clear everything below
    if (g_last_lines > 0) {
        con_printf(L"\x1b[%dA\x1b[0J", g_last_lines);
    }

    // Flush any queued verbose/warn messages (they appear above the bar)
    flush_queued_msgs();

    // Progress bar (40 chars wide)
    int bar_width = 40;
    int filled = static_cast<int>(pct / 100.0 * bar_width);
    if (filled > bar_width) filled = bar_width;

    con_printf(L"\x1b[96m[\x1b[0m");
    for (int i = 0; i < bar_width; i++) {
        if (i < filled)
            con_printf(L"\x1b[92m\u2588\x1b[0m");  // full block
        else
            con_printf(L"\x1b[90m\u2591\x1b[0m");  // light shade
    }
    con_printf(L"\x1b[96m]\x1b[0m %.1f%%\n", pct);

    // Stats line
    wchar_t xfer_buf[64], total_buf[64], rate_buf[64];
    format_bytes(transferred, xfer_buf, 64);
    format_bytes(stats.total_bytes, total_buf, 64);
    format_rate(rate, rate_buf, 64);

    con_printf(L" %s / %s | %u / %u chunks | %s",
               xfer_buf, total_buf, verified, stats.total_chunks, rate_buf);

    if (eta > 0) {
        if (eta >= 60.0)
            con_printf(L" | ETA %.0fm%.0fs", eta / 60.0, fmod(eta, 60.0));
        else
            con_printf(L" | ETA %.1fs", eta);
    }
    con_printf(L"\n");

    // Retry/fail line
    con_printf(L" Retries: %u | Failed: %u | Verified: %u",
               retries, failed, verified);
    if (skipped > 0)
        con_printf(L" | Skipped: %u", skipped);
    if (inflight > 0 && stats.connections > 1)
        con_printf(L" | Inflight: %d | Conns: %d", inflight, stats.connections);
    con_printf(L"\n");

    g_last_lines = 3;
}

void print_summary(const TransferStats& stats) {
    // Flush any remaining queued messages before final summary
    flush_queued_msgs();
    g_last_lines = 0;

    LARGE_INTEGER now;
    QueryPerformanceCounter(&now);
    double elapsed = static_cast<double>(now.QuadPart - stats.start_time.QuadPart) /
                     static_cast<double>(g_qpc_freq.QuadPart);
    if (elapsed < 0.001) elapsed = 0.001;

    uint64_t transferred = stats.bytes_transferred.load();
    uint64_t actual = (transferred > stats.bytes_skipped)
                      ? transferred - stats.bytes_skipped : 0;
    double rate = (elapsed > 0.001) ? static_cast<double>(actual) / elapsed : 0.0;

    wchar_t size_buf[64], rate_buf[64];
    format_bytes(actual, size_buf, 64);
    format_rate(rate, rate_buf, 64);

    uint32_t failed = stats.chunks_failed.load();
    uint32_t verified = stats.chunks_verified.load();
    bool aborted = stats.aborted.load();

    con_printf(L"\n");
    if (aborted) {
        wchar_t total_buf[64];
        format_bytes(stats.total_bytes, total_buf, 64);
        con_printf(L"\x1b[93mTransfer interrupted\x1b[0m after %.1fs \u2014 %s / %s (%u/%u chunks)\n",
                   elapsed, size_buf, total_buf, verified, stats.total_chunks);
        con_printf(L"Run \x1b[97mlargecopy copy\x1b[0m again to resume automatically\n");
    } else if (failed == 0) {
        con_printf(L"\x1b[92mTransfer complete\x1b[0m in %.1fs \u2014 %s at %s avg\n",
                   elapsed, size_buf, rate_buf);
        con_printf(L"All %u chunks verified \x1b[92m[OK]\x1b[0m\n", verified);
    } else {
        con_printf(L"\x1b[93mTransfer finished with errors\x1b[0m in %.1fs \u2014 %s at %s avg\n",
                   elapsed, size_buf, rate_buf);
        con_printf(L"\x1b[92m%u chunks verified\x1b[0m | \x1b[91m%u chunks FAILED\x1b[0m\n",
                   verified, failed);
        con_printf(L"Run \x1b[97mlargecopy resume\x1b[0m to retry failed chunks\n");
    }
}

void print_ledger_status(const wchar_t* ledger_path, const LedgerHeader* hdr,
                         uint32_t verified, uint32_t failed, uint32_t pending) {
    wchar_t size_buf[64], chunk_buf[64];
    format_bytes(hdr->source_size, size_buf, 64);
    format_bytes(hdr->chunk_size, chunk_buf, 64);

    con_printf(L"Ledger:  %s\n", ledger_path);
    con_printf(L"Source:  %s (%s)\n", hdr->source_path, size_buf);
    con_printf(L"Dest:    %s\n", hdr->dest_path);
    con_printf(L"Chunks:  %u total (%s each)\n", hdr->chunk_count, chunk_buf);
    con_printf(L"Status:  \x1b[92m%u verified\x1b[0m | \x1b[91m%u failed\x1b[0m | \x1b[93m%u pending\x1b[0m\n",
               verified, failed, pending);

    double pct = (hdr->chunk_count > 0)
                 ? (static_cast<double>(verified) / hdr->chunk_count) * 100.0
                 : 0.0;
    con_printf(L"Progress: %.1f%%\n", pct);

    if (hdr->completed) {
        con_printf(L"\x1b[92mTransfer marked complete\x1b[0m\n");
    } else if (pending > 0 || failed > 0) {
        con_printf(L"Run \x1b[97mlargecopy resume\x1b[0m to continue\n");
    }
}

// ── Compare grid ────────────────────────────────────────────────────────────
static int g_cmp_grid_lines = 0;

void print_compare_grid(const CmpCell* cells, int count, int grid_width,
                        uint32_t checked, uint32_t total, uint32_t mismatched,
                        double rate, double coverage_pct) {
    // Move cursor up to overwrite previous grid
    if (g_cmp_grid_lines > 0) {
        con_printf(L"\x1b[%dA\x1b[0G", g_cmp_grid_lines);
    }

    int lines = 0;
    int grid_height = (count + grid_width - 1) / grid_width;

    // Draw grid rows
    for (int row = 0; row < grid_height; row++) {
        con_printf(L"  ");
        for (int col = 0; col < grid_width; col++) {
            int idx = row * grid_width + col;
            if (idx >= count) {
                con_printf(L" ");  // padding for incomplete last row
            } else {
                switch (cells[idx]) {
                    case CmpCell::Unchecked:
                        con_printf(L"\x1b[90m\u00B7\x1b[0m"); // dim middle dot
                        break;
                    case CmpCell::Match:
                        con_printf(L"\x1b[92m\u25A0\x1b[0m"); // green square
                        break;
                    case CmpCell::Mismatch:
                        con_printf(L"\x1b[91m\u25A0\x1b[0m"); // red square
                        break;
                }
            }
        }
        con_printf(L"\n");
        lines++;
    }

    // Blank line
    con_printf(L"\n");
    lines++;

    // Status line
    uint32_t match_count = checked - mismatched;
    con_printf(L"  Checked: %u/%u", checked, total);
    if (match_count > 0) con_printf(L" | \x1b[92mMatch: %u\x1b[0m", match_count);
    if (mismatched > 0)  con_printf(L" | \x1b[91mMismatch: %u\x1b[0m", mismatched);
    if (rate > 0.0)      con_printf(L" | %.0f chunks/s", rate);
    con_printf(L" | %.1f%% coverage\n", coverage_pct);
    lines++;

    g_cmp_grid_lines = lines;
}

// ── Hash progress ──────────────────────────────────────────────────────────
static int g_hash_lines = 0;

void print_hash_progress(uint64_t hashed, uint64_t total, double rate) {
    if (g_hash_lines > 0) {
        con_printf(L"\x1b[%dA\x1b[0G", g_hash_lines);
    }

    double pct = (total > 0)
                 ? (static_cast<double>(hashed) / static_cast<double>(total)) * 100.0
                 : 0.0;
    if (pct > 100.0) pct = 100.0;

    // Progress bar (40 chars)
    int bar_width = 40;
    int filled = static_cast<int>(pct / 100.0 * bar_width);
    if (filled > bar_width) filled = bar_width;

    con_printf(L"\x1b[96m[\x1b[0m");
    for (int i = 0; i < bar_width; i++) {
        if (i < filled)
            con_printf(L"\x1b[92m\u2588\x1b[0m");
        else
            con_printf(L"\x1b[90m\u2591\x1b[0m");
    }
    con_printf(L"\x1b[96m]\x1b[0m %.1f%%\n", pct);

    wchar_t hashed_buf[64], total_buf[64], rate_buf[64];
    format_bytes(hashed, hashed_buf, 64);
    format_bytes(total, total_buf, 64);
    format_rate(rate, rate_buf, 64);
    con_printf(L" %s / %s | %s\n", hashed_buf, total_buf, rate_buf);

    g_hash_lines = 2;
}
