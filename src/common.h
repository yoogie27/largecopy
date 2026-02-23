#pragma once
// largecopy - High Performance File Copy Engine
// common.h - Shared types, constants, and utilities


#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <windows.h>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstdarg>
#include <atomic>
#include <malloc.h>

// ── Version ──────────────────────────────────────────────────────────────────
static constexpr const wchar_t* LC_VERSION = L"3.0.4";
static constexpr const char*    LC_MAGIC   = "LCPY0002";

// ── Defaults ─────────────────────────────────────────────────────────────────
static constexpr uint32_t DEFAULT_CHUNK_SIZE  = 50u * 1024u * 1024u; // 50 MB
static constexpr uint32_t SECTOR_SIZE         = 4096u;
static constexpr int      DEFAULT_INFLIGHT    = 8;
static constexpr int      DEFAULT_IO_THREADS  = 0;  // 0 = auto (processor count)
static constexpr int      DEFAULT_RETRIES     = 5;
static constexpr int      DEFAULT_CONNECTIONS = 4;   // parallel SMB sessions
static constexpr int      MAX_CONNECTIONS     = 64;
static constexpr int      WAN_MIN_INFLIGHT    = 4;
static constexpr int      WAN_MAX_INFLIGHT    = 128;
static constexpr int      MAX_PATH_EXTENDED   = 520;

// ── Chunk States ─────────────────────────────────────────────────────────────
enum class ChunkState : uint8_t {
    Pending    = 0,
    Reading    = 1,
    Hashing    = 2,
    Writing    = 3,
    Written    = 4,
    Verified   = 5,
    Sparse     = 6,   // chunk is all zeros - skip transfer
    DeltaMatch = 7,   // dest already matches source - skip transfer
    Failed     = 0xFF
};

// ── Ledger Structures ────────────────────────────────────────────────────────
#pragma pack(push, 1)

struct ChunkRecord {
    uint64_t   offset;          // byte offset in file
    uint32_t   length;          // actual data length
    uint32_t   aligned_length;  // sector-aligned length for unbuffered I/O
    ChunkState state;           // current chunk state
    uint8_t    retry_count;     // number of retries attempted
    uint8_t    reserved[6];     // alignment padding / future use
    uint64_t   hash_lo;         // xxHash3-128 low 64 bits
    uint64_t   hash_hi;         // xxHash3-128 high 64 bits
    uint64_t   timestamp;       // QPC tick at last state change
};

static_assert(sizeof(ChunkRecord) == 48, "ChunkRecord must be 48 bytes");

struct LedgerHeader {
    char       magic[8];                       // "LCPY0001"
    uint64_t   source_size;                    // total source file size
    uint32_t   chunk_size;                     // configured chunk size
    uint32_t   chunk_count;                    // total number of chunks
    uint64_t   created_qpc;                    // creation timestamp (QPC)
    wchar_t    source_path[MAX_PATH_EXTENDED]; // source file path
    wchar_t    dest_path[MAX_PATH_EXTENDED];   // destination file path
    uint64_t   source_path_hash;               // xxHash3 of source path for identity
    uint8_t    completed;                      // 1 = all chunks verified
    uint8_t    reserved[63];                   // future use
};

#pragma pack(pop)

// ── Commands ─────────────────────────────────────────────────────────────────
enum class Command {
    None,
    Copy,
    Resume,
    Verify,
    Status,
    Bench,
    Compare,
    Hash,
    Help
};

enum class ProfileType {
    None,
    WLAN,
    VPNFast,
    VPNSlow,
    ExpressRoute,
    Local,
    Internet
};


// ── Configuration ────────────────────────────────────────────────────────────
struct Config {
    Command  command        = Command::None;
    wchar_t  source[MAX_PATH_EXTENDED] = {};
    wchar_t  dest[MAX_PATH_EXTENDED]   = {};
    uint32_t chunk_size    = DEFAULT_CHUNK_SIZE;
    int      io_threads    = DEFAULT_IO_THREADS;
    int      inflight      = DEFAULT_INFLIGHT;
    int      retries       = DEFAULT_RETRIES;
    int      connections   = 1;         // parallel SMB sessions (1 = legacy mode)
    bool     no_checksum   = false;
    bool     compress      = false;
    bool     verify_after  = false;
    bool     force         = false;
    bool     verbose       = false;
    bool     dry_run       = false;
    bool     quiet         = false;
    bool     wan_mode      = false;     // enable WAN optimizations
    bool     adaptive      = false;     // adaptive inflight tuning
    bool     adaptive_user_set = false; // user explicitly passed --adaptive or --no-adaptive
    int      adaptive_max  = WAN_MAX_INFLIGHT; // ceiling for adaptive controller
    int      max_writes    = 0;         // max outstanding writes (0 = unlimited)
    bool     sparse        = false;     // sparse-aware: skip zero regions
    bool     delta         = false;     // delta: skip chunks dest already has
    ProfileType profile    = ProfileType::None; // optional tuning profile
};

// ── Transfer Statistics (atomic for cross-thread reads) ──────────────────────
struct TransferStats {
    std::atomic<uint64_t> bytes_transferred{0};
    std::atomic<uint32_t> chunks_verified{0};
    std::atomic<uint32_t> chunks_failed{0};
    std::atomic<uint32_t> chunks_skipped{0};   // sparse + delta skipped
    std::atomic<uint32_t> retry_count{0};
    std::atomic<int>      current_inflight{0}; // adaptive inflight gauge
    std::atomic<int>      writes_outstanding{0}; // pending write ops in SMB pipeline
    uint64_t              bytes_skipped{0};    // bytes from sparse/delta (not transferred)
    uint64_t              resume_bytes{0};     // bytes already complete when this run started
    std::atomic<bool>     finished{false};
    std::atomic<bool>     aborted{false};
    LARGE_INTEGER         start_time{};
    uint64_t              total_bytes{0};
    uint32_t              total_chunks{0};
    int                   connections{1};

    // TCP network stats (updated by main thread each tick)
    uint32_t              net_retrans_delta{0}; // retransmits since last sample
    uint32_t              net_timeouts{0};      // cumulative retransmit timeouts
    bool                  net_stats_active{false}; // true if TCP EStats available
};

// ── IOCP Completion Keys ─────────────────────────────────────────────────────
static constexpr ULONG_PTR IOCP_KEY_READ    = 1;
static constexpr ULONG_PTR IOCP_KEY_WRITE   = 2;
static constexpr ULONG_PTR IOCP_KEY_QUIT    = 0xDEAD;

// ── Chunk Context (used as OVERLAPPED wrapper) ───────────────────────────────
enum class ChunkPhase {
    Read,
    Hash,
    Write,
    VerifyRead
};

struct ChunkContext {
    OVERLAPPED overlapped;     // MUST be first member (CONTAINING_RECORD requirement)
    uint32_t   chunk_index;
    uint8_t*   buffer;         // from buffer pool (sector-aligned)
    uint32_t   data_length;    // actual data bytes
    uint32_t   aligned_length; // sector-aligned I/O size
    ChunkPhase phase;
    uint64_t   file_offset;    // byte offset in file
    uint64_t   hash_lo;        // computed hash low
    uint64_t   hash_hi;        // computed hash high
    int        conn_index;     // which connection handle was used (-1 = default)
    bool       buffered_tail;  // true if last chunk read buffered (non-aligned)
};

// ── Utility ──────────────────────────────────────────────────────────────────
inline uint32_t align_up(uint32_t value, uint32_t alignment) {
    return (value + alignment - 1) & ~(alignment - 1);
}

inline uint64_t align_up64(uint64_t value, uint64_t alignment) {
    return (value + alignment - 1) & ~(alignment - 1);
}

// Write wide string to stderr via WriteConsoleW (console) or UTF-8 (pipe).
inline void lc_write_stderr(const wchar_t* str, DWORD len) {
    HANDLE h = GetStdHandle(STD_ERROR_HANDLE);
    DWORD mode = 0;
    if (GetConsoleMode(h, &mode)) {
        DWORD written = 0;
        WriteConsoleW(h, str, len, &written, nullptr);
    } else {
        int utf8_len = WideCharToMultiByte(CP_UTF8, 0, str, len, nullptr, 0, nullptr, nullptr);
        if (utf8_len > 0) {
            char* buf = static_cast<char*>(_alloca(utf8_len));
            WideCharToMultiByte(CP_UTF8, 0, str, len, buf, utf8_len, nullptr, nullptr);
            DWORD written = 0;
            WriteFile(h, buf, utf8_len, &written, nullptr);
        }
    }
}

inline void lc_log(const wchar_t* fmt, ...) {
    wchar_t buf[2048];
    va_list args;
    va_start(args, fmt);
    int len = vswprintf(buf, 2047, fmt, args);
    va_end(args);
    if (len > 0) {
        buf[len] = L'\n';
        lc_write_stderr(buf, len + 1);
    }
}

inline void lc_error(const wchar_t* fmt, ...) {
    wchar_t buf[2048];
    int pos = swprintf(buf, 128, L"\x1b[91mERROR: \x1b[0m");
    va_list args;
    va_start(args, fmt);
    int len = vswprintf(buf + pos, 2047 - pos, fmt, args);
    va_end(args);
    if (len > 0) {
        pos += len;
        buf[pos] = L'\n';
        lc_write_stderr(buf, pos + 1);
    }
}

inline void lc_warn(const wchar_t* fmt, ...) {
    wchar_t buf[2048];
    int pos = swprintf(buf, 128, L"\x1b[93mWARN:  \x1b[0m");
    va_list args;
    va_start(args, fmt);
    int len = vswprintf(buf + pos, 2047 - pos, fmt, args);
    va_end(args);
    if (len > 0) {
        pos += len;
        buf[pos] = L'\n';
        lc_write_stderr(buf, pos + 1);
    }
}

inline const wchar_t* format_bytes(uint64_t bytes, wchar_t* buf, size_t buf_len) {
    if (bytes >= (1ULL << 40))
        swprintf(buf, buf_len, L"%.2f TB", (double)bytes / (1ULL << 40));
    else if (bytes >= (1ULL << 30))
        swprintf(buf, buf_len, L"%.2f GB", (double)bytes / (1ULL << 30));
    else if (bytes >= (1ULL << 20))
        swprintf(buf, buf_len, L"%.2f MB", (double)bytes / (1ULL << 20));
    else if (bytes >= (1ULL << 10))
        swprintf(buf, buf_len, L"%.2f KB", (double)bytes / (1ULL << 10));
    else
        swprintf(buf, buf_len, L"%llu B", bytes);
    return buf;
}

inline const wchar_t* format_rate(double bytes_per_sec, wchar_t* buf, size_t buf_len) {
    if (bytes_per_sec >= (double)(1ULL << 30))
        swprintf(buf, buf_len, L"%.2f GB/s", bytes_per_sec / (1ULL << 30));
    else if (bytes_per_sec >= (double)(1ULL << 20))
        swprintf(buf, buf_len, L"%.2f MB/s", bytes_per_sec / (1ULL << 20));
    else
        swprintf(buf, buf_len, L"%.2f KB/s", bytes_per_sec / (1ULL << 10));
    return buf;
}
