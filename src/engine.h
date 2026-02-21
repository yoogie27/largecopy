#pragma once
// largecopy - engine.h - IOCP pipeline engine (v3: auto-detection)


#include "common.h"
#include "buffer_pool.h"
#include "ledger.h"
#include "hasher.h"
#include "smb.h"
#include "wan.h"
#include "detect.h"

class CopyEngine {
public:
    // Run a copy operation.
    int run_copy(const Config& cfg);

    // Resume an interrupted copy.
    int run_resume(const Config& cfg);

    // Verify a completed copy against stored hashes.
    int run_verify(const Config& cfg);

    // Show status of a ledger file.
    int run_status(const Config& cfg);

    // Benchmark I/O to a path.
    int run_bench(const Config& cfg);

    // Compare two files by checking chunks in random order.
    int run_compare(const Config& cfg);

    // Compute and print xxHash3-128 of a file.
    int run_hash(const Config& cfg);

private:
    // Core transfer loop (used by both copy and resume).
    int execute_transfer(const Config& cfg, Ledger& ledger);

    // Submit a read for a chunk.
    bool submit_read(uint32_t chunk_index);

    // Handle a completed read.
    void on_read_complete(ChunkContext* ctx, DWORD bytes_transferred);

    // Handle a completed write.
    void on_write_complete(ChunkContext* ctx, DWORD bytes_transferred);

    // Handle I/O error with retry logic.
    void on_io_error(ChunkContext* ctx, DWORD error_code);

    // IOCP worker thread entry.
    static DWORD WINAPI iocp_worker_entry(LPVOID param);
    void iocp_worker_loop();

    // Hash completion callback (called from hash thread).
    static void on_hash_complete(ChunkContext* ctx, void* user_data);

    // Try to reopen handles after network error.
    bool reopen_handles();

    // Try to submit more reads if inflight is below target.
    void pump_reads();

    // Build the ledger file path from destination path.
    static void build_ledger_path(const wchar_t* dest, wchar_t* ledger, size_t len);

    // Members
    const Config*      cfg_         = nullptr;
    Ledger*            ledger_      = nullptr;
    BufferPool         buffer_pool_;
    HashPool           hash_pool_;
    TransferStats      stats_;
    EnvironmentProfile env_;
    NetworkProfile     net_profile_;

    // Connection pools (replace single handles)
    ConnPool           src_pool_;
    ConnPool           dst_pool_;

    // Legacy single-handle mode (connections=1, or verify mode)
    HANDLE             src_handle_  = INVALID_HANDLE_VALUE;
    HANDLE             dst_handle_  = INVALID_HANDLE_VALUE;
    HANDLE             tail_read_handle_ = INVALID_HANDLE_VALUE; // buffered tail read handle

    HANDLE             iocp_        = nullptr;
    HANDLE             done_event_  = nullptr;
    HANDLE*            io_threads_  = nullptr;
    int                io_thread_count_ = 0;

    // Adaptive inflight
    AdaptiveInflight   adaptive_;
    bool               use_adaptive_ = false;

    // Synchronous write mode: for network destinations, writes are done
    // synchronously from hash threads instead of async via IOCP.
    // This prevents overwhelming SMB servers (macOS smbd, etc.).
    bool               sync_writes_  = false;
    HANDLE             write_sem_    = nullptr;  // gates concurrent sync writes

    CRITICAL_SECTION   submit_cs_;
    volatile long      inflight_count_ = 0;
};
