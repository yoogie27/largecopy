#pragma once
// largecopy - wan.h - WAN optimization: connection pool, adaptive inflight, sparse, delta


#include "common.h"

// ── Connection Pool ─────────────────────────────────────────────────────────
// Opens N independent file handles to the same path. Each handle becomes a
// separate SMB session / TCP connection. Round-robin dispatch across them
// multiplies effective throughput over high-latency links.

class ConnPool {
public:
    ~ConnPool();

    // Open `count` read handles to `path`, all associated with `iocp`.
    bool open_read(const wchar_t* path, int count, HANDLE iocp, ULONG_PTR key);

    // Open `count` write handles to `path`, all associated with `iocp`.
    bool open_write(const wchar_t* path, int count, HANDLE iocp, ULONG_PTR key);

    // Get next handle via round-robin.
    HANDLE next();

    // Get handle by index.
    HANDLE at(int index) const;

    // Number of open handles.
    int count() const { return count_; }

    // Cancel I/O on all handles, close them, reopen.
    bool reopen(HANDLE iocp, ULONG_PTR key);

    // Close all handles.
    void close();

    // Is read or write pool?
    bool is_read() const { return is_read_; }

private:
    HANDLE*    handles_  = nullptr;
    int        count_    = 0;
    std::atomic<uint32_t> robin_{0};
    wchar_t    path_[MAX_PATH_EXTENDED] = {};
    bool       is_read_  = true;
    DWORD      flags_    = 0;
};

// ── Adaptive Inflight Controller ────────────────────────────────────────────
// Monitors throughput in sliding windows and adjusts the number of concurrent
// in-flight operations to maximize bandwidth utilization.

class AdaptiveInflight {
public:
    void init(int initial, int min_val, int max_val);

    // Current target inflight count.
    int target() const { return target_.load(std::memory_order_relaxed); }

    // Report a chunk completion (bytes transferred).
    void on_chunk_complete(uint32_t bytes);

    // Called periodically (~1s) from progress thread. Returns new target.
    int tick();

    // Is ramping up or down?
    bool ramping_up() const { return direction_ > 0; }

    // Force target down after stall recovery (also lowers max ceiling).
    void force_reduce(int new_target);

private:
    std::atomic<int>      target_{8};
    int                   min_ = 4;
    int                   max_ = 128;
    std::atomic<uint64_t> bytes_window_{0};
    LARGE_INTEGER         window_start_{};
    LARGE_INTEGER         freq_{};
    double                prev_throughput_ = 0.0;
    int                   direction_ = 1;       // +1 = up, -1 = down
    int                   stable_count_ = 0;
    int                   stall_count_  = 0;    // consecutive stall ticks
    int                   best_target_  = 0;    // highest target that worked well
};

// ── RTT Measurement ─────────────────────────────────────────────────────────
// Measures round-trip time to a remote path by timing a small I/O.

struct RTTResult {
    double rtt_ms;          // measured RTT in milliseconds
    double bdp_bytes;       // bandwidth-delay product (link_speed * rtt)
    int    suggested_inflight;
    int    suggested_connections;
};

// chunk_size: actual configured chunk size (used for BDP → inflight calculation).
RTTResult measure_rtt(const wchar_t* remote_path, uint64_t link_speed_bps, uint32_t chunk_size);

// ── Sparse Range Detection ──────────────────────────────────────────────────
// Uses FSCTL_QUERY_ALLOCATED_RANGES to find which regions of a file contain
// data. Unallocated regions are all-zeros and can be skipped.

struct AllocRange {
    uint64_t offset;
    uint64_t length;
};

// Detect allocated ranges. Returns heap-allocated array (caller must HeapFree).
// Returns nullptr if file has no sparse regions or on error.
bool query_allocated_ranges(HANDLE file_handle, uint64_t file_size,
                            AllocRange** out_ranges, uint32_t* out_count);

// Mark chunks that fall entirely within unallocated (zero) regions as Sparse.
// Returns number of chunks marked sparse.
uint32_t mark_sparse_chunks(struct ChunkRecord* records, uint32_t chunk_count,
                            uint32_t chunk_size, uint64_t file_size,
                            const AllocRange* ranges, uint32_t range_count);

// ── Delta Pre-Scan ──────────────────────────────────────────────────────────
// Reads existing destination chunks, hashes them, and marks any that already
// match the source hash as DeltaMatch (skip transfer).

uint32_t delta_prescan(const wchar_t* dest_path, struct ChunkRecord* records,
                       uint32_t chunk_count, uint32_t chunk_size, bool verbose);
