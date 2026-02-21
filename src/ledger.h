#pragma once
// largecopy - ledger.h - Memory-mapped binary ledger for crash-safe chunk tracking


#include "common.h"

class Ledger {
public:
    ~Ledger();

    // Create a new ledger for a fresh transfer.
    bool create(const wchar_t* ledger_path,
                const wchar_t* source_path,
                const wchar_t* dest_path,
                uint64_t source_size,
                uint32_t chunk_size);

    // Open an existing ledger for resume.
    bool open(const wchar_t* ledger_path);

    // Close and unmap.
    void close();

    // Access header (read-only after create/open).
    const LedgerHeader* header() const { return header_; }

    // Access chunk record by index.
    ChunkRecord* chunk(uint32_t index);
    const ChunkRecord* chunk(uint32_t index) const;

    // Update chunk state atomically (writes hash first, then state).
    void mark_state(uint32_t index, ChunkState state);
    void mark_verified(uint32_t index, uint64_t hash_lo, uint64_t hash_hi);
    void mark_failed(uint32_t index);

    // Increment retry count for a chunk.
    void increment_retry(uint32_t index);

    // Find next pending chunk. Returns -1 if none.
    int find_next_pending();

    // Count chunks in each state.
    uint32_t count_verified() const;
    uint32_t count_failed() const;
    uint32_t count_pending() const;
    uint32_t count_skipped() const;  // Sparse + DeltaMatch
    bool     all_verified() const;
    bool     all_done() const;       // all verified, sparse, or delta-matched

    // Mark entire transfer as completed.
    void mark_completed();

    // Flush memory-mapped changes to disk.
    void flush();

    // Reset all non-verified chunks back to Pending (for resume).
    void reset_incomplete();

    // Get ledger path
    const wchar_t* path() const { return ledger_path_; }

private:
    wchar_t         ledger_path_[MAX_PATH_EXTENDED] = {};
    HANDLE          file_handle_   = INVALID_HANDLE_VALUE;
    HANDLE          mapping_       = nullptr;
    uint8_t*        view_          = nullptr;
    LedgerHeader*   header_        = nullptr;
    ChunkRecord*    records_       = nullptr;
    size_t          view_size_     = 0;
    CRITICAL_SECTION cs_;          // protects find_next_pending
    bool            cs_init_       = false;
};
