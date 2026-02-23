#pragma once
// largecopy - console.h - VT100 progress display


#include "common.h"

// Initialize console for VT100 output (enable virtual terminal processing).
void console_init();

// Print the header banner.
void print_banner();

// Print transfer configuration info.
void print_config(const Config& cfg, uint64_t file_size, uint32_t chunk_count,
                  bool have_privilege);

// Update progress display (call every ~250ms from main thread).
void print_progress(const TransferStats& stats);

// Print final transfer summary.
void print_summary(const TransferStats& stats);

// Queue a message to display above the progress bar on the next tick.
// Thread-safe: can be called from IOCP/hash threads during transfer.
void console_queue_msg(const wchar_t* fmt, ...);

// Print ledger status (for 'status' command).
void print_ledger_status(const wchar_t* ledger_path, const struct LedgerHeader* hdr,
                         uint32_t verified, uint32_t failed, uint32_t pending);

// ── Compare grid display ────────────────────────────────────────────────────
// Cell states for the visual comparison grid.
enum class CmpCell : uint8_t {
    Unchecked = 0,
    Match     = 1,
    Mismatch  = 2,
};

// Draw (or redraw) the compare grid + status line.
// First call draws from scratch; subsequent calls overwrite in-place.
// count = grid cell count, total = total file chunks, checked/mismatched = progress.
void print_compare_grid(const CmpCell* cells, int count, int grid_width,
                        uint32_t checked, uint32_t total, uint32_t mismatched,
                        double rate_chunks_per_sec, double coverage_pct);

// ── Hash progress display ────────────────────────────────────────────────────
// Update hash progress bar (overwrites in-place).
void print_hash_progress(uint64_t hashed, uint64_t total, double rate);
