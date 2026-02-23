// largecopy - wan.cpp - WAN optimization implementation


#include "wan.h"
#include <winioctl.h>
#include <cwchar>

#define XXH_INLINE_ALL
#define XXH_STATIC_LINKING_ONLY
#include "xxhash.h"

// ═══════════════════════════════════════════════════════════════════════════
// Connection Pool
// ═══════════════════════════════════════════════════════════════════════════

ConnPool::~ConnPool() {
    close();
}

bool ConnPool::open_read(const wchar_t* path, int count, HANDLE iocp, ULONG_PTR key, bool buffered) {
    is_read_ = true;
    flags_ = FILE_FLAG_OVERLAPPED | FILE_FLAG_SEQUENTIAL_SCAN;
    if (!buffered) flags_ |= FILE_FLAG_NO_BUFFERING;
    wcsncpy(path_, path, MAX_PATH_EXTENDED - 1);
    count_ = count;
    robin_.store(0);

    handles_ = static_cast<HANDLE*>(
        HeapAlloc(GetProcessHeap(), HEAP_ZERO_MEMORY, count * sizeof(HANDLE)));
    if (!handles_) return false;

    for (int i = 0; i < count; i++) {
        handles_[i] = CreateFileW(
            path, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE,
            nullptr, OPEN_EXISTING, flags_, nullptr);

        if (handles_[i] == INVALID_HANDLE_VALUE) {
            lc_error(L"ConnPool: failed to open read handle %d/%d to '%s': %u",
                     i + 1, count, path, GetLastError());
            close();
            return false;
        }

        // Associate with IOCP
        if (!CreateIoCompletionPort(handles_[i], iocp, key, 0)) {
            lc_error(L"ConnPool: failed to associate handle %d with IOCP: %u",
                     i, GetLastError());
            close();
            return false;
        }
    }

    return true;
}

bool ConnPool::open_write(const wchar_t* path, int count, HANDLE iocp, ULONG_PTR key, bool force_ssd, bool buffered) {
    is_read_ = false;

    // Detect if destination is remote (UNC or mapped network drive)
    bool remote = false;
    if (path[0] == L'\\' && path[1] == L'\\') {
        remote = true;
    } else if (path[1] == L':') {
        wchar_t root[4] = { path[0], L':', L'\\', L'\0' };
        remote = (GetDriveTypeW(root) == DRIVE_REMOTE);
    }

    if (buffered) {
        flags_ = FILE_FLAG_OVERLAPPED;
    }
    else if (remote) {
        // For remote: check if it's NTFS/ReFS. If so, we can use NO_BUFFERING safely.
        // Some non-Windows servers (macOS smbd) still struggle with it, so we default
        // to buffered for Unknown/FAT32/exFAT unless force_ssd is set.
        bool use_no_buffering = force_ssd;
        wchar_t share_root[MAX_PATH_EXTENDED] = {};
        if (path[0] == L'\\' && path[1] == L'\\') {
            const wchar_t* p = path + 2;
            while (*p && *p != L'\\' && *p != L'/') p++;
            if (*p) p++;
            while (*p && *p != L'\\' && *p != L'/') p++;
            size_t len = static_cast<size_t>(p - path);
            wmemcpy(share_root, path, len);
            share_root[len] = L'\\';
            share_root[len + 1] = L'\0';
        } else if (path[1] == L':') {
            share_root[0] = path[0];
            share_root[1] = L':';
            share_root[2] = L'\\';
            share_root[3] = L'\0';
        }

        if (share_root[0] && !use_no_buffering) {
            wchar_t fs_name[32] = {};
            if (GetVolumeInformationW(share_root, nullptr, 0, nullptr, nullptr, nullptr, fs_name, 32)) {
                if (_wcsicmp(fs_name, L"NTFS") == 0 || _wcsicmp(fs_name, L"ReFS") == 0) {
                    use_no_buffering = true;
                }
            }
        }

        if (use_no_buffering) {
            // Use NO_BUFFERING to bypass local cache.
            // When force_ssd is requested, we also add WRITE_THROUGH to push through
            // server-side write-back caches (like on some high-end NAS).
            flags_ = FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING;
            if (force_ssd) flags_ |= FILE_FLAG_WRITE_THROUGH;
        } else {
            // Non-Windows / Legacy SMB: drop both NO_BUFFERING and WRITE_THROUGH.
            // Let the Windows SMB client cache writes and flush to server asynchronously.
            flags_ = FILE_FLAG_OVERLAPPED;
        }
    } else {
        // Local: bypass cache for maximum throughput on SSD/NVMe
        // When force_ssd is set, still use NO_BUFFERING|WRITE_THROUGH.
        flags_ = FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH;
    }
    wcsncpy(path_, path, MAX_PATH_EXTENDED - 1);
    count_ = count;
    robin_.store(0);

    handles_ = static_cast<HANDLE*>(
        HeapAlloc(GetProcessHeap(), HEAP_ZERO_MEMORY, count * sizeof(HANDLE)));
    if (!handles_) return false;

    for (int i = 0; i < count; i++) {
        handles_[i] = CreateFileW(
            path, GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE,
            nullptr, OPEN_EXISTING, flags_, nullptr);

        if (handles_[i] == INVALID_HANDLE_VALUE) {
            lc_error(L"ConnPool: failed to open write handle %d/%d to '%s': %u",
                     i + 1, count, path, GetLastError());
            close();
            return false;
        }

        if (!CreateIoCompletionPort(handles_[i], iocp, key, 0)) {
            lc_error(L"ConnPool: failed to associate write handle %d with IOCP: %u",
                     i, GetLastError());
            close();
            return false;
        }
    }

    return true;
}

HANDLE ConnPool::next() {
    uint32_t idx = robin_.fetch_add(1, std::memory_order_relaxed) % count_;
    return handles_[idx];
}

HANDLE ConnPool::at(int index) const {
    if (index < 0 || index >= count_) return INVALID_HANDLE_VALUE;
    return handles_[index];
}

bool ConnPool::reopen(HANDLE iocp, ULONG_PTR key) {
    lc_warn(L"ConnPool: reopening %d %s handles to '%s'...",
            count_, is_read_ ? L"read" : L"write", path_);

    // Cancel and close all existing handles
    for (int i = 0; i < count_; i++) {
        if (handles_[i] != INVALID_HANDLE_VALUE) {
            CancelIo(handles_[i]);
            CloseHandle(handles_[i]);
            handles_[i] = INVALID_HANDLE_VALUE;
        }
    }

    Sleep(1000); // brief pause for network recovery

    DWORD access = is_read_ ? GENERIC_READ : GENERIC_WRITE;
    DWORD creation = OPEN_EXISTING;

    for (int i = 0; i < count_; i++) {
        handles_[i] = CreateFileW(
            path_, access, FILE_SHARE_READ | FILE_SHARE_WRITE,
            nullptr, creation, flags_, nullptr);

        if (handles_[i] == INVALID_HANDLE_VALUE) {
            lc_error(L"ConnPool: reopen failed for handle %d: %u", i, GetLastError());
            return false;
        }

        CreateIoCompletionPort(handles_[i], iocp, key, 0);
    }

    robin_.store(0);
    lc_log(L"ConnPool: all %d handles reopened successfully", count_);
    return true;
}

void ConnPool::flush() {
    if (handles_) {
        for (int i = 0; i < count_; i++) {
            if (handles_[i] != INVALID_HANDLE_VALUE) {
                FlushFileBuffers(handles_[i]);
            }
        }
    }
}

void ConnPool::close() {
    if (handles_) {
        for (int i = 0; i < count_; i++) {
            if (handles_[i] != INVALID_HANDLE_VALUE) {
                CloseHandle(handles_[i]);
                handles_[i] = INVALID_HANDLE_VALUE;
            }
        }
        HeapFree(GetProcessHeap(), 0, handles_);
        handles_ = nullptr;
    }
    count_ = 0;
}

// ═══════════════════════════════════════════════════════════════════════════
// Adaptive Inflight Controller
// ═══════════════════════════════════════════════════════════════════════════

void AdaptiveInflight::init(int initial, int min_val, int max_val) {
    target_.store(initial);
    min_ = min_val;
    max_ = max_val;
    bytes_window_.store(0);
    QueryPerformanceFrequency(&freq_);
    QueryPerformanceCounter(&window_start_);
    prev_throughput_ = 0.0;
    direction_ = 1;
    stable_count_ = 0;
    stall_count_ = 0;
    best_target_ = initial;
}

void AdaptiveInflight::on_chunk_complete(uint32_t bytes) {
    bytes_window_.fetch_add(bytes, std::memory_order_relaxed);
}

int AdaptiveInflight::tick() {
    LARGE_INTEGER now;
    QueryPerformanceCounter(&now);

    double elapsed = static_cast<double>(now.QuadPart - window_start_.QuadPart) /
                     static_cast<double>(freq_.QuadPart);

    // Measure every ~2 seconds — fast enough to react to SMB stalls,
    // slow enough to avoid fighting TCP's own congestion control (~RTT scale).
    if (elapsed < 2.0) return target_.load();

    uint64_t bytes = bytes_window_.exchange(0, std::memory_order_relaxed);
    double throughput = static_cast<double>(bytes) / elapsed;

    window_start_ = now;

    int current = target_.load();

    // ── Gentle AIMD — complement TCP, don't duplicate it ──
    // TCP already has per-RTT congestion control.  This adaptive controller
    // only guards against application-level stalls (SMB server overwhelm).
    // Cuts are deliberately gentle to avoid compounding with TCP's AIMD.

    if (throughput < 1.0 && prev_throughput_ > 1.0) {
        // ── STALL DETECTED (near-zero throughput) ──
        stall_count_++;
        if (stall_count_ >= 3) {
            // Only cut after 3 consecutive near-zero ticks (~6 seconds)
            current = current * 5 / 6; // -17% (gentle)
            if (current < min_) current = min_;
            direction_ = -1;
            stable_count_ = 0;
        }
    } else if (prev_throughput_ > 0.0) {
        double improvement = (throughput - prev_throughput_) / prev_throughput_;

        if (improvement < -0.40) {
            // Severe throughput drop (>40%) - likely server struggling.
            stall_count_++;
            if (stall_count_ >= 3) {
                current = current * 85 / 100;  // -15% (was -25%)
                if (current < min_) current = min_;
                direction_ = -1;
                stable_count_ = 0;
            }
        } else if (improvement > 0.03) {
            // Throughput improved - switch back to ramp-up mode
            stable_count_ = 0;
            stall_count_ = 0;
            direction_ = 1;

            if (current > best_target_) best_target_ = current;
            if (current < max_) {
                int step = current / 4; // +25% (was +20%)
                if (step < 1) step = 1;
                current += step;
                if (current > max_) current = max_;
            }
        } else if (improvement < -0.10) {
            // Moderate throughput drop - nudge down gently
            stable_count_ = 0;
            direction_ = -1;
            if (current > min_) {
                int step = current / 20; // -5% (was -10%)
                if (step < 1) step = 1;
                current -= step;
                if (current < min_) current = min_;
            }
        } else {
            // Stable - probe up more ambitiously if far below best_target_
            stall_count_ = 0;
            if (current > best_target_) best_target_ = current;
            stable_count_++;
            if (stable_count_ >= 2 && current < max_) {
                // Faster probing (every 2 stable ticks instead of 3)
                int inc = 1;
                if (current < best_target_ * 3 / 4) inc = current / 8;
                if (inc < 1) inc = 1;

                current += inc;
                stable_count_ = 0;
                direction_ = 1;
            }
        }
    } else {
        // First measurement - moderate ramp-up
        stall_count_ = 0;
        if (current < max_) {
            int step = current / 2;
            if (step < 2) step = 2;
            current += step;
            if (current > max_) current = max_;
        }
    }

    prev_throughput_ = throughput;
    target_.store(current);
    return current;
}

void AdaptiveInflight::force_reduce(int new_target) {
    if (new_target < min_) new_target = min_;
    target_.store(new_target);
    // Don't permanently lower max_ — allow full recovery after transient stalls.
    // TCP already handles congestion; the adaptive controller should only guard
    // against application-level SMB server overwhelm, not compound TCP's AIMD.
    direction_ = -1;
    stable_count_ = 0;
    stall_count_++;
    prev_throughput_ = 0.0;  // reset measurement baseline
}

// ═══════════════════════════════════════════════════════════════════════════
// RTT Measurement
// ═══════════════════════════════════════════════════════════════════════════

RTTResult measure_rtt(const wchar_t* remote_path, uint64_t link_speed_bps, uint32_t chunk_size) {
    RTTResult result = {};
    result.rtt_ms = 0;
    result.suggested_inflight = DEFAULT_INFLIGHT;
    result.suggested_connections = DEFAULT_CONNECTIONS;

    // Measure RTT by timing a small synchronous file read
    HANDLE h = CreateFileW(remote_path, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE,
                            nullptr, OPEN_EXISTING,
                            FILE_FLAG_NO_BUFFERING, nullptr);
    if (h == INVALID_HANDLE_VALUE) return result;

    // Allocate a sector-aligned buffer for one sector
    uint8_t* buf = static_cast<uint8_t*>(
        VirtualAlloc(nullptr, SECTOR_SIZE, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE));
    if (!buf) { CloseHandle(h); return result; }

    // Do 5 reads and take the median
    LARGE_INTEGER freq, start, end;
    QueryPerformanceFrequency(&freq);

    double times[5] = {};
    int valid = 0;

    for (int i = 0; i < 5; i++) {
        // Seek back to start
        LARGE_INTEGER zero = {};
        SetFilePointerEx(h, zero, nullptr, FILE_BEGIN);

        QueryPerformanceCounter(&start);

        DWORD bytes_read = 0;
        ReadFile(h, buf, SECTOR_SIZE, &bytes_read, nullptr);

        QueryPerformanceCounter(&end);

        if (bytes_read > 0) {
            times[valid++] = static_cast<double>(end.QuadPart - start.QuadPart) /
                             static_cast<double>(freq.QuadPart) * 1000.0;
        }
    }

    VirtualFree(buf, 0, MEM_RELEASE);
    CloseHandle(h);

    if (valid == 0) return result;

    // Simple insertion sort for median
    for (int i = 1; i < valid; i++) {
        double key = times[i];
        int j = i - 1;
        while (j >= 0 && times[j] > key) {
            times[j + 1] = times[j];
            j--;
        }
        times[j + 1] = key;
    }

    result.rtt_ms = times[valid / 2]; // median

    // Calculate BDP: bandwidth * RTT
    double link_bytes_per_sec = static_cast<double>(link_speed_bps) / 8.0;
    result.bdp_bytes = link_bytes_per_sec * (result.rtt_ms / 1000.0);

    // Suggest inflight: enough to fill the pipe
    // Each inflight chunk = chunk_size bytes on the wire.
    // CRITICAL: must use actual chunk_size, not DEFAULT_CHUNK_SIZE!
    // With 1MB chunks on a 1Gbps/10ms link, BDP = 1.25 MB → need ~3 inflight.
    // With 50MB default you'd get 0.025 → inflight would be 1. Completely wrong.
    uint32_t cs = chunk_size > 0 ? chunk_size : DEFAULT_CHUNK_SIZE;
    double chunks_to_fill = result.bdp_bytes / static_cast<double>(cs);
    result.suggested_inflight = static_cast<int>(chunks_to_fill * 2.0); // 2x overcommit
    if (result.suggested_inflight < WAN_MIN_INFLIGHT)
        result.suggested_inflight = WAN_MIN_INFLIGHT;
    if (result.suggested_inflight > WAN_MAX_INFLIGHT)
        result.suggested_inflight = WAN_MAX_INFLIGHT;

    // Suggest connections: 1 per ~4 inflight, min 2
    result.suggested_connections = result.suggested_inflight / 4;
    if (result.suggested_connections < 2) result.suggested_connections = 2;
    if (result.suggested_connections > MAX_CONNECTIONS)
        result.suggested_connections = MAX_CONNECTIONS;

    return result;
}

// ═══════════════════════════════════════════════════════════════════════════
// Sparse Range Detection
// ═══════════════════════════════════════════════════════════════════════════

bool query_allocated_ranges(HANDLE file_handle, uint64_t file_size,
                            AllocRange** out_ranges, uint32_t* out_count) {
    *out_ranges = nullptr;
    *out_count = 0;

    // Input: the entire file range
    FILE_ALLOCATED_RANGE_BUFFER query = {};
    query.FileOffset.QuadPart = 0;
    query.Length.QuadPart = static_cast<LONGLONG>(file_size);

    // Start with space for 1024 ranges
    DWORD buf_size = 1024 * sizeof(FILE_ALLOCATED_RANGE_BUFFER);
    auto* output = static_cast<FILE_ALLOCATED_RANGE_BUFFER*>(
        HeapAlloc(GetProcessHeap(), 0, buf_size));
    if (!output) return false;

    DWORD bytes_returned = 0;
    BOOL ok = DeviceIoControl(
        file_handle, FSCTL_QUERY_ALLOCATED_RANGES,
        &query, sizeof(query),
        output, buf_size,
        &bytes_returned, nullptr);

    if (!ok && GetLastError() != ERROR_MORE_DATA) {
        HeapFree(GetProcessHeap(), 0, output);
        return false; // Not a sparse file or not supported
    }

    uint32_t count = bytes_returned / sizeof(FILE_ALLOCATED_RANGE_BUFFER);
    if (count == 0) {
        HeapFree(GetProcessHeap(), 0, output);
        return false;
    }

    // Convert to our AllocRange format
    auto* ranges = static_cast<AllocRange*>(
        HeapAlloc(GetProcessHeap(), 0, count * sizeof(AllocRange)));
    if (!ranges) {
        HeapFree(GetProcessHeap(), 0, output);
        return false;
    }

    for (uint32_t i = 0; i < count; i++) {
        ranges[i].offset = static_cast<uint64_t>(output[i].FileOffset.QuadPart);
        ranges[i].length = static_cast<uint64_t>(output[i].Length.QuadPart);
    }

    HeapFree(GetProcessHeap(), 0, output);
    *out_ranges = ranges;
    *out_count = count;
    return true;
}

uint32_t mark_sparse_chunks(ChunkRecord* records, uint32_t chunk_count,
                            uint32_t chunk_size, uint64_t /*file_size*/,
                            const AllocRange* ranges, uint32_t range_count) {
    if (!ranges || range_count == 0) return 0;

    uint32_t sparse_count = 0;

    for (uint32_t ci = 0; ci < chunk_count; ci++) {
        uint64_t chunk_start = static_cast<uint64_t>(ci) * chunk_size;
        uint64_t chunk_end = chunk_start + records[ci].length;

        // Check if this chunk overlaps any allocated range
        bool has_data = false;
        for (uint32_t ri = 0; ri < range_count; ri++) {
            uint64_t range_end = ranges[ri].offset + ranges[ri].length;
            // Overlap test
            if (chunk_start < range_end && chunk_end > ranges[ri].offset) {
                has_data = true;
                break;
            }
        }

        if (!has_data && records[ci].state == ChunkState::Pending) {
            records[ci].state = ChunkState::Sparse;
            records[ci].hash_lo = 0;
            records[ci].hash_hi = 0;
            sparse_count++;
        }
    }

    return sparse_count;
}

// ═══════════════════════════════════════════════════════════════════════════
// Delta Pre-Scan
// ═══════════════════════════════════════════════════════════════════════════

uint32_t delta_prescan(const wchar_t* dest_path, ChunkRecord* records,
                       uint32_t chunk_count, uint32_t chunk_size, bool verbose) {
    // Check if destination file exists
    DWORD attr = GetFileAttributesW(dest_path);
    if (attr == INVALID_FILE_ATTRIBUTES) return 0;

    // Open destination for reading
    HANDLE h = CreateFileW(dest_path, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE,
                            nullptr, OPEN_EXISTING,
                            FILE_FLAG_NO_BUFFERING | FILE_FLAG_SEQUENTIAL_SCAN, nullptr);
    if (h == INVALID_HANDLE_VALUE) return 0;

    // Get destination file size
    LARGE_INTEGER dest_size;
    GetFileSizeEx(h, &dest_size);

    uint32_t aligned_chunk = align_up(chunk_size, SECTOR_SIZE);
    uint8_t* buf = static_cast<uint8_t*>(
        VirtualAlloc(nullptr, aligned_chunk, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE));
    if (!buf) { CloseHandle(h); return 0; }

    uint32_t matched = 0;

    for (uint32_t i = 0; i < chunk_count; i++) {
        // Only check pending chunks that have a stored hash
        if (records[i].state != ChunkState::Pending) continue;
        if (records[i].hash_lo == 0 && records[i].hash_hi == 0) continue;

        uint64_t offset = records[i].offset;
        uint32_t len = records[i].length;
        uint32_t aligned_len = records[i].aligned_length;

        // Make sure destination is large enough
        if (offset + len > static_cast<uint64_t>(dest_size.QuadPart)) continue;

        // Seek and read
        LARGE_INTEGER seek;
        seek.QuadPart = static_cast<LONGLONG>(offset);
        SetFilePointerEx(h, seek, nullptr, FILE_BEGIN);

        DWORD bytes_read = 0;
        if (!ReadFile(h, buf, aligned_len, &bytes_read, nullptr)) continue;
        if (bytes_read < len) continue;

        // Hash the destination chunk
        XXH128_hash_t hash = XXH3_128bits(buf, len);

        // Compare with stored source hash
        if (hash.low64 == records[i].hash_lo && hash.high64 == records[i].hash_hi) {
            records[i].state = ChunkState::DeltaMatch;
            matched++;
            if (verbose) {
                lc_log(L"  Delta: chunk %u matches (offset 0x%llX)", i, offset);
            }
        }
    }

    VirtualFree(buf, 0, MEM_RELEASE);
    CloseHandle(h);

    return matched;
}
