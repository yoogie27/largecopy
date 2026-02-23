// largecopy - engine.cpp - IOCP pipeline engine (v3: auto-detection)


#include "engine.h"
#include "console.h"
#include "privilege.h"

#define XXH_INLINE_ALL
#define XXH_STATIC_LINKING_ONLY
#include "xxhash.h"

static constexpr DWORD SYNC_WRITE_TIMEOUT_MS = 15000; // 15s per write guard for sync path

// ── Helper: build ledger path from destination ──────────────────────────────
// For remote destinations, store the ledger locally to avoid memory-mapped I/O
// over SMB (which causes background flushes that interfere with data writes).
void CopyEngine::build_ledger_path(const wchar_t* dest, wchar_t* ledger, size_t len) {
    bool remote = (dest[0] == L'\\' && dest[1] == L'\\');
    if (!remote && dest[1] == L':') {
        wchar_t root[4] = { dest[0], L':', L'\\', L'\0' };
        remote = (GetDriveTypeW(root) == DRIVE_REMOTE);
    }

    if (remote) {
        // Build local ledger path: %TEMP%\largecopy\<hash_of_dest>.lcledger
        wchar_t temp_dir[MAX_PATH_EXTENDED];
        DWORD tlen = GetTempPathW(MAX_PATH_EXTENDED, temp_dir);
        if (tlen == 0) {
            // Fallback
            swprintf(ledger, len, L"%s.lcledger", dest);
            return;
        }

        // Create largecopy subdirectory
        wchar_t lc_dir[MAX_PATH_EXTENDED];
        swprintf(lc_dir, MAX_PATH_EXTENDED, L"%slargecopy", temp_dir);
        CreateDirectoryW(lc_dir, nullptr);  // ignore error if exists

        // Hash the destination path for a unique filename
        // Simple hash: sum of chars * position
        uint64_t h = 0;
        for (int i = 0; dest[i]; i++) {
            h = h * 31 + dest[i];
        }
        swprintf(ledger, len, L"%s\\%016llX.lcledger", lc_dir, h);
    } else {
        swprintf(ledger, len, L"%s.lcledger", dest);
    }
}

// ── IOCP worker thread entry ────────────────────────────────────────────────
DWORD WINAPI CopyEngine::iocp_worker_entry(LPVOID param) {
    auto* engine = static_cast<CopyEngine*>(param);
    engine->iocp_worker_loop();
    return 0;
}

// ── Hash completion callback (called from hash thread) ──────────────────────
void CopyEngine::on_hash_complete(ChunkContext* ctx, void* user_data) {
    auto* engine = static_cast<CopyEngine*>(user_data);

    // ── Synchronous write path (network destinations) ───────────────────
    // Writes are done right here in the hash thread. This avoids all async
    // I/O complexity with SMB and prevents overwhelming the server.
    if (engine->sync_writes_) {
        // Acquire write slot (blocks until available)
        if (engine->write_sem_) {
            WaitForSingleObject(engine->write_sem_, INFINITE);
        }

        ctx->phase = ChunkPhase::Write;
        engine->stats_.writes_outstanding.fetch_add(1, std::memory_order_relaxed);
        HANDLE dst = engine->dst_handle_;
        DWORD  write_len = ctx->aligned_length;
        BOOL   ok = FALSE;
        int    max_retries = engine->cfg_->retries;

        for (int attempt = 0; attempt <= max_retries; attempt++) {
            OVERLAPPED ov = {};
            ov.Offset     = static_cast<DWORD>(ctx->file_offset & 0xFFFFFFFF);
            ov.OffsetHigh = static_cast<DWORD>(ctx->file_offset >> 32);

            DWORD written = 0;
            ok = WriteFile(dst, ctx->buffer, write_len, nullptr, &ov);
            DWORD err = ok ? ERROR_SUCCESS : GetLastError();

            if (!ok && err == ERROR_IO_PENDING) {
                if (GetOverlappedResultEx(dst, &ov, &written, SYNC_WRITE_TIMEOUT_MS, TRUE)) {
                    ok = TRUE;
                } else {
                    err = GetLastError();
                    if (err == WAIT_TIMEOUT || err == ERROR_OPERATION_ABORTED) {
                        CancelIoEx(dst, &ov);
                    }
                }
            }

            if (ok) break;

            // Network disconnection - try to reopen
            if (err == ERROR_NETNAME_DELETED || err == ERROR_BAD_NETPATH ||
                err == ERROR_UNEXP_NET_ERR || err == ERROR_SEM_TIMEOUT) {
                if (engine->dst_handle_ != INVALID_HANDLE_VALUE) {
                    CancelIo(engine->dst_handle_);
                    CloseHandle(engine->dst_handle_);
                }
                engine->dst_handle_ = CreateFileW(
                    engine->ledger_->header()->dest_path,
                    GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE,
                    nullptr, OPEN_EXISTING,
                    FILE_FLAG_OVERLAPPED | FILE_FLAG_SEQUENTIAL_SCAN,
                    nullptr);
                if (engine->dst_handle_ == INVALID_HANDLE_VALUE) {
                    lc_error(L"Failed to reopen destination after disconnect");
                    break;
                }
                dst = engine->dst_handle_;
            }

            if (attempt < max_retries) {
                DWORD backoff = 100u << attempt;
                if (backoff > 10000) backoff = 10000;
                lc_warn(L"Chunk %u write error %u (retry %d/%d)",
                        ctx->chunk_index, err, attempt + 1, max_retries);
                engine->stats_.retry_count.fetch_add(1, std::memory_order_relaxed);
                Sleep(backoff);
            }
        }

        // Release write slot
        if (engine->write_sem_) {
            ReleaseSemaphore(engine->write_sem_, 1, nullptr);
        }

        if (ok) {
            // Success - update ledger & stats (same as on_write_complete)
            if (engine->cfg_->no_checksum) {
                engine->ledger_->mark_verified(ctx->chunk_index, 0, 0);
            } else {
                engine->ledger_->mark_verified(ctx->chunk_index, ctx->hash_lo, ctx->hash_hi);
            }

            engine->stats_.bytes_transferred.fetch_add(ctx->data_length, std::memory_order_relaxed);
            engine->stats_.chunks_verified.fetch_add(1, std::memory_order_relaxed);

            if (engine->use_adaptive_) {
                engine->adaptive_.on_chunk_complete(ctx->data_length);
            }

            if (engine->cfg_->verbose) {
                console_queue_msg(L"  Chunk %u verified (offset 0x%llX, %u bytes)",
                                  ctx->chunk_index, ctx->file_offset, ctx->data_length);
            }
        } else {
            // Failed after all retries
            lc_error(L"Chunk %u write failed after %d retries",
                     ctx->chunk_index, max_retries);
            engine->ledger_->mark_failed(ctx->chunk_index);
            engine->stats_.chunks_failed.fetch_add(1, std::memory_order_relaxed);
        }

        // Return buffer and update inflight
        engine->stats_.writes_outstanding.fetch_sub(1, std::memory_order_relaxed);
        engine->buffer_pool_.release(ctx->buffer);
        InterlockedDecrement(&engine->inflight_count_);
        engine->stats_.current_inflight.store(engine->inflight_count_, std::memory_order_relaxed);

        // Pump more reads
        engine->pump_reads();

        // Check if done
        if (engine->ledger_->all_done()) {
            SetEvent(engine->done_event_);
        }

        HeapFree(GetProcessHeap(), 0, ctx);
        return;
    }

    // ── Async write path (local destinations via IOCP) ──────────────────
    // Acquire write slot (throttles concurrent async writes)
    if (engine->write_sem_) {
        WaitForSingleObject(engine->write_sem_, INFINITE);
    }

    memset(&ctx->overlapped, 0, sizeof(OVERLAPPED));
    ctx->overlapped.Offset     = static_cast<DWORD>(ctx->file_offset & 0xFFFFFFFF);
    ctx->overlapped.OffsetHigh = static_cast<DWORD>(ctx->file_offset >> 32);
    ctx->phase = ChunkPhase::Write;

    // Pick a destination handle
    HANDLE dst;
    if (engine->dst_pool_.count() > 0) {
        dst = engine->dst_pool_.next();
    } else {
        dst = engine->dst_handle_;
    }

    engine->stats_.writes_outstanding.fetch_add(1, std::memory_order_relaxed);
    BOOL ok = WriteFile(dst, ctx->buffer, ctx->aligned_length,
                        nullptr, &ctx->overlapped);
    if (!ok && GetLastError() != ERROR_IO_PENDING) {
        engine->on_io_error(ctx, GetLastError());
    } else {
        // Pipeline optimization: try to submit more reads now rather than
        // waiting for the write to complete.  Safe because pump_reads()
        // checks inflight < target and uses try_acquire (non-blocking).
        engine->pump_reads();
    }
}

// ── Submit a read for one chunk ─────────────────────────────────────────────
bool CopyEngine::submit_read(uint32_t chunk_index) {
    const ChunkRecord* rec = ledger_->chunk(chunk_index);
    if (!rec) return false;

    uint8_t* buf = buffer_pool_.try_acquire();
    if (!buf) {
        // Chunk was claimed by find_next_pending (Pending→Reading).
        // Restore it so it can be picked up on the next pump_reads() call.
        ledger_->mark_state(chunk_index, ChunkState::Pending);
        return false;
    }

    auto* ctx = static_cast<ChunkContext*>(
        HeapAlloc(GetProcessHeap(), HEAP_ZERO_MEMORY, sizeof(ChunkContext)));
    if (!ctx) {
        buffer_pool_.release(buf);
        ledger_->mark_state(chunk_index, ChunkState::Pending);
        return false;
    }

    ctx->chunk_index   = chunk_index;
    ctx->buffer        = buf;
    ctx->data_length   = rec->length;
    ctx->aligned_length = rec->aligned_length;
    ctx->file_offset   = rec->offset;
    ctx->phase         = ChunkPhase::Read;
    ctx->hash_lo       = 0;
    ctx->hash_hi       = 0;
    ctx->conn_index    = -1;
    ctx->buffered_tail = (rec->length != rec->aligned_length);

    ctx->overlapped.Offset     = static_cast<DWORD>(rec->offset & 0xFFFFFFFF);
    ctx->overlapped.OffsetHigh = static_cast<DWORD>(rec->offset >> 32);

    InterlockedIncrement(&inflight_count_);
    stats_.current_inflight.store(inflight_count_, std::memory_order_relaxed);

    // Pick a source handle from pool (buffered tail uses dedicated handle)
    HANDLE src;
    DWORD  read_len = rec->aligned_length;
    if (ctx->buffered_tail) {
        read_len = rec->length; // true data length, may be unaligned
        if (tail_read_handle_ == INVALID_HANDLE_VALUE) {
            tail_read_handle_ = CreateFileW(
                cfg_->source, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE,
                nullptr, OPEN_EXISTING,
                FILE_FLAG_OVERLAPPED | FILE_FLAG_SEQUENTIAL_SCAN, nullptr);
            if (tail_read_handle_ == INVALID_HANDLE_VALUE) {
                lc_error(L"Tail read handle open failed: %u", GetLastError());
            } else {
                CreateIoCompletionPort(tail_read_handle_, iocp_, IOCP_KEY_READ, 0);
            }
        }
        src = tail_read_handle_;
        // If we couldn't open a buffered tail handle, fall back to primary
        if (src == INVALID_HANDLE_VALUE) {
            src = (src_pool_.count() > 0) ? src_pool_.next() : src_handle_;
        }
    } else {
        if (src_pool_.count() > 0) {
            src = src_pool_.next();
        } else {
            src = src_handle_;
        }
    }

    BOOL ok = ReadFile(src, buf, read_len, nullptr, &ctx->overlapped);
    if (!ok && GetLastError() != ERROR_IO_PENDING) {
        DWORD err = GetLastError();
        InterlockedDecrement(&inflight_count_);
        stats_.current_inflight.store(inflight_count_, std::memory_order_relaxed);
        buffer_pool_.release(buf);
        HeapFree(GetProcessHeap(), 0, ctx);
        lc_error(L"ReadFile failed for chunk %u: %u", chunk_index, err);
        // Restore chunk to pending so it can be retried
        ledger_->mark_state(chunk_index, ChunkState::Pending);
        return false;
    }

    return true;
}

// ── Handle completed read ───────────────────────────────────────────────────
void CopyEngine::on_read_complete(ChunkContext* ctx, DWORD bytes_transferred) {
    // Tail chunk read via buffered handle may not be aligned; pad to aligned_length
    if (ctx->buffered_tail) {
        if (bytes_transferred < ctx->data_length) {
            // Short read - requeue
            ledger_->mark_state(ctx->chunk_index, ChunkState::Pending);
            buffer_pool_.release(ctx->buffer);
            InterlockedDecrement(&inflight_count_);
            stats_.current_inflight.store(inflight_count_, std::memory_order_relaxed);
            pump_reads();
            HeapFree(GetProcessHeap(), 0, ctx);
            return;
        }
        if (ctx->aligned_length > ctx->data_length) {
            memset(ctx->buffer + ctx->data_length, 0,
                   ctx->aligned_length - ctx->data_length);
        }
    }

    if (cfg_->no_checksum && !sync_writes_) {
        // Fast path: no hash, async write - call directly from IOCP thread
        on_hash_complete(ctx, this);
    } else {
        // Enqueue to hash thread pool. When sync_writes_, this is mandatory
        // because the callback blocks on synchronous writes - must not block
        // IOCP threads which handle read completions.
        ctx->phase = ChunkPhase::Hash;
        if (!hash_pool_.enqueue(ctx)) {
            // Allocation failed — restore chunk so it can be retried
            ledger_->mark_state(ctx->chunk_index, ChunkState::Pending);
            buffer_pool_.release(ctx->buffer);
            InterlockedDecrement(&inflight_count_);
            stats_.current_inflight.store(inflight_count_, std::memory_order_relaxed);
            HeapFree(GetProcessHeap(), 0, ctx);
            pump_reads();
            return;
        }
    }
}

// ── Handle completed write (async path only - sync writes handled in on_hash_complete)
void CopyEngine::on_write_complete(ChunkContext* ctx, DWORD /*bytes_transferred*/) {
    stats_.writes_outstanding.fetch_sub(1, std::memory_order_relaxed);

    // Release write throttle slot (async mode only)
    if (write_sem_ && !sync_writes_) {
        ReleaseSemaphore(write_sem_, 1, nullptr);
    }

    // Update ledger
    if (cfg_->no_checksum) {
        ledger_->mark_verified(ctx->chunk_index, 0, 0);
    } else {
        ledger_->mark_verified(ctx->chunk_index, ctx->hash_lo, ctx->hash_hi);
    }

    // Update stats
    stats_.bytes_transferred.fetch_add(ctx->data_length, std::memory_order_relaxed);
    stats_.chunks_verified.fetch_add(1, std::memory_order_relaxed);

    if (use_adaptive_) {
        adaptive_.on_chunk_complete(ctx->data_length);
    }

    if (cfg_->verbose) {
        console_queue_msg(L"  Chunk %u verified (offset 0x%llX, %u bytes)",
                          ctx->chunk_index, ctx->file_offset, ctx->data_length);
    }

    // Return buffer to pool
    buffer_pool_.release(ctx->buffer);
    InterlockedDecrement(&inflight_count_);
    stats_.current_inflight.store(inflight_count_, std::memory_order_relaxed);

    // Pump more reads up to target inflight
    pump_reads();

    // Check if done
    if (ledger_->all_done()) {
        SetEvent(done_event_);
    }

    HeapFree(GetProcessHeap(), 0, ctx);
}

// ── Pump reads: submit more reads up to target inflight ─────────────────────
void CopyEngine::pump_reads() {
    int target = use_adaptive_ ? adaptive_.target() : cfg_->inflight;

    while (inflight_count_ < target) {
        int next = ledger_->find_next_pending();
        if (next < 0) break;
        if (!submit_read(static_cast<uint32_t>(next))) break;
    }
}

// ── Handle I/O error with retry ─────────────────────────────────────────────
void CopyEngine::on_io_error(ChunkContext* ctx, DWORD error_code) {
    // Release write throttle slot if this was an async write operation
    // (sync writes handle their own semaphore in on_hash_complete)
    if (ctx->phase == ChunkPhase::Write) {
        stats_.writes_outstanding.fetch_sub(1, std::memory_order_relaxed);
        if (write_sem_ && !sync_writes_) {
            ReleaseSemaphore(write_sem_, 1, nullptr);
        }
    }

    // Intentional cancellation (stall recovery) - silently re-queue, no retry
    if (error_code == ERROR_OPERATION_ABORTED) {
        ledger_->mark_state(ctx->chunk_index, ChunkState::Pending);
        buffer_pool_.release(ctx->buffer);
        InterlockedDecrement(&inflight_count_);
        stats_.current_inflight.store(inflight_count_, std::memory_order_relaxed);
        pump_reads();
        HeapFree(GetProcessHeap(), 0, ctx);
        return;
    }

    ledger_->increment_retry(ctx->chunk_index);
    stats_.retry_count.fetch_add(1, std::memory_order_relaxed);

    const ChunkRecord* rec = ledger_->chunk(ctx->chunk_index);
    int max_retries = cfg_->retries;

    if (rec && rec->retry_count < static_cast<uint8_t>(max_retries)) {
        lc_warn(L"Chunk %u I/O error %u (retry %u/%d)",
                ctx->chunk_index, error_code, rec->retry_count, max_retries);

        // Exponential backoff
        DWORD backoff = 100u << (rec->retry_count - 1);
        if (backoff > 10000) backoff = 10000;
        Sleep(backoff);

        // Handle disconnection
        if (error_code == ERROR_NETNAME_DELETED ||
            error_code == ERROR_BAD_NETPATH ||
            error_code == ERROR_UNEXP_NET_ERR ||
            error_code == ERROR_SEM_TIMEOUT) {
            reopen_handles();
        }

        // Re-queue the chunk
        ledger_->mark_state(ctx->chunk_index, ChunkState::Pending);
        buffer_pool_.release(ctx->buffer);
        InterlockedDecrement(&inflight_count_);
        stats_.current_inflight.store(inflight_count_, std::memory_order_relaxed);

        // Pump reads
        pump_reads();
    } else {
        lc_error(L"Chunk %u failed after %d retries (error %u)",
                 ctx->chunk_index, max_retries, error_code);
        ledger_->mark_failed(ctx->chunk_index);
        stats_.chunks_failed.fetch_add(1, std::memory_order_relaxed);

        buffer_pool_.release(ctx->buffer);
        InterlockedDecrement(&inflight_count_);
        stats_.current_inflight.store(inflight_count_, std::memory_order_relaxed);

        if (ledger_->count_pending() == 0 && inflight_count_ == 0) {
            SetEvent(done_event_);
        }
    }

    HeapFree(GetProcessHeap(), 0, ctx);
}

// ── IOCP worker loop ────────────────────────────────────────────────────────
void CopyEngine::iocp_worker_loop() {
    for (;;) {
        DWORD bytes = 0;
        ULONG_PTR key = 0;
        LPOVERLAPPED overlapped = nullptr;

        BOOL ok = GetQueuedCompletionStatus(iocp_, &bytes, &key, &overlapped, INFINITE);

        if (key == IOCP_KEY_QUIT) break;
        if (!overlapped) continue;

        auto* ctx = reinterpret_cast<ChunkContext*>(overlapped);

        if (!ok) {
            DWORD err = GetLastError();
            on_io_error(ctx, err);
            continue;
        }

        switch (ctx->phase) {
            case ChunkPhase::Read:
                on_read_complete(ctx, bytes);
                break;
            case ChunkPhase::Write:
                on_write_complete(ctx, bytes);
                break;
            case ChunkPhase::VerifyRead:
                {
                    XXH128_hash_t hash = XXH3_128bits(ctx->buffer, ctx->data_length);
                    const ChunkRecord* rec = ledger_->chunk(ctx->chunk_index);
                    if (rec && (hash.low64 != rec->hash_lo || hash.high64 != rec->hash_hi)) {
                        lc_error(L"VERIFY MISMATCH chunk %u at offset 0x%llX!",
                                 ctx->chunk_index, ctx->file_offset);
                        stats_.chunks_failed.fetch_add(1, std::memory_order_relaxed);
                    } else {
                        stats_.chunks_verified.fetch_add(1, std::memory_order_relaxed);
                    }
                    stats_.bytes_transferred.fetch_add(ctx->data_length, std::memory_order_relaxed);
                    buffer_pool_.release(ctx->buffer);
                    InterlockedDecrement(&inflight_count_);

                    // Submit next verify read
                    int next = ledger_->find_next_pending();
                    if (next >= 0) {
                        const ChunkRecord* nrec = ledger_->chunk(static_cast<uint32_t>(next));
                        uint8_t* buf = buffer_pool_.acquire();
                        auto* nctx = static_cast<ChunkContext*>(
                            HeapAlloc(GetProcessHeap(), HEAP_ZERO_MEMORY, sizeof(ChunkContext)));
                        nctx->chunk_index = static_cast<uint32_t>(next);
                        nctx->buffer = buf;
                        nctx->data_length = nrec->length;
                        nctx->aligned_length = nrec->aligned_length;
                        nctx->file_offset = nrec->offset;
                        nctx->phase = ChunkPhase::VerifyRead;
                        nctx->overlapped.Offset = static_cast<DWORD>(nrec->offset & 0xFFFFFFFF);
                        nctx->overlapped.OffsetHigh = static_cast<DWORD>(nrec->offset >> 32);
                        InterlockedIncrement(&inflight_count_);

                        HANDLE src = (src_pool_.count() > 0) ? src_pool_.next() : src_handle_;
                        ReadFile(src, buf, nrec->aligned_length, nullptr, &nctx->overlapped);
                    }

                    if (stats_.chunks_verified.load() + stats_.chunks_failed.load() >=
                        stats_.total_chunks) {
                        SetEvent(done_event_);
                    }
                    HeapFree(GetProcessHeap(), 0, ctx);
                }
                break;
            default:
                break;
        }
    }
}

// ── Reopen handles after network error ──────────────────────────────────────
bool CopyEngine::reopen_handles() {
    lc_warn(L"Attempting to reopen file handles...");

    bool ok = true;
    if (src_pool_.count() > 0) {
        ok = src_pool_.reopen(iocp_, IOCP_KEY_READ) && ok;
    } else if (src_handle_ != INVALID_HANDLE_VALUE) {
        CancelIo(src_handle_);
        CloseHandle(src_handle_);
        Sleep(1000);
        DWORD rflags = FILE_FLAG_OVERLAPPED | FILE_FLAG_SEQUENTIAL_SCAN;
        if (!cfg_->buffered && !env_.source.is_remote) rflags |= FILE_FLAG_NO_BUFFERING;
        src_handle_ = CreateFileW(
            cfg_->source, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE,
            nullptr, OPEN_EXISTING,
            rflags,
            nullptr);
        if (src_handle_ != INVALID_HANDLE_VALUE)
            CreateIoCompletionPort(src_handle_, iocp_, IOCP_KEY_READ, 0);
        else ok = false;
    }

    if (sync_writes_) {
        // Sync write mode: reopen non-overlapped handle (no IOCP)
        if (dst_handle_ != INVALID_HANDLE_VALUE) {
            CloseHandle(dst_handle_);
            Sleep(1000);
            dst_handle_ = CreateFileW(
                cfg_->dest, GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE,
                nullptr, OPEN_EXISTING,
                FILE_FLAG_OVERLAPPED | FILE_FLAG_SEQUENTIAL_SCAN,
                nullptr);
            if (dst_handle_ == INVALID_HANDLE_VALUE) ok = false;
        }
    } else if (dst_pool_.count() > 0) {
        ok = dst_pool_.reopen(iocp_, IOCP_KEY_WRITE) && ok;
    } else if (dst_handle_ != INVALID_HANDLE_VALUE) {
        CancelIo(dst_handle_);
        CloseHandle(dst_handle_);
        Sleep(1000);
        DWORD rflags = FILE_FLAG_OVERLAPPED;
        if (!cfg_->buffered) rflags |= FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH;
        dst_handle_ = CreateFileW(
            cfg_->dest, GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE,
            nullptr, OPEN_EXISTING,
            rflags, nullptr);
        if (dst_handle_ != INVALID_HANDLE_VALUE)
            CreateIoCompletionPort(dst_handle_, iocp_, IOCP_KEY_WRITE, 0);
        else ok = false;
    }

    // Reopen buffered tail read handle if present
    if (tail_read_handle_ != INVALID_HANDLE_VALUE) {
        CancelIo(tail_read_handle_);
        CloseHandle(tail_read_handle_);
        tail_read_handle_ = CreateFileW(
            cfg_->source, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE,
            nullptr, OPEN_EXISTING,
            FILE_FLAG_OVERLAPPED | FILE_FLAG_SEQUENTIAL_SCAN,
            nullptr);
        if (tail_read_handle_ != INVALID_HANDLE_VALUE) {
            CreateIoCompletionPort(tail_read_handle_, iocp_, IOCP_KEY_READ, 0);
        } else {
            ok = false;
        }
    }

    return ok;
}

// ── Core transfer execution ─────────────────────────────────────────────────
int CopyEngine::execute_transfer(const Config& cfg, Ledger& ledger) {
    cfg_    = &cfg;
    ledger_ = &ledger;

    const LedgerHeader* hdr = ledger.header();
    if (!hdr || hdr->chunk_count == 0) {
        lc_log(L"Nothing to transfer (0 chunks)");
        return 0;
    }

    // ── Thread counts ──
    SYSTEM_INFO si;
    GetSystemInfo(&si);

    io_thread_count_ = cfg.io_threads;
    if (io_thread_count_ == 0) {
        io_thread_count_ = static_cast<int>(si.dwNumberOfProcessors);
        if (io_thread_count_ > 16) io_thread_count_ = 16;
    }

    int hash_thread_count = static_cast<int>(si.dwNumberOfProcessors) / 2;
    if (hash_thread_count < 2) hash_thread_count = 2;

    // ── Buffer pool ──
    uint32_t aligned_chunk = align_up(hdr->chunk_size, SECTOR_SIZE);
    int pool_size = cfg.inflight;
    if (cfg.adaptive) {
        // Pool must cover the full adaptive range to prevent deadlock:
        // pump_reads() calls submit_read() in a loop up to the adaptive target,
        // and buffer_pool_.acquire() would block if the pool is too small,
        // stalling hash/IOCP threads that need to complete writes to free buffers.
        int amax = cfg.adaptive_max > 0 ? cfg.adaptive_max : WAN_MAX_INFLIGHT;
        pool_size = amax;
        if (pool_size > 128) pool_size = 128;
    }
    // Never exceed what we can fit in ~75% of available RAM
    MEMORYSTATUSEX meminfo = {};
    meminfo.dwLength = sizeof(meminfo);
    if (GlobalMemoryStatusEx(&meminfo) && meminfo.ullAvailPhys > 0) {
        int max_by_ram = static_cast<int>((meminfo.ullAvailPhys * 3 / 4) / aligned_chunk);
        if (max_by_ram < 4) max_by_ram = 4;
        if (pool_size > max_by_ram) pool_size = max_by_ram;
    }
    if (!buffer_pool_.init(pool_size, aligned_chunk)) {
        return 1;
    }

    // ── Create IOCP ──
    iocp_ = CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, io_thread_count_);
    if (!iocp_) {
        lc_error(L"Failed to create IOCP: %u", GetLastError());
        return 1;
    }

    // ── Detect if destination is remote ──
    int conn_count = cfg.connections;
    use_adaptive_ = cfg.adaptive;

    bool dst_remote = env_.dest.is_remote;

    // Default to the fastest async path. Only use synchronous writes if 
    // the user explicitly requests 'safe' mode for network destinations.
    // Sync mode helps with some Non-Windows SMB servers (Samba, macOS) 
    // that may stall under heavy overlapped I/O.
    sync_writes_ = cfg.force_safe_net && dst_remote;

    // User override: --ssd always forces async path (no sync fallback)
    if (cfg.force_ssd) {
        sync_writes_ = false;
    }

    // ── Open source handles (always async via IOCP for fast reads) ──
    // For remote sources: use buffered I/O so the SMB redirector can do
    // sequential read-ahead.  NO_BUFFERING on SMB reads bypasses the
    // client-side cache, killing prefetch and causing pipeline stalls.
    bool src_remote = env_.source.is_remote;
    bool src_buffered = cfg.buffered || src_remote;

    if (conn_count > 1) {
        if (!src_pool_.open_read(hdr->source_path, conn_count, iocp_, IOCP_KEY_READ, src_buffered)) {
            lc_error(L"Failed to open source connection pool");
            return 1;
        }
    } else {
        DWORD rflags = FILE_FLAG_OVERLAPPED | FILE_FLAG_SEQUENTIAL_SCAN;
        if (!src_buffered) rflags |= FILE_FLAG_NO_BUFFERING;
        
        src_handle_ = CreateFileW(
            hdr->source_path, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE,
            nullptr, OPEN_EXISTING, rflags, nullptr);
        if (src_handle_ == INVALID_HANDLE_VALUE) {
            lc_error(L"Cannot open source file '%s': %u", hdr->source_path, GetLastError());
            return 1;
        }
        CreateIoCompletionPort(src_handle_, iocp_, IOCP_KEY_READ, 0);
    }

    // ── Open destination handle ──
    if (sync_writes_) {
        // Network destination: single non-overlapped handle for synchronous
        // positioned writes. NOT associated with IOCP - writes happen directly
        // in hash threads, gated by write_sem_.
        DWORD wflags = FILE_FLAG_OVERLAPPED | FILE_FLAG_SEQUENTIAL_SCAN;
        dst_handle_ = CreateFileW(
            hdr->dest_path, GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE,
            nullptr, OPEN_EXISTING, wflags, nullptr);
        if (dst_handle_ == INVALID_HANDLE_VALUE) {
            lc_error(L"Cannot open destination file '%s': %u", hdr->dest_path, GetLastError());
            return 1;
        }
        stats_.connections = 1;
    } else if (conn_count > 1) {
        // Local multi-connection: async overlapped via IOCP
        if (!dst_pool_.open_write(hdr->dest_path, conn_count, iocp_, IOCP_KEY_WRITE, cfg.force_ssd, cfg.buffered)) {
            lc_error(L"Failed to open destination connection pool");
            return 1;
        }
        stats_.connections = conn_count;
    } else {
        // Local single-connection: async overlapped via IOCP
        DWORD wflags = FILE_FLAG_OVERLAPPED;
        if (!cfg.buffered) {
            wflags |= FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH;
        }
        
        // For remote single-handle, match ConnPool logic:
        if (dst_remote && !cfg.buffered) {
            bool use_nb = cfg.force_ssd;
            if (env_.dest.fs_type == FsType::NTFS || env_.dest.fs_type == FsType::ReFS) {
                use_nb = true;
            }
            if (!use_nb) {
                wflags = FILE_FLAG_OVERLAPPED; // buffered for non-Windows remote
            } else if (!cfg.force_ssd) {
                wflags = FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING; // no write-through for network by default
            } else {
                wflags = FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH; // force_ssd = full performance
            }
        }
        
        dst_handle_ = CreateFileW(
            hdr->dest_path, GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE,
            nullptr, OPEN_EXISTING, wflags, nullptr);
        if (dst_handle_ == INVALID_HANDLE_VALUE) {
            lc_error(L"Cannot open destination file '%s': %u", hdr->dest_path, GetLastError());
            return 1;
        }
        CreateIoCompletionPort(dst_handle_, iocp_, IOCP_KEY_WRITE, 0);
        stats_.connections = 1;
    }

    // ── SMB compression (try, verify, fall back gracefully) ──
    if (cfg.compress) {
        bool comp_ok = false;
        if (conn_count > 1) {
            // Try on first handle - if it fails, the server doesn't support it
            comp_ok = enable_smb_compression(dst_pool_.at(0));
            if (comp_ok) {
                for (int i = 1; i < dst_pool_.count(); i++)
                    enable_smb_compression(dst_pool_.at(i));
            }
        } else if (dst_handle_ != INVALID_HANDLE_VALUE) {
            comp_ok = enable_smb_compression(dst_handle_);
        }
        if (comp_ok) {
            lc_log(L"SMB compression enabled on destination");
        } else if (!cfg.quiet) {
            lc_log(L"SMB compression not supported by server (continuing without)");
        }
    }

    // ── TCP stats (network transfers only) ──
    // src_remote and dst_remote already defined above
    bool any_remote = src_remote || dst_remote;

    if (any_remote && net_profile_.server_name[0]) {
        if (netstats_init(net_profile_.server_name)) {
            stats_.net_stats_active = true;
            NetStats ns = {};
            netstats_sample(ns);
            prev_net_stats_ = ns;
        }
    }

    // ── Create done event ──
    done_event_ = CreateEventW(nullptr, TRUE, FALSE, nullptr);
    InitializeCriticalSection(&submit_cs_);

    // ── Adaptive inflight ──
    if (use_adaptive_) {
        int amax = cfg.adaptive_max > 0 ? cfg.adaptive_max : WAN_MAX_INFLIGHT;
        adaptive_.init(cfg.inflight, WAN_MIN_INFLIGHT, amax);
    }

    // ── Write throttle ──
    // For sync writes: exactly 1 writer at a time on the single non-overlapped
    // handle. Multiple threads calling WriteFile on a non-overlapped handle is
    // undefined behavior - the semaphore ensures mutual exclusion.
    if (sync_writes_) {
        write_sem_ = CreateSemaphoreW(nullptr, 1, 1, nullptr);
        if (!cfg.quiet) {
            lc_log(L"Sync write mode: sequential writes to network destination");
        }
    } else if (cfg.max_writes > 0) {
        write_sem_ = CreateSemaphoreW(nullptr, cfg.max_writes, cfg.max_writes, nullptr);
    }

    // ── Start hash thread pool ──
    // Always start when sync_writes_: hash threads also handle synchronous writes
    // and must not run on IOCP threads (which would block read completions).
    if (!cfg.no_checksum || sync_writes_) {
        if (!hash_pool_.start(hash_thread_count, on_hash_complete, this)) {
            return 1;
        }
    }

    // ── Start IOCP worker threads ──
    io_threads_ = static_cast<HANDLE*>(
        HeapAlloc(GetProcessHeap(), HEAP_ZERO_MEMORY, io_thread_count_ * sizeof(HANDLE)));
    for (int i = 0; i < io_thread_count_; i++) {
        io_threads_[i] = CreateThread(nullptr, 0, iocp_worker_entry, this, 0, nullptr);
    }

    // ── Initialize stats ──
    stats_.total_bytes  = hdr->source_size;
    stats_.total_chunks = hdr->chunk_count;
    stats_.bytes_transferred.store(0);
    stats_.chunks_verified.store(ledger.count_verified());
    stats_.chunks_skipped.store(ledger.count_skipped());
    stats_.chunks_failed.store(0);
    stats_.retry_count.store(0);
    QueryPerformanceCounter(&stats_.start_time);

    // Account for already-done bytes (resume + sparse + delta)
    uint64_t already_done = 0;
    uint64_t skip_bytes   = 0;
    for (uint32_t i = 0; i < hdr->chunk_count; i++) {
        ChunkState st = ledger.chunk(i)->state;
        if (st == ChunkState::Verified || st == ChunkState::Sparse ||
            st == ChunkState::DeltaMatch) {
            already_done += ledger.chunk(i)->length;
        }
        if (st == ChunkState::Sparse || st == ChunkState::DeltaMatch) {
            skip_bytes += ledger.chunk(i)->length;
        }
    }
    stats_.bytes_transferred.store(already_done);
    stats_.bytes_skipped = skip_bytes;
    stats_.resume_bytes  = already_done;

    // ── Submit initial batch of reads ──
    int initial = use_adaptive_ ? adaptive_.target() : cfg.inflight;
    if (static_cast<uint32_t>(initial) > hdr->chunk_count)
        initial = static_cast<int>(hdr->chunk_count);

    for (int i = 0; i < initial; i++) {
        int next = ledger.find_next_pending();
        if (next < 0) break;
        submit_read(static_cast<uint32_t>(next));
    }

    // If nothing to do
    if (inflight_count_ == 0 && ledger.count_pending() == 0) {
        SetEvent(done_event_);
    }

    // ── Main thread: progress display + adaptive tuning + stall recovery ──
    extern volatile long g_abort;
    uint64_t stall_last_bytes = stats_.bytes_transferred.load();
    int      stall_ticks = 0;
    int      stall_recovery_count = 0;

    while (WaitForSingleObject(done_event_, 250) == WAIT_TIMEOUT) {
        if (!cfg.quiet) {
            print_progress(stats_);
        }

        // Check for Ctrl+C abort
        if (g_abort) {
            stats_.aborted.store(true, std::memory_order_relaxed);
            lc_warn(L"Aborting transfer - flushing ledger...");
            // Cancel pending async read I/O
            if (src_pool_.count() > 0) {
                for (int ci = 0; ci < src_pool_.count(); ci++) CancelIoEx(src_pool_.at(ci), nullptr);
            } else if (src_handle_ != INVALID_HANDLE_VALUE) {
                CancelIoEx(src_handle_, nullptr);
            }
            if (tail_read_handle_ != INVALID_HANDLE_VALUE) {
                CancelIoEx(tail_read_handle_, nullptr);
            }
            // Cancel pending async write I/O (only for non-sync mode)
            if (!sync_writes_) {
                if (dst_pool_.count() > 0) {
                    for (int ci = 0; ci < dst_pool_.count(); ci++) CancelIoEx(dst_pool_.at(ci), nullptr);
                } else if (dst_handle_ != INVALID_HANDLE_VALUE) {
                    CancelIoEx(dst_handle_, nullptr);
                }
            }
            // Wake main loop immediately
            SetEvent(done_event_);
            break;
        }

        // Adaptive: adjust inflight every tick
        if (use_adaptive_) {
            int new_target = adaptive_.tick();
            stats_.current_inflight.store(inflight_count_, std::memory_order_relaxed);
            // If target increased, pump more reads
            if (inflight_count_ < new_target) {
                pump_reads();
            }
        }

        // ── TCP stats sampling ────────────────────────────────────────────
        if (stats_.net_stats_active) {
            NetStats ns = {};
            netstats_sample(ns);

            stats_.net_retrans_delta = ns.retrans_pkts - prev_net_stats_.retrans_pkts;
            stats_.net_timeouts      = ns.timeouts;
            stats_.net_rtt_ms        = ns.rtt_ms;
            stats_.net_cwnd          = ns.cwnd;
            stats_.net_rwin_sent     = ns.rwin_cur << ns.rcv_win_scale;
            stats_.net_out_of_order  = ns.rcv_pkts_out_order - prev_net_stats_.rcv_pkts_out_order;

            // Per-tick bottleneck percentages
            uint64_t dr = ns.lim_rwin_ms - prev_net_stats_.lim_rwin_ms;
            uint64_t dc = ns.lim_cwnd_ms - prev_net_stats_.lim_cwnd_ms;
            uint64_t ds = ns.lim_sender_ms - prev_net_stats_.lim_sender_ms;
            uint64_t total_lim = dr + dc + ds;

            if (total_lim > 0) {
                stats_.net_lim_rwin_pct   = static_cast<uint32_t>((dr * 100) / total_lim);
                stats_.net_lim_cwnd_pct   = static_cast<uint32_t>((dc * 100) / total_lim);
                stats_.net_lim_sender_pct = static_cast<uint32_t>((ds * 100) / total_lim);
            } else {
                stats_.net_lim_rwin_pct   = 0;
                stats_.net_lim_cwnd_pct   = 0;
                stats_.net_lim_sender_pct = 0;
            }

            // Accumulate cumulative totals for final summary
            stats_.net_total_lim_rwin_ms   += dr;
            stats_.net_total_lim_cwnd_ms   += dc;
            stats_.net_total_lim_sender_ms += ds;
            stats_.net_total_retrans       = ns.retrans_pkts;
            stats_.net_total_out_of_order  = ns.rcv_pkts_out_order;
            stats_.net_conn_count          = ns.conn_count;
            stats_.net_sample_count++;
            stats_.net_rtt_sum += ns.rtt_ms;

            // Track MSS (keep min/max across all samples)
            if (ns.mss_min > 0) {
                bool first_mss = (stats_.net_mss_min == 0);
                if (first_mss || ns.mss_min < stats_.net_mss_min)
                    stats_.net_mss_min = ns.mss_min;
                if (ns.mss_max > stats_.net_mss_max)
                    stats_.net_mss_max = ns.mss_max;

                // One-time verbose warning for VPN/tunnel clamping
                if (first_mss && ns.mss_min < 1400) {
                    console_queue_msg(L"  \x1b[93mMSS: %u bytes (VPN/tunnel clamping detected, normal=1460)\x1b[0m",
                                      ns.mss_min);
                }
            }

            // Verbose: emit a line when bottleneck type changes
            if (cfg_->verbose) {
                int cur_bottleneck = 0;
                if (total_lim > 0) {
                    if (stats_.net_lim_rwin_pct > 50)        cur_bottleneck = 1;
                    else if (stats_.net_lim_cwnd_pct > 50)   cur_bottleneck = 2;
                    else if (stats_.net_lim_sender_pct > 50)  cur_bottleneck = 3;
                }
                if (cur_bottleneck != prev_bottleneck_) {
                    switch (cur_bottleneck) {
                    case 1:
                        console_queue_msg(L"  \x1b[93mTCP bottleneck: Receiver RWIN (%u%%)\x1b[0m  RTT %ums  RWIN %u KB",
                                          stats_.net_lim_rwin_pct, ns.rtt_ms, stats_.net_rwin_sent / 1024);
                        break;
                    case 2:
                        console_queue_msg(L"  \x1b[91mTCP bottleneck: Network CWND (%u%%)\x1b[0m  RTT %ums  CWND %u",
                                          stats_.net_lim_cwnd_pct, ns.rtt_ms, ns.cwnd);
                        break;
                    case 3:
                        console_queue_msg(L"  \x1b[96mTCP bottleneck: App/Disk (%u%%)\x1b[0m  RTT %ums",
                                          stats_.net_lim_sender_pct, ns.rtt_ms);
                        break;
                    default:
                        break;
                    }
                    prev_bottleneck_ = cur_bottleneck;
                }

                if (stats_.net_retrans_delta > 0) {
                    console_queue_msg(L"  \x1b[91mTCP retransmits: %u pkts\x1b[0m",
                                      stats_.net_retrans_delta);
                }
                if (stats_.net_out_of_order > 0) {
                    console_queue_msg(L"  \x1b[93mTCP out-of-order: %u pkts\x1b[0m",
                                      stats_.net_out_of_order);
                }
            }

            prev_net_stats_ = ns;
        }

        // ── Stall detection & recovery (async writes only) ────────────────
        // Not needed for sync_writes_ mode - writes are blocking and self-limiting.
        if (!sync_writes_) {
            uint64_t current_bytes = stats_.bytes_transferred.load();
            if (current_bytes == stall_last_bytes && inflight_count_ > 0) {
                stall_ticks++;
                if (stall_ticks >= 60) {  // 15 seconds of zero progress (at 250ms per tick)
                    stall_recovery_count++;
                    int cur = inflight_count_;

                    if (dst_pool_.count() > 0) {
                        for (int ci = 0; ci < dst_pool_.count(); ci++)
                            CancelIoEx(dst_pool_.at(ci), nullptr);
                    } else if (dst_handle_ != INVALID_HANDLE_VALUE) {
                        CancelIoEx(dst_handle_, nullptr);
                    }
                    if (src_pool_.count() > 0) {
                        for (int ci = 0; ci < src_pool_.count(); ci++)
                            CancelIoEx(src_pool_.at(ci), nullptr);
                    } else if (src_handle_ != INVALID_HANDLE_VALUE) {
                        CancelIoEx(src_handle_, nullptr);
                    }

                    if (use_adaptive_) {
                        int new_target = cur / 2;
                        if (new_target < WAN_MIN_INFLIGHT) new_target = WAN_MIN_INFLIGHT;
                        if (stall_recovery_count >= 3) new_target = WAN_MIN_INFLIGHT;
                        adaptive_.force_reduce(new_target);
                        console_queue_msg(L"\x1b[93mWARN:\x1b[0m  Write stall detected (inflight=%d) - target reduced to %d",
                                          cur, new_target);
                    }
                    stall_ticks = 0;
                    Sleep(500);
                }
            } else {
                stall_ticks = 0;
                stall_last_bytes = current_bytes;
                if (stall_recovery_count > 0) stall_recovery_count--;
            }
        }
    }

    // Final progress
    if (!cfg.quiet) {
        print_progress(stats_);
    }

    // ── Shutdown ──
    if (stats_.aborted.load() && cfg.fast_abort) {
        lc_warn(L"Fast abort: skipping thread join and ledger flush");
        // We still need to close handles to avoid leaks, but we don't wait for threads
        src_pool_.close();
        dst_pool_.close();
        if (src_handle_ != INVALID_HANDLE_VALUE) { CloseHandle(src_handle_); src_handle_ = INVALID_HANDLE_VALUE; }
        if (dst_handle_ != INVALID_HANDLE_VALUE) { CloseHandle(dst_handle_); dst_handle_ = INVALID_HANDLE_VALUE; }
        if (tail_read_handle_ != INVALID_HANDLE_VALUE) { CloseHandle(tail_read_handle_); tail_read_handle_ = INVALID_HANDLE_VALUE; }
        if (iocp_) { CloseHandle(iocp_); iocp_ = nullptr; }
        return 3;
    }

    for (int i = 0; i < io_thread_count_; i++) {
        PostQueuedCompletionStatus(iocp_, 0, IOCP_KEY_QUIT, nullptr);
    }
    WaitForMultipleObjects(io_thread_count_, io_threads_, TRUE, 2000); // reduced timeout from 5000

    if (!cfg.no_checksum) {
        hash_pool_.stop();
    }

    // Flush ledger (skip if aborted)
    if (!stats_.aborted.load()) {
        if (ledger.all_done()) {
            ledger.mark_completed();
        }
        ledger.flush();
    }

    // Flush destination data to stable storage (critical for remote targets
    // where write-back caching means data may still be in server RAM).
    if (env_.dest.is_remote && dst_handle_ != INVALID_HANDLE_VALUE) {
        FlushFileBuffers(dst_handle_);
    }
    if (env_.dest.is_remote) {
        dst_pool_.flush();
    }

    // Truncate destination to exact size if not sector-aligned
    if (hdr->source_size % SECTOR_SIZE != 0) {
        // Close all handles first
        src_pool_.close();
        dst_pool_.close();
        if (dst_handle_ != INVALID_HANDLE_VALUE) {
            CloseHandle(dst_handle_);
            dst_handle_ = INVALID_HANDLE_VALUE;
        }
        if (src_handle_ != INVALID_HANDLE_VALUE) {
            CloseHandle(src_handle_);
            src_handle_ = INVALID_HANDLE_VALUE;
        }

        HANDLE h = CreateFileW(hdr->dest_path, GENERIC_WRITE, FILE_SHARE_READ,
                                nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (h != INVALID_HANDLE_VALUE) {
            LARGE_INTEGER li;
            li.QuadPart = static_cast<LONGLONG>(hdr->source_size);
            SetFilePointerEx(h, li, nullptr, FILE_BEGIN);
            SetEndOfFile(h);
            CloseHandle(h);
        }
    }

    // Cleanup
    for (int i = 0; i < io_thread_count_; i++) {
        if (io_threads_[i]) CloseHandle(io_threads_[i]);
    }
    HeapFree(GetProcessHeap(), 0, io_threads_);
    io_threads_ = nullptr;

    src_pool_.close();
    dst_pool_.close();
    if (src_handle_ != INVALID_HANDLE_VALUE) { CloseHandle(src_handle_); src_handle_ = INVALID_HANDLE_VALUE; }
    if (dst_handle_ != INVALID_HANDLE_VALUE) { CloseHandle(dst_handle_); dst_handle_ = INVALID_HANDLE_VALUE; }
    if (tail_read_handle_ != INVALID_HANDLE_VALUE) { CloseHandle(tail_read_handle_); tail_read_handle_ = INVALID_HANDLE_VALUE; }
    if (iocp_) { CloseHandle(iocp_); iocp_ = nullptr; }
    if (write_sem_) { CloseHandle(write_sem_); write_sem_ = nullptr; }
    if (done_event_) { CloseHandle(done_event_); done_event_ = nullptr; }
    netstats_cleanup();
    DeleteCriticalSection(&submit_cs_);
    buffer_pool_.destroy();

    if (!cfg.quiet) {
        print_summary(stats_);
    }

    return (stats_.chunks_failed.load() > 0) ? 2 : 0;
}

// ── Copy command ────────────────────────────────────────────────────────────
int CopyEngine::run_copy(const Config& cfg) {
    // Get source file size and timestamp (for change detection)
    WIN32_FILE_ATTRIBUTE_DATA src_attr = {};
    if (!GetFileAttributesExW(cfg.source, GetFileExInfoStandard, &src_attr)) {
        lc_error(L"Cannot access source file '%s': %u", cfg.source, GetLastError());
        return 1;
    }
    uint64_t source_size = (static_cast<uint64_t>(src_attr.nFileSizeHigh) << 32) |
                            src_attr.nFileSizeLow;
    FILETIME source_mtime_before = src_attr.ftLastWriteTime;

    if (source_size == 0) {
        lc_warn(L"Source file is empty - creating empty destination");
        HANDLE h = CreateFileW(cfg.dest, GENERIC_WRITE, 0, nullptr, CREATE_ALWAYS,
                                FILE_ATTRIBUTE_NORMAL, nullptr);
        if (h != INVALID_HANDLE_VALUE) CloseHandle(h);
        return 0;
    }

    // ── Auto-resume from existing ledger ──────────────────────────────────
    // If the destination already exists and we're not forcing a fresh copy,
    // look for a valid ledger from a previous interrupted transfer and
    // resume automatically. This is the most efficient path: already-verified
    // chunks are skipped with zero I/O (no re-read, no re-hash, no re-write).
    if (!cfg.force) {
        DWORD dest_attr = GetFileAttributesW(cfg.dest);
        if (dest_attr != INVALID_FILE_ATTRIBUTES) {
            wchar_t probe_ledger[MAX_PATH_EXTENDED];
            build_ledger_path(cfg.dest, probe_ledger, MAX_PATH_EXTENDED);

            Ledger probe;
            bool can_resume = false;

            if (probe.open(probe_ledger)) {
                const LedgerHeader* hdr = probe.header();

                // Validate: same source size + same source path identity
                if (hdr->source_size == source_size) {
                    XXH128_hash_t ph = XXH3_128bits(
                        cfg.source, wcslen(cfg.source) * sizeof(wchar_t));
                    if (ph.low64 == hdr->source_path_hash)
                        can_resume = true;
                }

                if (can_resume && hdr->completed) {
                    probe.close();
                    if (!cfg.quiet) {
                        lc_log(L"Transfer already completed. Nothing to do.");
                        lc_log(L"Use --force to re-copy from scratch.");
                    }
                    return 0;
                }
                probe.close();
            }

            if (can_resume) {
                // Delegate to the resume path (it reopens the ledger,
                // auto-configures, and runs execute_transfer)
                if (!cfg.quiet) {
                    lc_log(L"Found valid ledger for interrupted transfer - resuming automatically.\n");
                }
                Config resume_cfg = cfg;
                wcsncpy(resume_cfg.source, cfg.dest, MAX_PATH_EXTENDED - 1);
                int result = run_resume(resume_cfg);

                // Post-resume: verify + ledger cleanup (run_resume doesn't do these)
                if (result == 0) {
                    Ledger done_check;
                    if (done_check.open(probe_ledger)) {
                        bool all_ok = done_check.all_done();
                        done_check.close();

                        if (all_ok) {
                            if (cfg.verify_after) {
                                if (!cfg.quiet) lc_log(L"\nRunning post-copy verification...");
                                Config verify_cfg = cfg;
                                verify_cfg.command = Command::Verify;
                                result = run_verify(verify_cfg);
                            }
                            if (result == 0) {
                                DeleteFileW(probe_ledger);
                                if (!cfg.quiet) lc_log(L"Ledger removed (transfer complete)");
                            }
                        }
                    }
                }
                return result;
            }

            // Dest exists but no valid ledger - can't resume safely
            lc_error(L"Destination '%s' already exists. Use --force to overwrite.", cfg.dest);
            return 1;
        }
    }

    // ── Fresh copy ──────────────────────────────────────────────────────────

    // Detect full environment (storage, system, network)
    EnvironmentProfile env;
    detect_environment(cfg.source, cfg.dest, env);
    env_ = env;
    net_profile_ = env.network;

    // Auto-configure based on detected environment
    Config tuned = auto_configure(cfg, env);

    // WAN mode: measure RTT (informational + raise values if BDP demands more)
    if (tuned.wan_mode && env.network.is_remote) {
        const wchar_t* remote_path = env.dest.is_remote ? tuned.dest : tuned.source;

        // Pass link speed for comparison; measure_rtt will probe actual throughput
        uint64_t bw = env.network.effective_bw_bps > 0
                     ? env.network.effective_bw_bps : env.network.link_speed_bps;
        RTTResult rtt = measure_rtt(remote_path, bw, tuned.chunk_size);
        if (rtt.rtt_ms > 0) {
            if (!cfg.quiet) {
                wchar_t bdp_buf[64], bw_buf[64];
                format_bytes(static_cast<uint64_t>(rtt.bdp_bytes), bdp_buf, 64);
                format_rate(rtt.measured_bw_bps / 8.0, bw_buf, 64);
                lc_log(L"RTT:     %.1f ms | BDP: %s | Path: %s", rtt.rtt_ms, bdp_buf, bw_buf);
            }

            // Apply RTT-based suggestions — but only when the user didn't
            // explicitly set the value via CLI.  User overrides are sacred.
            if (!cfg.inflight_user_set && rtt.suggested_inflight != tuned.inflight) {
                tuned.inflight = rtt.suggested_inflight;
            }
            if (!cfg.connections_user_set) {
                if (rtt.suggested_connections < tuned.connections) {
                    tuned.connections = rtt.suggested_connections;
                } else if (rtt.suggested_connections > tuned.connections) {
                    tuned.connections = rtt.suggested_connections;
                }
            }
        }
    }

    // Acquire privilege
    bool have_priv = acquire_volume_privilege();
    if (!have_priv && !cfg.quiet) {
        lc_warn(L"Could not acquire SE_MANAGE_VOLUME_NAME - SetFileValidData unavailable");
        lc_warn(L"Run as Administrator for faster pre-allocation");
    }

    // Print config
    if (!cfg.quiet) {
        print_banner();
        print_environment(env);
        print_network_profile(net_profile_);
        print_config(tuned, source_size,
                     static_cast<uint32_t>((source_size + tuned.chunk_size - 1) / tuned.chunk_size),
                     have_priv);
    }

    if (cfg.dry_run) {
        lc_log(L"Dry run - no files modified.");
        return 0;
    }

    // Pre-allocate destination (skip for non-Windows remote - SetEndOfFile on
    // some SMB servers causes them to zero-fill asynchronously which
    // interferes with our writes)
    bool dest_remote = env_.dest.is_remote;
    bool dest_windows = (env_.dest.fs_type == FsType::NTFS || env_.dest.fs_type == FsType::ReFS);

    if (dest_remote && !dest_windows) {
        // For non-Windows remote: just create/truncate the file, let writes extend it
        if (!cfg.quiet) lc_log(L"Creating destination file (remote - no pre-allocation)...");
        HANDLE h = CreateFileW(cfg.dest, GENERIC_WRITE, FILE_SHARE_READ,
                                nullptr, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (h == INVALID_HANDLE_VALUE) {
            lc_error(L"Cannot create destination file: %u", GetLastError());
            return 1;
        }
        CloseHandle(h);
    } else {
        if (!cfg.quiet) {
            if (dest_remote) lc_log(L"Pre-allocating destination (remote - Windows server detected)...");
            else lc_log(L"Pre-allocating destination...");
        }
        if (!preallocate_destination(cfg.dest, source_size, have_priv)) {
            return 1;
        }
    }

    // Create ledger
    wchar_t ledger_path[MAX_PATH_EXTENDED];
    build_ledger_path(cfg.dest, ledger_path, MAX_PATH_EXTENDED);

    Ledger ledger;
    if (!ledger.create(ledger_path, cfg.source, cfg.dest, source_size, tuned.chunk_size)) {
        return 1;
    }

    // ── Sparse detection ──
    if (cfg.sparse) {
        HANDLE src_h = CreateFileW(cfg.source, GENERIC_READ, FILE_SHARE_READ,
                                    nullptr, OPEN_EXISTING, 0, nullptr);
        if (src_h != INVALID_HANDLE_VALUE) {
            AllocRange* ranges = nullptr;
            uint32_t range_count = 0;
            if (query_allocated_ranges(src_h, source_size, &ranges, &range_count)) {
                uint32_t sparse_count = mark_sparse_chunks(
                    ledger.chunk(0), ledger.header()->chunk_count,
                    tuned.chunk_size, source_size, ranges, range_count);
                if (sparse_count > 0 && !cfg.quiet) {
                    wchar_t skip_buf[64];
                    format_bytes(static_cast<uint64_t>(sparse_count) * tuned.chunk_size, skip_buf, 64);
                    lc_log(L"Sparse:  %u zero-filled chunks skipped (~%s)", sparse_count, skip_buf);
                }
                HeapFree(GetProcessHeap(), 0, ranges);
            }
            CloseHandle(src_h);
        }
    }

    // ── Delta pre-scan ──
    if (cfg.delta) {
        if (!cfg.quiet) lc_log(L"Delta:   Scanning existing destination for matching chunks...");
        uint32_t matched = delta_prescan(cfg.dest, ledger.chunk(0),
                                          ledger.header()->chunk_count,
                                          tuned.chunk_size, cfg.verbose);
        if (matched > 0 && !cfg.quiet) {
            wchar_t skip_buf[64];
            format_bytes(static_cast<uint64_t>(matched) * tuned.chunk_size, skip_buf, 64);
            lc_log(L"Delta:   %u chunks already match (~%s skipped)", matched, skip_buf);
        }
    }

    if (!cfg.quiet) lc_log(L"Ledger:  %s\n", ledger_path);

    // Execute!
    int result = execute_transfer(tuned, ledger);

    // Check if source was modified during transfer
    if (result == 0) {
        WIN32_FILE_ATTRIBUTE_DATA src_attr_after = {};
        if (GetFileAttributesExW(cfg.source, GetFileExInfoStandard, &src_attr_after)) {
            if (CompareFileTime(&source_mtime_before, &src_attr_after.ftLastWriteTime) != 0) {
                lc_warn(L"WARNING: Source file was modified during transfer!");
                lc_warn(L"The destination may contain a mix of old and new data.");
                lc_warn(L"Re-run the copy with --force to ensure consistency.");
            }
        }
    }

    // Post-copy verification
    if (result == 0 && cfg.verify_after) {
        if (!cfg.quiet) lc_log(L"\nRunning post-copy verification...");
        ledger.close();  // release file lock so run_verify can reopen it
        Config verify_cfg = cfg;
        verify_cfg.command = Command::Verify;
        result = run_verify(verify_cfg);
    }

    // Clean up ledger on success
    // Note: cfg.verify_after short-circuits so all_done() is never called
    // on a closed ledger (header_ would be null after close).
    if (result == 0 && (cfg.verify_after || ledger.all_done())) {
        ledger.close();
        DeleteFileW(ledger_path);
        if (!cfg.quiet) lc_log(L"Ledger removed (transfer complete)");
    }

    return result;
}

// ── Resume command ──────────────────────────────────────────────────────────
int CopyEngine::run_resume(const Config& cfg) {
    wchar_t ledger_path[MAX_PATH_EXTENDED];
    const wchar_t* input = cfg.source;

    size_t len = wcslen(input);
    if (len > 10 && _wcsicmp(input + len - 10, L".lcledger") == 0) {
        wcsncpy(ledger_path, input, MAX_PATH_EXTENDED - 1);
    } else {
        build_ledger_path(input, ledger_path, MAX_PATH_EXTENDED);
    }

    Ledger ledger;
    if (!ledger.open(ledger_path)) {
        lc_error(L"Cannot open ledger. Is this a valid largecopy ledger file?");
        return 1;
    }

    const LedgerHeader* hdr = ledger.header();

    if (hdr->completed) {
        lc_log(L"Transfer already completed. Nothing to resume.");
        return 0;
    }

    ledger.reset_incomplete();

    uint32_t remaining = ledger.count_pending();
    if (remaining == 0) {
        lc_log(L"All chunks already verified. Marking complete.");
        ledger.mark_completed();
        return 0;
    }

    // Detect full environment
    EnvironmentProfile env;
    detect_environment(hdr->source_path, hdr->dest_path, env);
    env_ = env;
    net_profile_ = env.network;

    if (!cfg.quiet) {
        print_banner();
        lc_log(L"Resuming transfer: %u chunks remaining", remaining);
        lc_log(L"Source: %s", hdr->source_path);
        lc_log(L"Dest:   %s\n", hdr->dest_path);
        print_environment(env);
    }

    // Auto-configure based on detected environment
    Config tuned = auto_configure(cfg, env);
    wcsncpy(tuned.source, hdr->source_path, MAX_PATH_EXTENDED - 1);
    wcsncpy(tuned.dest, hdr->dest_path, MAX_PATH_EXTENDED - 1);
    tuned.chunk_size = hdr->chunk_size;

    // WAN mode: measure RTT and refine
    if (tuned.wan_mode && env.network.is_remote) {
        const wchar_t* remote_path = env.dest.is_remote ? hdr->dest_path : hdr->source_path;
        uint64_t bw = env.network.effective_bw_bps > 0
                     ? env.network.effective_bw_bps : env.network.link_speed_bps;
        RTTResult rtt = measure_rtt(remote_path, bw, tuned.chunk_size);
        if (rtt.rtt_ms > 0 && !cfg.quiet) {
            wchar_t bdp_buf[64], bw_buf[64];
            format_bytes(static_cast<uint64_t>(rtt.bdp_bytes), bdp_buf, 64);
            format_rate(rtt.measured_bw_bps / 8.0, bw_buf, 64);
            lc_log(L"RTT: %.1f ms | BDP: %s | Path: %s", rtt.rtt_ms, bdp_buf, bw_buf);
        }
        if (!cfg.inflight_user_set && rtt.suggested_inflight != tuned.inflight)
            tuned.inflight = rtt.suggested_inflight;
        if (!cfg.connections_user_set) {
            if (rtt.suggested_connections < tuned.connections)
                tuned.connections = rtt.suggested_connections;
            else if (rtt.suggested_connections > tuned.connections)
                tuned.connections = rtt.suggested_connections;
        }
    }

    return execute_transfer(tuned, ledger);
}

// ── Verify command ──────────────────────────────────────────────────────────
int CopyEngine::run_verify(const Config& cfg) {
    wchar_t ledger_path[MAX_PATH_EXTENDED];
    build_ledger_path(cfg.dest, ledger_path, MAX_PATH_EXTENDED);

    Ledger ledger;
    if (!ledger.open(ledger_path)) {
        lc_error(L"No ledger found for destination. Cannot verify without chunk hashes.");
        return 1;
    }

    const LedgerHeader* hdr = ledger.header();

    if (!cfg.quiet) {
        print_banner();
        lc_log(L"Verifying: %s", cfg.dest);
        lc_log(L"Against:   %s (%u chunks)\n", cfg.source, hdr->chunk_count);
    }

    // Reset verifiable chunks to pending (skip sparse/delta)
    for (uint32_t i = 0; i < hdr->chunk_count; i++) {
        ChunkRecord* rec = ledger.chunk(i);
        if (rec->state == ChunkState::Sparse || rec->state == ChunkState::DeltaMatch)
            continue;
        uint64_t h_lo = rec->hash_lo;
        uint64_t h_hi = rec->hash_hi;
        rec->state = ChunkState::Pending;
        rec->hash_lo = h_lo;
        rec->hash_hi = h_hi;
    }

    cfg_ = &cfg;
    ledger_ = &ledger;
    use_adaptive_ = false;

    SYSTEM_INFO si;
    GetSystemInfo(&si);
    io_thread_count_ = static_cast<int>(si.dwNumberOfProcessors);
    if (io_thread_count_ > 16) io_thread_count_ = 16;

    uint32_t aligned_chunk = align_up(hdr->chunk_size, SECTOR_SIZE);
    if (!buffer_pool_.init(cfg.inflight, aligned_chunk)) return 1;

    // Open destination for reading
    iocp_ = CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, io_thread_count_);

    int conn = cfg.connections;
    if (conn > 1) {
        if (!src_pool_.open_read(cfg.dest, conn, iocp_, IOCP_KEY_READ, cfg.buffered)) return 1;
    } else {
        DWORD rflags = FILE_FLAG_OVERLAPPED | FILE_FLAG_SEQUENTIAL_SCAN;
        if (!cfg.buffered) rflags |= FILE_FLAG_NO_BUFFERING;
        
        src_handle_ = CreateFileW(
            cfg.dest, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE,
            nullptr, OPEN_EXISTING,
            rflags, nullptr);
        if (src_handle_ == INVALID_HANDLE_VALUE) {
            lc_error(L"Cannot open destination for verification: %u", GetLastError());
            return 1;
        }
        CreateIoCompletionPort(src_handle_, iocp_, IOCP_KEY_READ, 0);
    }

    done_event_ = CreateEventW(nullptr, TRUE, FALSE, nullptr);
    InitializeCriticalSection(&submit_cs_);

    io_threads_ = static_cast<HANDLE*>(
        HeapAlloc(GetProcessHeap(), HEAP_ZERO_MEMORY, io_thread_count_ * sizeof(HANDLE)));
    for (int i = 0; i < io_thread_count_; i++) {
        io_threads_[i] = CreateThread(nullptr, 0, iocp_worker_entry, this, 0, nullptr);
    }

    // Count verifiable chunks (exclude sparse/delta)
    uint32_t verifiable = 0;
    for (uint32_t i = 0; i < hdr->chunk_count; i++) {
        if (ledger.chunk(i)->state == ChunkState::Pending) verifiable++;
    }

    stats_.total_bytes = hdr->source_size;
    stats_.total_chunks = verifiable;
    stats_.bytes_transferred.store(0);
    stats_.chunks_verified.store(0);
    stats_.chunks_failed.store(0);
    QueryPerformanceCounter(&stats_.start_time);

    // Submit initial verify reads
    int initial = cfg.inflight;
    for (int i = 0; i < initial; i++) {
        int next = ledger.find_next_pending();
        if (next < 0) break;

        const ChunkRecord* rec = ledger.chunk(static_cast<uint32_t>(next));
        uint8_t* buf = buffer_pool_.acquire();
        auto* ctx = static_cast<ChunkContext*>(
            HeapAlloc(GetProcessHeap(), HEAP_ZERO_MEMORY, sizeof(ChunkContext)));
        ctx->chunk_index = static_cast<uint32_t>(next);
        ctx->buffer = buf;
        ctx->data_length = rec->length;
        ctx->aligned_length = rec->aligned_length;
        ctx->file_offset = rec->offset;
        ctx->phase = ChunkPhase::VerifyRead;
        ctx->overlapped.Offset = static_cast<DWORD>(rec->offset & 0xFFFFFFFF);
        ctx->overlapped.OffsetHigh = static_cast<DWORD>(rec->offset >> 32);
        InterlockedIncrement(&inflight_count_);

        HANDLE src = (src_pool_.count() > 0) ? src_pool_.next() : src_handle_;
        ReadFile(src, buf, rec->aligned_length, nullptr, &ctx->overlapped);
    }

    {
        extern volatile long g_abort;
        while (WaitForSingleObject(done_event_, 250) == WAIT_TIMEOUT) {
            if (!cfg.quiet) print_progress(stats_);
            if (g_abort) {
                lc_warn(L"Verify aborted by user.");
                break;
            }
        }
    }
    if (!cfg.quiet) print_progress(stats_);

    // Cleanup
    for (int i = 0; i < io_thread_count_; i++) {
        PostQueuedCompletionStatus(iocp_, 0, IOCP_KEY_QUIT, nullptr);
    }
    WaitForMultipleObjects(io_thread_count_, io_threads_, TRUE, 5000);
    for (int i = 0; i < io_thread_count_; i++) {
        if (io_threads_[i]) CloseHandle(io_threads_[i]);
    }
    HeapFree(GetProcessHeap(), 0, io_threads_);
    src_pool_.close();
    if (src_handle_ != INVALID_HANDLE_VALUE) { CloseHandle(src_handle_); src_handle_ = INVALID_HANDLE_VALUE; }
    CloseHandle(iocp_); iocp_ = nullptr;
    CloseHandle(done_event_); done_event_ = nullptr;
    DeleteCriticalSection(&submit_cs_);
    buffer_pool_.destroy();

    uint32_t failed = stats_.chunks_failed.load();
    if (failed == 0) {
        if (!cfg.quiet)
            lc_log(L"\n\x1b[92mVerification PASSED\x1b[0m - all %u chunks match",
                   stats_.total_chunks);
        return 0;
    } else {
        lc_error(L"\nVerification FAILED - %u chunks mismatched!", failed);
        return 1;
    }
}

// ── Status command ──────────────────────────────────────────────────────────
int CopyEngine::run_status(const Config& cfg) {
    wchar_t ledger_path[MAX_PATH_EXTENDED];
    const wchar_t* input = cfg.source;

    size_t len = wcslen(input);
    if (len > 10 && _wcsicmp(input + len - 10, L".lcledger") == 0) {
        wcsncpy(ledger_path, input, MAX_PATH_EXTENDED - 1);
    } else {
        build_ledger_path(input, ledger_path, MAX_PATH_EXTENDED);
    }

    Ledger ledger;
    if (!ledger.open(ledger_path)) {
        lc_error(L"Cannot open ledger '%s'", ledger_path);
        return 1;
    }

    print_banner();
    print_ledger_status(ledger_path, ledger.header(),
                        ledger.count_verified(),
                        ledger.count_failed(),
                        ledger.count_pending());
    return 0;
}

// ── Benchmark command ───────────────────────────────────────────────────────
int CopyEngine::run_bench(const Config& cfg) {
    print_banner();
    lc_log(L"Benchmarking I/O to: %s", cfg.dest);

    // Detect environment for the bench target
    EnvironmentProfile env;
    detect_storage(cfg.dest, env.dest);
    detect_system(env.system);
    bool is_remote = env.dest.is_remote;

    if (is_remote) {
        probe_network(cfg.dest, env.network);
        lc_log(L"Target:  %s | %s",
               disk_type_str(env.dest.disk_type),
               env.dest.fs_name[0] ? env.dest.fs_name : L"?");
        if (env.network.link_speed_bps > 0) {
            wchar_t speed_buf[64];
            double gbps = static_cast<double>(env.network.link_speed_bps) / 1e9;
            swprintf(speed_buf, 64, L"%.1f Gbps", gbps);
            lc_log(L"Link:    %s | MTU %u | NICs: %u",
                   speed_buf, env.network.mtu, env.network.nic_count);
        }
    } else {
        lc_log(L"Target:  %s | %s | Cluster: %u B | Sector: %u B",
               disk_type_str(env.dest.disk_type),
               env.dest.fs_name[0] ? env.dest.fs_name : L"?",
               env.dest.cluster_size, env.dest.sector_size);
    }

    wchar_t temp_path[MAX_PATH_EXTENDED];
    swprintf(temp_path, MAX_PATH_EXTENDED, L"%s\\_largecopy_bench.tmp", cfg.dest);

    const uint64_t bench_size = 1ULL * 1024 * 1024 * 1024;
    const uint32_t chunk = cfg.chunk_size;

    lc_log(L"Writing 1 GB with %u MB chunks...", chunk / (1024 * 1024));

    bool have_priv = acquire_volume_privilege();
    preallocate_destination(temp_path, bench_size, have_priv);

    // For remote paths, skip FILE_FLAG_WRITE_THROUGH (causes brutal latency over SMB)
    DWORD write_flags = FILE_FLAG_SEQUENTIAL_SCAN;
    if (!cfg.buffered) {
        write_flags |= FILE_FLAG_NO_BUFFERING;
        if (!is_remote) {
            write_flags |= FILE_FLAG_WRITE_THROUGH;
        }
    }

    HANDLE h = CreateFileW(temp_path, GENERIC_WRITE, 0, nullptr, OPEN_EXISTING,
                            write_flags, nullptr);
    if (h == INVALID_HANDLE_VALUE) {
        lc_error(L"Cannot open benchmark file: %u", GetLastError());
        DeleteFileW(temp_path);
        return 1;
    }

    uint8_t* buf = static_cast<uint8_t*>(VirtualAlloc(nullptr, chunk, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE));
    memset(buf, 0xAB, chunk);

    LARGE_INTEGER start, end, freq;
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&start);

    extern volatile long g_abort;
    uint64_t written = 0;
    uint32_t chunks_done = 0;
    uint32_t total_chunks = static_cast<uint32_t>((bench_size + chunk - 1) / chunk);
    while (written < bench_size && !g_abort) {
        DWORD to_write = static_cast<DWORD>((bench_size - written < chunk) ? (bench_size - written) : chunk);
        if (!cfg.buffered) to_write = align_up(to_write, SECTOR_SIZE);
        DWORD bytes_written = 0;
        if (!WriteFile(h, buf, to_write, &bytes_written, nullptr)) {
            lc_error(L"Write failed at offset %llu: %u", written, GetLastError());
            break;
        }
        written += to_write;
        chunks_done++;

        // Progress for slow network writes
        if (is_remote && chunks_done % 2 == 0) {
            QueryPerformanceCounter(&end);
            double el = static_cast<double>(end.QuadPart - start.QuadPart) /
                        static_cast<double>(freq.QuadPart);
            double rt = (el > 0.001) ? static_cast<double>(written) / el : 0;
            wchar_t rb[64];
            format_rate(rt, rb, 64);
            lc_log(L"  Write: %u/%u chunks | %s", chunks_done, total_chunks, rb);
        }
    }

    // Skip FlushFileBuffers on remote - it forces server disk flush and can hang
    if (!is_remote || cfg.buffered) {
        FlushFileBuffers(h);
    }
    QueryPerformanceCounter(&end);

    double elapsed = static_cast<double>(end.QuadPart - start.QuadPart) /
                     static_cast<double>(freq.QuadPart);
    double rate = static_cast<double>(bench_size) / elapsed;

    wchar_t rate_buf[64];
    format_rate(rate, rate_buf, 64);
    lc_log(L"Write: 1 GB in %.2fs = %s", elapsed, rate_buf);

    CloseHandle(h);

    lc_log(L"Reading 1 GB...");

    DWORD read_flags = FILE_FLAG_SEQUENTIAL_SCAN;
    if (!cfg.buffered) read_flags |= FILE_FLAG_NO_BUFFERING;
    
    h = CreateFileW(temp_path, GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING,
                     read_flags, nullptr);
    if (h != INVALID_HANDLE_VALUE) {
        QueryPerformanceCounter(&start);
        uint64_t total_read = 0;
        chunks_done = 0;
        while (total_read < bench_size && !g_abort) {
            DWORD bytes_read = 0;
            if (!ReadFile(h, buf, chunk, &bytes_read, nullptr) || bytes_read == 0) break;
            total_read += bytes_read;
            chunks_done++;

            if (is_remote && chunks_done % 2 == 0) {
                QueryPerformanceCounter(&end);
                double el = static_cast<double>(end.QuadPart - start.QuadPart) /
                            static_cast<double>(freq.QuadPart);
                double rt = (el > 0.001) ? static_cast<double>(total_read) / el : 0;
                wchar_t rb[64];
                format_rate(rt, rb, 64);
                lc_log(L"  Read:  %u/%u chunks | %s", chunks_done, total_chunks, rb);
            }
        }
        QueryPerformanceCounter(&end);
        elapsed = static_cast<double>(end.QuadPart - start.QuadPart) /
                  static_cast<double>(freq.QuadPart);
        rate = static_cast<double>(bench_size) / elapsed;
        format_rate(rate, rate_buf, 64);
        lc_log(L"Read:  1 GB in %.2fs = %s", elapsed, rate_buf);
        CloseHandle(h);
    }

    lc_log(L"Cleaning up...");
    VirtualFree(buf, 0, MEM_RELEASE);
    DeleteFileW(temp_path);
    lc_log(L"Benchmark complete.");

    return 0;
}

// ── Compare command ─────────────────────────────────────────────────────────
// Checks ALL chunks in random order. User presses Ctrl+C when satisfied.
// Grid represents file positions - cells light up randomly as coverage grows.
int CopyEngine::run_compare(const Config& cfg) {
    print_banner();

    // ── Get file sizes ──────────────────────────────────────────────────
    WIN32_FILE_ATTRIBUTE_DATA attr_a = {}, attr_b = {};
    if (!GetFileAttributesExW(cfg.source, GetFileExInfoStandard, &attr_a)) {
        lc_error(L"Cannot access file A '%s': %u", cfg.source, GetLastError());
        return 1;
    }
    if (!GetFileAttributesExW(cfg.dest, GetFileExInfoStandard, &attr_b)) {
        lc_error(L"Cannot access file B '%s': %u", cfg.dest, GetLastError());
        return 1;
    }

    uint64_t size_a = (static_cast<uint64_t>(attr_a.nFileSizeHigh) << 32) | attr_a.nFileSizeLow;
    uint64_t size_b = (static_cast<uint64_t>(attr_b.nFileSizeHigh) << 32) | attr_b.nFileSizeLow;

    wchar_t size_buf[64];
    format_bytes(size_a, size_buf, 64);
    lc_log(L"File A:  %s (%s)", cfg.source, size_buf);
    format_bytes(size_b, size_buf, 64);
    lc_log(L"File B:  %s (%s)", cfg.dest, size_buf);

    // ── Size mismatch = instant answer ──────────────────────────────────
    if (size_a != size_b) {
        wchar_t da[64], db[64];
        format_bytes(size_a, da, 64);
        format_bytes(size_b, db, 64);
        lc_log(L"\n\x1b[91mFiles DIFFER\x1b[0m - sizes don't match (A: %s, B: %s)", da, db);
        return 1;
    }

    if (size_a == 0) {
        lc_log(L"\n\x1b[92mFiles match\x1b[0m - both empty");
        return 0;
    }

    // ── Build full comparison plan (all chunks, shuffled) ───────────────
    static constexpr uint32_t CMP_CHUNK = 1u * 1024u * 1024u;  // 1 MB blocks
    uint32_t total_chunks = static_cast<uint32_t>((size_a + CMP_CHUNK - 1) / CMP_CHUNK);

    lc_log(L"Chunks:  %u x 1 MB | Checking all in random order (Ctrl+C to stop)\n",
           total_chunks);

    // ── Fisher-Yates shuffle for random traversal order ─────────────────
    auto* order = static_cast<uint32_t*>(
        HeapAlloc(GetProcessHeap(), 0, total_chunks * sizeof(uint32_t)));
    if (!order) { lc_error(L"Allocation failed"); return 1; }

    for (uint32_t i = 0; i < total_chunks; i++) order[i] = i;

    LARGE_INTEGER seed_qpc;
    QueryPerformanceCounter(&seed_qpc);
    uint64_t rng = static_cast<uint64_t>(seed_qpc.QuadPart) ^ 0x9E3779B97F4A7C15ULL;

    for (uint32_t i = total_chunks - 1; i > 0; i--) {
        rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17;
        uint32_t j = static_cast<uint32_t>(rng % (i + 1));
        uint32_t tmp = order[i]; order[i] = order[j]; order[j] = tmp;
    }

    // ── Grid: maps file positions to visual cells ───────────────────────
    // Cap grid at 2048 cells; for large files, each cell covers multiple chunks.
    static constexpr int MAX_GRID_CELLS = 2048;
    int grid_cells = (total_chunks <= static_cast<uint32_t>(MAX_GRID_CELLS))
                     ? static_cast<int>(total_chunks) : MAX_GRID_CELLS;

    auto* cells = static_cast<CmpCell*>(
        HeapAlloc(GetProcessHeap(), HEAP_ZERO_MEMORY, grid_cells * sizeof(CmpCell)));
    if (!cells) { HeapFree(GetProcessHeap(), 0, order); return 1; }

    int grid_width = static_cast<int>(ceil(sqrt(static_cast<double>(grid_cells))));
    if (grid_width < 4) grid_width = 4;
    if (grid_width > 64) grid_width = 64;

    // ── Allocate I/O buffers ────────────────────────────────────────────
    uint8_t* buf_a = static_cast<uint8_t*>(
        VirtualAlloc(nullptr, CMP_CHUNK, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE));
    uint8_t* buf_b = static_cast<uint8_t*>(
        VirtualAlloc(nullptr, CMP_CHUNK, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE));
    if (!buf_a || !buf_b) {
        lc_error(L"Buffer allocation failed");
        HeapFree(GetProcessHeap(), 0, order);
        HeapFree(GetProcessHeap(), 0, cells);
        if (buf_a) VirtualFree(buf_a, 0, MEM_RELEASE);
        if (buf_b) VirtualFree(buf_b, 0, MEM_RELEASE);
        return 1;
    }

    // ── Open both files (overlapped for parallel reads, random access) ──
    DWORD flags = FILE_FLAG_OVERLAPPED | FILE_FLAG_RANDOM_ACCESS;

    HANDLE h_a = CreateFileW(cfg.source, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE,
                              nullptr, OPEN_EXISTING, flags, nullptr);
    if (h_a == INVALID_HANDLE_VALUE) {
        lc_error(L"Cannot open file A: %u", GetLastError());
        VirtualFree(buf_a, 0, MEM_RELEASE); VirtualFree(buf_b, 0, MEM_RELEASE);
        HeapFree(GetProcessHeap(), 0, order); HeapFree(GetProcessHeap(), 0, cells);
        return 1;
    }

    HANDLE h_b = CreateFileW(cfg.dest, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE,
                              nullptr, OPEN_EXISTING, flags, nullptr);
    if (h_b == INVALID_HANDLE_VALUE) {
        lc_error(L"Cannot open file B: %u", GetLastError());
        CloseHandle(h_a);
        VirtualFree(buf_a, 0, MEM_RELEASE); VirtualFree(buf_b, 0, MEM_RELEASE);
        HeapFree(GetProcessHeap(), 0, order); HeapFree(GetProcessHeap(), 0, cells);
        return 1;
    }

    // ── Draw initial grid ───────────────────────────────────────────────
    print_compare_grid(cells, grid_cells, grid_width, 0, total_chunks, 0, 0.0, 0.0);

    // ── Compare loop - all chunks in random order ───────────────────────
    LARGE_INTEGER freq, start_time, now;
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&start_time);

    extern volatile long g_abort;
    uint32_t checked = 0, mismatched = 0;

    for (uint32_t si = 0; si < total_chunks && !g_abort; si++) {
        uint32_t chunk_idx = order[si];
        uint64_t offset = static_cast<uint64_t>(chunk_idx) * CMP_CHUNK;
        uint32_t data_len = CMP_CHUNK;
        if (offset + data_len > size_a) data_len = static_cast<uint32_t>(size_a - offset);

        // ── Parallel overlapped reads from both files ────────────────
        OVERLAPPED ov_a = {}, ov_b = {};
        ov_a.hEvent = CreateEventW(nullptr, TRUE, FALSE, nullptr);
        ov_b.hEvent = CreateEventW(nullptr, TRUE, FALSE, nullptr);
        ov_a.Offset     = static_cast<DWORD>(offset & 0xFFFFFFFF);
        ov_a.OffsetHigh = static_cast<DWORD>(offset >> 32);
        ov_b.Offset     = ov_a.Offset;
        ov_b.OffsetHigh = ov_a.OffsetHigh;

        BOOL ok_a = ReadFile(h_a, buf_a, data_len, nullptr, &ov_a);
        DWORD err_a = ok_a ? 0 : GetLastError();
        BOOL ok_b = ReadFile(h_b, buf_b, data_len, nullptr, &ov_b);
        DWORD err_b = ok_b ? 0 : GetLastError();

        DWORD bytes_a = 0, bytes_b = 0;
        bool read_ok = true;

        if (!ok_a && err_a == ERROR_IO_PENDING)
            GetOverlappedResult(h_a, &ov_a, &bytes_a, TRUE);
        else if (ok_a)
            GetOverlappedResult(h_a, &ov_a, &bytes_a, TRUE);
        else {
            lc_error(L"Read failed on file A at chunk %u: %u", chunk_idx, err_a);
            read_ok = false;
        }

        if (read_ok) {
            if (!ok_b && err_b == ERROR_IO_PENDING)
                GetOverlappedResult(h_b, &ov_b, &bytes_b, TRUE);
            else if (ok_b)
                GetOverlappedResult(h_b, &ov_b, &bytes_b, TRUE);
            else {
                lc_error(L"Read failed on file B at chunk %u: %u", chunk_idx, err_b);
                read_ok = false;
            }
        }

        CloseHandle(ov_a.hEvent);
        CloseHandle(ov_b.hEvent);

        if (!read_ok) break;

        // ── Hash both chunks and compare ────────────────────────────
        XXH128_hash_t hash_a = XXH3_128bits(buf_a, data_len);
        XXH128_hash_t hash_b = XXH3_128bits(buf_b, data_len);

        checked++;

        // Map chunk index to grid cell (file-position-based)
        int cell_idx = (grid_cells == static_cast<int>(total_chunks))
                       ? static_cast<int>(chunk_idx)
                       : static_cast<int>(static_cast<uint64_t>(chunk_idx) * grid_cells / total_chunks);
        if (cell_idx >= grid_cells) cell_idx = grid_cells - 1;

        if (hash_a.low64 == hash_b.low64 && hash_a.high64 == hash_b.high64) {
            if (cells[cell_idx] == CmpCell::Unchecked)
                cells[cell_idx] = CmpCell::Match;
        } else {
            cells[cell_idx] = CmpCell::Mismatch;
            mismatched++;
        }

        // ── Redraw grid periodically ────────────────────────────────
        bool should_draw = (si % 4 == 0) || (si == total_chunks - 1)
                           || (mismatched == 1);
        if (should_draw) {
            QueryPerformanceCounter(&now);
            double elapsed = static_cast<double>(now.QuadPart - start_time.QuadPart) /
                             static_cast<double>(freq.QuadPart);
            double rate = (elapsed > 0.001) ? static_cast<double>(checked) / elapsed : 0.0;
            double cov = static_cast<double>(checked) / static_cast<double>(total_chunks) * 100.0;
            print_compare_grid(cells, grid_cells, grid_width,
                               checked, total_chunks, mismatched, rate, cov);
        }
    }

    // ── Final redraw ────────────────────────────────────────────────────
    QueryPerformanceCounter(&now);
    double elapsed = static_cast<double>(now.QuadPart - start_time.QuadPart) /
                     static_cast<double>(freq.QuadPart);
    double rate = (elapsed > 0.001) ? static_cast<double>(checked) / elapsed : 0.0;
    double coverage = static_cast<double>(checked) / static_cast<double>(total_chunks) * 100.0;
    print_compare_grid(cells, grid_cells, grid_width,
                       checked, total_chunks, mismatched, rate, coverage);

    // ── Verdict ─────────────────────────────────────────────────────────
    wchar_t time_buf[64];
    if (elapsed >= 60.0)
        swprintf(time_buf, 64, L"%.0fm%.0fs", elapsed / 60.0, fmod(elapsed, 60.0));
    else
        swprintf(time_buf, 64, L"%.1fs", elapsed);

    if (g_abort) {
        lc_log(L"\nComparison stopped - %.1f%% coverage (%u/%u chunks checked, %s)",
               coverage, checked, total_chunks, time_buf);
        if (mismatched > 0)
            lc_log(L"\x1b[91m%u mismatches found\x1b[0m", mismatched);
        else
            lc_log(L"\x1b[92mAll %u checked chunks match\x1b[0m", checked);
    } else if (mismatched > 0) {
        lc_log(L"\n\x1b[91mFiles DIFFER\x1b[0m - %u/%u chunks mismatched (%s)",
               mismatched, total_chunks, time_buf);
    } else {
        lc_log(L"\n\x1b[92mFiles match\x1b[0m - all %u chunks identical (%s)",
               total_chunks, time_buf);
    }

    // ── Cleanup ─────────────────────────────────────────────────────────
    CloseHandle(h_a);
    CloseHandle(h_b);
    VirtualFree(buf_a, 0, MEM_RELEASE);
    VirtualFree(buf_b, 0, MEM_RELEASE);
    HeapFree(GetProcessHeap(), 0, order);
    HeapFree(GetProcessHeap(), 0, cells);

    return (mismatched > 0) ? 1 : 0;
}

// ── Hash command ────────────────────────────────────────────────────────────
int CopyEngine::run_hash(const Config& cfg) {
    print_banner();

    // ── Open file and get size ──────────────────────────────────────────
    WIN32_FILE_ATTRIBUTE_DATA attr = {};
    if (!GetFileAttributesExW(cfg.source, GetFileExInfoStandard, &attr)) {
        lc_error(L"Cannot access file '%s': %u", cfg.source, GetLastError());
        return 1;
    }

    uint64_t file_size = (static_cast<uint64_t>(attr.nFileSizeHigh) << 32) | attr.nFileSizeLow;
    wchar_t size_buf[64];
    format_bytes(file_size, size_buf, 64);
    lc_log(L"File:    %s (%s)", cfg.source, size_buf);
    lc_log(L"Hash:    xxHash3-128\n");

    // ── Empty file: instant hash ────────────────────────────────────────
    if (file_size == 0) {
        XXH128_hash_t hash = XXH3_128bits(nullptr, 0);
        wchar_t line[600];
        int len = swprintf(line, 600, L"%016llX%016llX  %s\n",
                           static_cast<unsigned long long>(hash.high64),
                           static_cast<unsigned long long>(hash.low64),
                           cfg.source);
        HANDLE out = GetStdHandle(STD_OUTPUT_HANDLE);
        DWORD mode = 0;
        if (GetConsoleMode(out, &mode)) {
            DWORD w = 0; WriteConsoleW(out, line, len, &w, nullptr);
        } else {
            int u8 = WideCharToMultiByte(CP_UTF8, 0, line, len, nullptr, 0, nullptr, nullptr);
            if (u8 > 0) {
                char* b = static_cast<char*>(_alloca(u8));
                WideCharToMultiByte(CP_UTF8, 0, line, len, b, u8, nullptr, nullptr);
                DWORD w = 0; WriteFile(out, b, u8, &w, nullptr);
            }
        }
        return 0;
    }

    // ── Open for sequential buffered reads ──────────────────────────────
    HANDLE h = CreateFileW(cfg.source, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE,
                           nullptr, OPEN_EXISTING, FILE_FLAG_SEQUENTIAL_SCAN, nullptr);
    if (h == INVALID_HANDLE_VALUE) {
        lc_error(L"Cannot open file: %u", GetLastError());
        return 1;
    }

    static constexpr uint32_t HASH_BUF_SIZE = 4u * 1024u * 1024u;  // 4 MB reads
    uint8_t* buf = static_cast<uint8_t*>(
        VirtualAlloc(nullptr, HASH_BUF_SIZE, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE));
    if (!buf) {
        lc_error(L"Buffer allocation failed");
        CloseHandle(h);
        return 1;
    }

    // ── Streaming hash ──────────────────────────────────────────────────
    XXH3_state_t* state = XXH3_createState();
    XXH3_128bits_reset(state);

    LARGE_INTEGER freq, start_time, last_draw, now;
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&start_time);
    last_draw = start_time;

    extern volatile long g_abort;
    uint64_t total_read = 0;

    while (total_read < file_size && !g_abort) {
        uint32_t to_read = HASH_BUF_SIZE;
        if (file_size - total_read < to_read)
            to_read = static_cast<uint32_t>(file_size - total_read);

        DWORD bytes_read = 0;
        if (!ReadFile(h, buf, to_read, &bytes_read, nullptr) || bytes_read == 0) {
            lc_error(L"Read error at offset %llu: %u", total_read, GetLastError());
            break;
        }

        XXH3_128bits_update(state, buf, bytes_read);
        total_read += bytes_read;

        // Progress every 250ms
        QueryPerformanceCounter(&now);
        double since = static_cast<double>(now.QuadPart - last_draw.QuadPart) /
                       static_cast<double>(freq.QuadPart);
        if (since >= 0.25 || total_read >= file_size) {
            double elapsed = static_cast<double>(now.QuadPart - start_time.QuadPart) /
                             static_cast<double>(freq.QuadPart);
            double hash_rate = (elapsed > 0.001) ? static_cast<double>(total_read) / elapsed : 0.0;
            if (!cfg.quiet) print_hash_progress(total_read, file_size, hash_rate);
            last_draw = now;
        }
    }

    XXH128_hash_t hash = XXH3_128bits_digest(state);
    XXH3_freeState(state);
    CloseHandle(h);
    VirtualFree(buf, 0, MEM_RELEASE);

    if (g_abort) {
        lc_warn(L"\nHash computation aborted.");
        return 1;
    }

    // ── Final timing ────────────────────────────────────────────────────
    QueryPerformanceCounter(&now);
    double elapsed = static_cast<double>(now.QuadPart - start_time.QuadPart) /
                     static_cast<double>(freq.QuadPart);
    double hash_rate = (elapsed > 0.001) ? static_cast<double>(file_size) / elapsed : 0.0;
    wchar_t rate_buf[64], time_buf[64];
    format_rate(hash_rate, rate_buf, 64);
    if (elapsed >= 60.0)
        swprintf(time_buf, 64, L"%.0fm%.0fs", elapsed / 60.0, fmod(elapsed, 60.0));
    else
        swprintf(time_buf, 64, L"%.1fs", elapsed);

    if (!cfg.quiet)
        lc_log(L"\nCompleted in %s (%s)\n", time_buf, rate_buf);

    // ── Print hash to stdout (machine-consumable) ───────────────────────
    wchar_t hash_line[600];
    int hlen = swprintf(hash_line, 600, L"%016llX%016llX  %s\n",
                        static_cast<unsigned long long>(hash.high64),
                        static_cast<unsigned long long>(hash.low64),
                        cfg.source);

    HANDLE stdout_h = GetStdHandle(STD_OUTPUT_HANDLE);
    DWORD mode = 0;
    if (GetConsoleMode(stdout_h, &mode)) {
        DWORD written = 0;
        WriteConsoleW(stdout_h, hash_line, hlen, &written, nullptr);
    } else {
        int utf8_len = WideCharToMultiByte(CP_UTF8, 0, hash_line, hlen, nullptr, 0, nullptr, nullptr);
        if (utf8_len > 0) {
            char* utf8 = static_cast<char*>(_alloca(utf8_len));
            WideCharToMultiByte(CP_UTF8, 0, hash_line, hlen, utf8, utf8_len, nullptr, nullptr);
            DWORD written = 0;
            WriteFile(stdout_h, utf8, utf8_len, &written, nullptr);
        }
    }

    return 0;
}
