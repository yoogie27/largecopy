// largecopy - hasher.cpp - Async xxHash3-128 thread pool


#include "hasher.h"
#define XXH_INLINE_ALL
#define XXH_STATIC_LINKING_ONLY
#include "xxhash.h"

bool HashPool::start(int thread_count, HashCompleteCallback callback, void* user_data) {
    callback_     = callback;
    user_data_    = user_data;
    thread_count_ = thread_count;
    running_      = true;

    InitializeSListHead(&queue_);

    // Semaphore: each enqueue releases 1 count, waking exactly one thread.
    // Unlike an auto-reset event, a semaphore accumulates counts so that
    // N rapid enqueues wake N threads (instead of only the first).
    wake_event_ = CreateSemaphoreW(nullptr, 0, LONG_MAX, nullptr);
    if (!wake_event_) {
        lc_error(L"Failed to create hash pool wake semaphore: %u", GetLastError());
        return false;
    }

    threads_ = static_cast<HANDLE*>(HeapAlloc(GetProcessHeap(), HEAP_ZERO_MEMORY,
                                               thread_count * sizeof(HANDLE)));
    if (!threads_) {
        lc_error(L"Failed to allocate hash thread array");
        return false;
    }

    for (int i = 0; i < thread_count; i++) {
        threads_[i] = CreateThread(nullptr, 0, worker_thread, this, 0, nullptr);
        if (!threads_[i]) {
            lc_error(L"Failed to create hash thread %d: %u", i, GetLastError());
            return false;
        }
    }

    return true;
}

bool HashPool::enqueue(ChunkContext* ctx) {
    auto* node = static_cast<QueueNode*>(
        _aligned_malloc(sizeof(QueueNode), MEMORY_ALLOCATION_ALIGNMENT));
    if (!node) return false;

    node->ctx = ctx;
    InterlockedPushEntrySList(&queue_, &node->entry);
    ReleaseSemaphore(wake_event_, 1, nullptr);
    return true;
}

void HashPool::stop() {
    running_ = false;

    // Wake all threads
    if (wake_event_) {
        ReleaseSemaphore(wake_event_, thread_count_, nullptr);
    }

    if (threads_) {
        WaitForMultipleObjects(thread_count_, threads_, TRUE, 5000);
        for (int i = 0; i < thread_count_; i++) {
            if (threads_[i]) CloseHandle(threads_[i]);
        }
        HeapFree(GetProcessHeap(), 0, threads_);
        threads_ = nullptr;
    }

    if (wake_event_) {
        CloseHandle(wake_event_);
        wake_event_ = nullptr;
    }

    // Drain remaining queue
    while (auto* entry = InterlockedPopEntrySList(&queue_)) {
        _aligned_free(entry);
    }
}

DWORD WINAPI HashPool::worker_thread(LPVOID param) {
    auto* self = static_cast<HashPool*>(param);
    self->worker_loop();
    return 0;
}

void HashPool::worker_loop() {
    while (running_) {
        auto* entry = InterlockedPopEntrySList(&queue_);
        if (!entry) {
            WaitForSingleObject(wake_event_, 50); // short timeout to check running_
            continue;
        }

        auto* node = reinterpret_cast<QueueNode*>(entry);
        ChunkContext* ctx = node->ctx;
        _aligned_free(node);

        // Compute xxHash3-128 on the buffer
        XXH128_hash_t hash = XXH3_128bits(ctx->buffer, ctx->data_length);
        ctx->hash_lo = hash.low64;
        ctx->hash_hi = hash.high64;

        // Invoke callback (which should submit write I/O)
        if (callback_) {
            callback_(ctx, user_data_);
        }
    }
}
