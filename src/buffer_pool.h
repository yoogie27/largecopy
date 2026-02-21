#pragma once
// largecopy - buffer_pool.h - Lock-free aligned buffer pool


#include "common.h"

class BufferPool {
public:
    // Initialize pool with `count` buffers of `buffer_size` bytes each.
    // Buffers are sector-aligned via VirtualAlloc.
    bool init(int count, uint32_t buffer_size);

    // Destroy pool and free all memory.
    void destroy();

    // Acquire a buffer (blocks if pool exhausted).
    uint8_t* acquire();

    // Try to acquire a buffer without blocking. Returns nullptr if none available.
    uint8_t* try_acquire();

    // Release a buffer back to the pool.
    void release(uint8_t* buf);

    uint32_t buffer_size() const { return buffer_size_; }

private:
    SLIST_HEADER  free_list_;
    uint8_t**     all_buffers_ = nullptr;
    int           count_       = 0;
    uint32_t      buffer_size_ = 0;
    HANDLE        available_   = nullptr; // semaphore for blocking acquire
};
