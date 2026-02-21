// largecopy - buffer_pool.cpp - Lock-free aligned buffer pool


#include "buffer_pool.h"

// SList entry wrapping a buffer pointer
struct BufferEntry {
    SLIST_ENTRY entry;
    uint8_t*    buffer;
};

bool BufferPool::init(int count, uint32_t buffer_size) {
    count_ = count;
    buffer_size_ = buffer_size;

    InitializeSListHead(&free_list_);

    // Create semaphore: initial = count, max = count
    available_ = CreateSemaphoreW(nullptr, count, count, nullptr);
    if (!available_) {
        lc_error(L"Failed to create buffer pool semaphore: %u", GetLastError());
        return false;
    }

    // Allocate tracking array
    all_buffers_ = static_cast<uint8_t**>(HeapAlloc(GetProcessHeap(), HEAP_ZERO_MEMORY,
                                                     count * sizeof(uint8_t*)));
    if (!all_buffers_) {
        lc_error(L"Failed to allocate buffer tracking array");
        return false;
    }

    // Allocate each buffer with VirtualAlloc (guaranteed page-aligned = sector-aligned)
    for (int i = 0; i < count; i++) {
        all_buffers_[i] = static_cast<uint8_t*>(
            VirtualAlloc(nullptr, buffer_size, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE));
        if (!all_buffers_[i]) {
            lc_error(L"Failed to allocate buffer %d (%u bytes): %u",
                     i, buffer_size, GetLastError());
            destroy();
            return false;
        }

        // Create SList entry and push to free list
        auto* node = static_cast<BufferEntry*>(
            _aligned_malloc(sizeof(BufferEntry), MEMORY_ALLOCATION_ALIGNMENT));
        if (!node) {
            lc_error(L"Failed to allocate SList node");
            destroy();
            return false;
        }
        node->buffer = all_buffers_[i];
        InterlockedPushEntrySList(&free_list_, &node->entry);
    }

    return true;
}

void BufferPool::destroy() {
    // Drain the free list
    while (auto* entry = InterlockedPopEntrySList(&free_list_)) {
        _aligned_free(entry);
    }

    // Free all VirtualAlloc'd buffers
    if (all_buffers_) {
        for (int i = 0; i < count_; i++) {
            if (all_buffers_[i]) {
                VirtualFree(all_buffers_[i], 0, MEM_RELEASE);
                all_buffers_[i] = nullptr;
            }
        }
        HeapFree(GetProcessHeap(), 0, all_buffers_);
        all_buffers_ = nullptr;
    }

    if (available_) {
        CloseHandle(available_);
        available_ = nullptr;
    }
}

uint8_t* BufferPool::acquire() {
    // Wait for a buffer to become available (blocks if pool exhausted)
    WaitForSingleObject(available_, INFINITE);

    // Pop from lock-free stack
    for (;;) {
        auto* entry = InterlockedPopEntrySList(&free_list_);
        if (entry) {
            auto* node = reinterpret_cast<BufferEntry*>(entry);
            uint8_t* buf = node->buffer;
            _aligned_free(node);
            return buf;
        }
        // Shouldn't happen if semaphore is correct, but spin just in case
        YieldProcessor();
    }
}

uint8_t* BufferPool::try_acquire() {
    // Non-blocking: return immediately if no buffer available
    DWORD result = WaitForSingleObject(available_, 0);
    if (result != WAIT_OBJECT_0) return nullptr;

    auto* entry = InterlockedPopEntrySList(&free_list_);
    if (entry) {
        auto* node = reinterpret_cast<BufferEntry*>(entry);
        uint8_t* buf = node->buffer;
        _aligned_free(node);
        return buf;
    }
    // Semaphore signaled but list empty (race) - put the count back
    ReleaseSemaphore(available_, 1, nullptr);
    return nullptr;
}

void BufferPool::release(uint8_t* buf) {
    // Create a new SList entry for this buffer
    auto* node = static_cast<BufferEntry*>(
        _aligned_malloc(sizeof(BufferEntry), MEMORY_ALLOCATION_ALIGNMENT));
    if (node) {
        node->buffer = buf;
        InterlockedPushEntrySList(&free_list_, &node->entry);
    }

    // Signal that a buffer is available
    ReleaseSemaphore(available_, 1, nullptr);
}
