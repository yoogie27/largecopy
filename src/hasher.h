#pragma once
// largecopy - hasher.h - Async xxHash3-128 thread pool


#include "common.h"

// Forward declaration
struct ChunkContext;

// Callback invoked on hash thread after hashing completes.
// The callback should submit the write I/O.
typedef void (*HashCompleteCallback)(ChunkContext* ctx, void* user_data);

class HashPool {
public:
    // Start the hash thread pool with `thread_count` threads.
    bool start(int thread_count, HashCompleteCallback callback, void* user_data);

    // Enqueue a chunk for hashing.
    void enqueue(ChunkContext* ctx);

    // Signal all threads to stop and wait for them to finish.
    void stop();

    bool running() const { return running_; }

private:
    static DWORD WINAPI worker_thread(LPVOID param);
    void worker_loop();

    struct QueueNode {
        SLIST_ENTRY entry;
        ChunkContext* ctx;
    };

    SLIST_HEADER         queue_;
    HANDLE               wake_event_   = nullptr;
    HANDLE*              threads_      = nullptr;
    int                  thread_count_ = 0;
    volatile bool        running_      = false;
    HashCompleteCallback callback_     = nullptr;
    void*                user_data_    = nullptr;
};
