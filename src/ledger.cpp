// largecopy - ledger.cpp - Memory-mapped binary ledger for crash-safe chunk tracking


#include "ledger.h"
#define XXH_INLINE_ALL
#define XXH_STATIC_LINKING_ONLY
#include "xxhash.h"

Ledger::~Ledger() {
    close();
}

bool Ledger::create(const wchar_t* ledger_path,
                    const wchar_t* source_path,
                    const wchar_t* dest_path,
                    uint64_t source_size,
                    uint32_t chunk_size) {
    wcsncpy(ledger_path_, ledger_path, MAX_PATH_EXTENDED - 1);

    uint32_t chunk_count = static_cast<uint32_t>(
        (source_size + chunk_size - 1) / chunk_size);
    if (source_size == 0) chunk_count = 0;

    view_size_ = sizeof(LedgerHeader) + static_cast<size_t>(chunk_count) * sizeof(ChunkRecord);

    // Create the ledger file
    file_handle_ = CreateFileW(ledger_path, GENERIC_READ | GENERIC_WRITE,
                                FILE_SHARE_READ, nullptr, CREATE_ALWAYS,
                                FILE_ATTRIBUTE_NORMAL, nullptr);
    if (file_handle_ == INVALID_HANDLE_VALUE) {
        lc_error(L"Failed to create ledger file '%s': %u", ledger_path, GetLastError());
        return false;
    }

    // Set file size
    LARGE_INTEGER li;
    li.QuadPart = static_cast<LONGLONG>(view_size_);
    SetFilePointerEx(file_handle_, li, nullptr, FILE_BEGIN);
    SetEndOfFile(file_handle_);

    // Memory map
    mapping_ = CreateFileMappingW(file_handle_, nullptr, PAGE_READWRITE, 0, 0, nullptr);
    if (!mapping_) {
        lc_error(L"Failed to create file mapping for ledger: %u", GetLastError());
        close();
        return false;
    }

    view_ = static_cast<uint8_t*>(MapViewOfFile(mapping_, FILE_MAP_ALL_ACCESS, 0, 0, 0));
    if (!view_) {
        lc_error(L"Failed to map ledger into memory: %u", GetLastError());
        close();
        return false;
    }

    // Zero entire view
    memset(view_, 0, view_size_);

    header_  = reinterpret_cast<LedgerHeader*>(view_);
    records_ = reinterpret_cast<ChunkRecord*>(view_ + sizeof(LedgerHeader));

    // Fill header
    memcpy(header_->magic, LC_MAGIC, 8);
    header_->source_size = source_size;
    header_->chunk_size  = chunk_size;
    header_->chunk_count = chunk_count;

    LARGE_INTEGER qpc;
    QueryPerformanceCounter(&qpc);
    header_->created_qpc = static_cast<uint64_t>(qpc.QuadPart);

    wcsncpy(header_->source_path, source_path, MAX_PATH_EXTENDED - 1);
    wcsncpy(header_->dest_path, dest_path, MAX_PATH_EXTENDED - 1);

    // Hash source path for identity check on resume
    size_t path_bytes = wcslen(source_path) * sizeof(wchar_t);
    XXH128_hash_t path_hash = XXH3_128bits(source_path, path_bytes);
    header_->source_path_hash = path_hash.low64;
    header_->completed = 0;

    // Initialize chunk records
    uint64_t offset = 0;
    for (uint32_t i = 0; i < chunk_count; i++) {
        records_[i].offset = offset;
        uint32_t len = static_cast<uint32_t>(
            (offset + chunk_size <= source_size) ? chunk_size : (source_size - offset));
        records_[i].length = len;
        records_[i].aligned_length = align_up(len, SECTOR_SIZE);
        records_[i].state = ChunkState::Pending;
        records_[i].retry_count = 0;
        records_[i].hash_lo = 0;
        records_[i].hash_hi = 0;
        records_[i].timestamp = 0;
        offset += chunk_size;
    }

    // Initialize critical section for find_next_pending
    InitializeCriticalSection(&cs_);
    cs_init_ = true;

    flush();
    return true;
}

bool Ledger::open(const wchar_t* ledger_path) {
    wcsncpy(ledger_path_, ledger_path, MAX_PATH_EXTENDED - 1);

    file_handle_ = CreateFileW(ledger_path, GENERIC_READ | GENERIC_WRITE,
                                FILE_SHARE_READ, nullptr, OPEN_EXISTING,
                                FILE_ATTRIBUTE_NORMAL, nullptr);
    if (file_handle_ == INVALID_HANDLE_VALUE) {
        lc_error(L"Failed to open ledger file '%s': %u", ledger_path, GetLastError());
        return false;
    }

    // Get file size
    LARGE_INTEGER file_size;
    GetFileSizeEx(file_handle_, &file_size);
    view_size_ = static_cast<size_t>(file_size.QuadPart);

    if (view_size_ < sizeof(LedgerHeader)) {
        lc_error(L"Ledger file too small");
        close();
        return false;
    }

    mapping_ = CreateFileMappingW(file_handle_, nullptr, PAGE_READWRITE, 0, 0, nullptr);
    if (!mapping_) {
        lc_error(L"Failed to create file mapping for ledger: %u", GetLastError());
        close();
        return false;
    }

    view_ = static_cast<uint8_t*>(MapViewOfFile(mapping_, FILE_MAP_ALL_ACCESS, 0, 0, 0));
    if (!view_) {
        lc_error(L"Failed to map ledger: %u", GetLastError());
        close();
        return false;
    }

    header_  = reinterpret_cast<LedgerHeader*>(view_);
    records_ = reinterpret_cast<ChunkRecord*>(view_ + sizeof(LedgerHeader));

    // Validate magic
    if (memcmp(header_->magic, LC_MAGIC, 8) != 0) {
        lc_error(L"Invalid ledger magic - not a largecopy ledger file");
        close();
        return false;
    }

    // Validate size consistency
    size_t expected = sizeof(LedgerHeader) +
                      static_cast<size_t>(header_->chunk_count) * sizeof(ChunkRecord);
    if (view_size_ < expected) {
        lc_error(L"Ledger file truncated (expected %zu bytes, got %zu)", expected, view_size_);
        close();
        return false;
    }

    InitializeCriticalSection(&cs_);
    cs_init_ = true;

    return true;
}

void Ledger::close() {
    if (view_) {
        FlushViewOfFile(view_, 0);
        UnmapViewOfFile(view_);
        view_ = nullptr;
    }
    header_ = nullptr;
    records_ = nullptr;

    if (mapping_) {
        CloseHandle(mapping_);
        mapping_ = nullptr;
    }
    if (file_handle_ != INVALID_HANDLE_VALUE) {
        CloseHandle(file_handle_);
        file_handle_ = INVALID_HANDLE_VALUE;
    }
    if (cs_init_) {
        DeleteCriticalSection(&cs_);
        cs_init_ = false;
    }
}

ChunkRecord* Ledger::chunk(uint32_t index) {
    if (index >= header_->chunk_count) return nullptr;
    return &records_[index];
}

const ChunkRecord* Ledger::chunk(uint32_t index) const {
    if (index >= header_->chunk_count) return nullptr;
    return &records_[index];
}

void Ledger::mark_state(uint32_t index, ChunkState state) {
    if (index >= header_->chunk_count) return;
    LARGE_INTEGER qpc;
    QueryPerformanceCounter(&qpc);
    records_[index].timestamp = static_cast<uint64_t>(qpc.QuadPart);

    // Write state last (memory ordering: the timestamp is visible before state change)
    MemoryBarrier();
    records_[index].state = state;
}

void Ledger::mark_verified(uint32_t index, uint64_t hash_lo, uint64_t hash_hi) {
    if (index >= header_->chunk_count) return;

    // Write hash FIRST, then state - so on crash, if state=Verified the hash is valid
    records_[index].hash_lo = hash_lo;
    records_[index].hash_hi = hash_hi;

    LARGE_INTEGER qpc;
    QueryPerformanceCounter(&qpc);
    records_[index].timestamp = static_cast<uint64_t>(qpc.QuadPart);

    MemoryBarrier();
    records_[index].state = ChunkState::Verified;
}

void Ledger::mark_failed(uint32_t index) {
    if (index >= header_->chunk_count) return;
    LARGE_INTEGER qpc;
    QueryPerformanceCounter(&qpc);
    records_[index].timestamp = static_cast<uint64_t>(qpc.QuadPart);
    MemoryBarrier();
    records_[index].state = ChunkState::Failed;
}

void Ledger::increment_retry(uint32_t index) {
    if (index >= header_->chunk_count) return;
    records_[index].retry_count++;
}

int Ledger::find_next_pending() {
    EnterCriticalSection(&cs_);
    for (uint32_t i = 0; i < header_->chunk_count; i++) {
        if (records_[i].state == ChunkState::Pending) {
            records_[i].state = ChunkState::Reading;  // claim it
            LeaveCriticalSection(&cs_);
            return static_cast<int>(i);
        }
    }
    LeaveCriticalSection(&cs_);
    return -1;
}

uint32_t Ledger::count_verified() const {
    uint32_t count = 0;
    for (uint32_t i = 0; i < header_->chunk_count; i++) {
        if (records_[i].state == ChunkState::Verified) count++;
    }
    return count;
}

uint32_t Ledger::count_failed() const {
    uint32_t count = 0;
    for (uint32_t i = 0; i < header_->chunk_count; i++) {
        if (records_[i].state == ChunkState::Failed) count++;
    }
    return count;
}

uint32_t Ledger::count_pending() const {
    uint32_t count = 0;
    for (uint32_t i = 0; i < header_->chunk_count; i++) {
        if (records_[i].state == ChunkState::Pending) count++;
    }
    return count;
}

uint32_t Ledger::count_skipped() const {
    uint32_t count = 0;
    for (uint32_t i = 0; i < header_->chunk_count; i++) {
        if (records_[i].state == ChunkState::Sparse ||
            records_[i].state == ChunkState::DeltaMatch) count++;
    }
    return count;
}

bool Ledger::all_verified() const {
    for (uint32_t i = 0; i < header_->chunk_count; i++) {
        if (records_[i].state != ChunkState::Verified) return false;
    }
    return true;
}

bool Ledger::all_done() const {
    for (uint32_t i = 0; i < header_->chunk_count; i++) {
        ChunkState s = records_[i].state;
        if (s != ChunkState::Verified && s != ChunkState::Sparse &&
            s != ChunkState::DeltaMatch) return false;
    }
    return true;
}

void Ledger::mark_completed() {
    header_->completed = 1;
    flush();
}

void Ledger::flush() {
    if (view_) {
        FlushViewOfFile(view_, 0);
    }
    if (file_handle_ != INVALID_HANDLE_VALUE) {
        FlushFileBuffers(file_handle_);
    }
}

void Ledger::reset_incomplete() {
    for (uint32_t i = 0; i < header_->chunk_count; i++) {
        ChunkState s = records_[i].state;
        if (s != ChunkState::Verified && s != ChunkState::Sparse &&
            s != ChunkState::DeltaMatch) {
            records_[i].state = ChunkState::Pending;
            records_[i].retry_count = 0;
        }
    }
    header_->completed = 0;
    flush();
}
