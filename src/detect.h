#pragma once
// largecopy - detect.h - Comprehensive system & storage auto-detection


#include "common.h"
#include "smb.h"

// ── Storage medium type ─────────────────────────────────────────────────────
enum class DiskType {
    Unknown,
    HDD,        // rotational - seek-sensitive, prefer sequential
    SSD,        // NAND flash - random OK, high parallelism
    NVMe,       // NVMe SSD - extreme parallelism, very low latency
    Network,    // remote SMB/NFS - latency-dominated
    RAM         // RAM disk
};

// ── Filesystem type ─────────────────────────────────────────────────────────
enum class FsType {
    Unknown,
    NTFS,
    ReFS,
    FAT32,
    ExFAT,
    Other
};

// ── Storage profile for one endpoint (source or dest) ───────────────────────
struct StorageProfile {
    DiskType     disk_type       = DiskType::Unknown;
    FsType       fs_type         = FsType::Unknown;
    uint32_t     cluster_size    = 4096;       // filesystem cluster size
    uint32_t     sector_size     = 512;        // physical sector size
    uint64_t     free_bytes      = 0;          // free space on volume
    uint64_t     total_bytes     = 0;          // total volume size
    bool         is_sparse_capable = false;    // NTFS/ReFS sparse file support
    bool         is_compressed   = false;      // NTFS compression enabled
    bool         is_remote       = false;
    bool         trim_supported  = false;      // SSD TRIM
    wchar_t      volume_name[64] = {};
    wchar_t      fs_name[32]     = {};
    wchar_t      drive_root[8]   = {};         // e.g. "C:\\"
};

// ── System profile ──────────────────────────────────────────────────────────
struct SystemProfile {
    uint32_t     cpu_count       = 1;
    uint32_t     numa_nodes      = 1;
    uint64_t     total_ram       = 0;          // bytes
    uint64_t     available_ram   = 0;          // bytes
    bool         is_admin        = false;
    bool         is_elevated     = false;      // running elevated (UAC)
    uint32_t     os_build        = 0;
    bool         smb_compression_os = false;   // OS supports SMB compression
    wchar_t      os_version[64]  = {};
};

// ── Full environment profile ────────────────────────────────────────────────
struct EnvironmentProfile {
    StorageProfile  source;
    StorageProfile  dest;
    SystemProfile   system;
    NetworkProfile  network;
};

// ── Detection functions ─────────────────────────────────────────────────────

// Detect storage characteristics for a given file/directory path.
void detect_storage(const wchar_t* path, StorageProfile& profile);

// Detect system-level characteristics.
void detect_system(SystemProfile& profile);

// Full environment detection (calls all of the above).
void detect_environment(const wchar_t* source_path, const wchar_t* dest_path,
                        EnvironmentProfile& env);

// ── Auto-configuration ──────────────────────────────────────────────────────

// Given detected environment, auto-configure optimal settings.
// Only overrides values the user did NOT explicitly set on the command line.
// Returns a tuned copy of the config.
Config auto_configure(const Config& user_cfg, const EnvironmentProfile& env);

// ── Pretty-print ────────────────────────────────────────────────────────────

const wchar_t* disk_type_str(DiskType t);
const wchar_t* fs_type_str(FsType t);
void print_environment(const EnvironmentProfile& env);
