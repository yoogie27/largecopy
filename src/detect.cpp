// largecopy - detect.cpp - Comprehensive system & storage auto-detection


#include "detect.h"
#include <winioctl.h>
#include <winternl.h>
#include <cwchar>

// ═══════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════

// Extract drive root from a path: "C:\foo" → "C:\\" , "\\server\share\..." → ""
static void get_drive_root(const wchar_t* path, wchar_t* root, size_t /*root_len*/) {
    root[0] = L'\0';
    if (!path || !path[0]) return;

    // UNC path - no local drive root
    if (path[0] == L'\\' && path[1] == L'\\') return;

    // Drive letter path
    if (path[1] == L':') {
        root[0] = path[0];
        root[1] = L':';
        root[2] = L'\\';
        root[3] = L'\0';
    }
}

// Get the physical drive number for a volume root
static int get_physical_drive(const wchar_t* root) {
    if (!root[0]) return -1;

    // Open the volume
    wchar_t vol_path[16];
    swprintf(vol_path, 16, L"\\\\.\\%c:", root[0]);

    HANDLE h = CreateFileW(vol_path, 0, FILE_SHARE_READ | FILE_SHARE_WRITE,
                            nullptr, OPEN_EXISTING, 0, nullptr);
    if (h == INVALID_HANDLE_VALUE) return -1;

    // Query volume disk extents
    VOLUME_DISK_EXTENTS extents = {};
    DWORD bytes = 0;
    BOOL ok = DeviceIoControl(h, IOCTL_VOLUME_GET_VOLUME_DISK_EXTENTS,
                               nullptr, 0, &extents, sizeof(extents), &bytes, nullptr);
    CloseHandle(h);

    if (ok && extents.NumberOfDiskExtents > 0) {
        return static_cast<int>(extents.Extents[0].DiskNumber);
    }
    return -1;
}

// Query if a physical drive is SSD (no seek penalty) or NVMe
static DiskType detect_disk_type_for_drive(int drive_number) {
    if (drive_number < 0) return DiskType::Unknown;

    wchar_t drive_path[64];
    swprintf(drive_path, 64, L"\\\\.\\PhysicalDrive%d", drive_number);

    HANDLE h = CreateFileW(drive_path, 0, FILE_SHARE_READ | FILE_SHARE_WRITE,
                            nullptr, OPEN_EXISTING, 0, nullptr);
    if (h == INVALID_HANDLE_VALUE) return DiskType::Unknown;

    // Method 1: STORAGE_PROPERTY_QUERY for seek penalty (SSD detection)
    STORAGE_PROPERTY_QUERY query = {};
    query.PropertyId = StorageDeviceSeekPenaltyProperty;
    query.QueryType = PropertyStandardQuery;

    DEVICE_SEEK_PENALTY_DESCRIPTOR seek_desc = {};
    DWORD bytes = 0;
    BOOL ok = DeviceIoControl(h, IOCTL_STORAGE_QUERY_PROPERTY,
                               &query, sizeof(query),
                               &seek_desc, sizeof(seek_desc), &bytes, nullptr);
    bool is_ssd = false;
    if (ok && bytes >= sizeof(seek_desc)) {
        is_ssd = !seek_desc.IncursSeekPenalty;
    }

    // Method 2: Check for NVMe via bus type
    STORAGE_PROPERTY_QUERY bus_query = {};
    bus_query.PropertyId = StorageAdapterProperty;
    bus_query.QueryType = PropertyStandardQuery;

    uint8_t buf[1024] = {};
    ok = DeviceIoControl(h, IOCTL_STORAGE_QUERY_PROPERTY,
                          &bus_query, sizeof(bus_query),
                          buf, sizeof(buf), &bytes, nullptr);
    bool is_nvme = false;
    if (ok && bytes >= sizeof(STORAGE_ADAPTER_DESCRIPTOR)) {
        auto* desc = reinterpret_cast<STORAGE_ADAPTER_DESCRIPTOR*>(buf);
        is_nvme = (desc->BusType == BusTypeNvme);
    }

    // Method 3: Check TRIM support
    STORAGE_PROPERTY_QUERY trim_query = {};
    trim_query.PropertyId = StorageDeviceTrimProperty;
    trim_query.QueryType = PropertyStandardQuery;

    DEVICE_TRIM_DESCRIPTOR trim_desc = {};
    ok = DeviceIoControl(h, IOCTL_STORAGE_QUERY_PROPERTY,
                          &trim_query, sizeof(trim_query),
                          &trim_desc, sizeof(trim_desc), &bytes, nullptr);

    CloseHandle(h);

    if (is_nvme) return DiskType::NVMe;
    if (is_ssd) return DiskType::SSD;
    return DiskType::HDD;
}

// ═══════════════════════════════════════════════════════════════════════════
// Storage Detection
// ═══════════════════════════════════════════════════════════════════════════

void detect_storage(const wchar_t* path, StorageProfile& profile) {
    memset(&profile, 0, sizeof(profile));
    profile.disk_type = DiskType::Unknown;
    profile.fs_type = FsType::Unknown;
    profile.cluster_size = 4096;
    profile.sector_size = 512;

    if (!path || !path[0]) return;

    // Resolve relative path to full path
    wchar_t full_path[MAX_PATH_EXTENDED] = {};
    if (!GetFullPathNameW(path, MAX_PATH_EXTENDED, full_path, nullptr)) {
        wcsncpy(full_path, path, MAX_PATH_EXTENDED - 1);
    }

    // Check if remote
    if (full_path[0] == L'\\' && full_path[1] == L'\\') {
        profile.is_remote = true;
        profile.disk_type = DiskType::Network;

        // Try to get filesystem info for the remote share
        // Build the share root: \\server\share
        wchar_t share_root[MAX_PATH_EXTENDED] = {};
        const wchar_t* p = full_path + 2;
        while (*p && *p != L'\\' && *p != L'/') p++;
        if (*p) p++;
        while (*p && *p != L'\\' && *p != L'/') p++;
        size_t len = static_cast<size_t>(p - full_path);
        wmemcpy(share_root, full_path, len);
        share_root[len] = L'\\';
        share_root[len + 1] = L'\0';

        wchar_t fs_name[32] = {};
        wchar_t vol_name[64] = {};
        DWORD serial = 0, max_comp = 0, flags = 0;
        if (GetVolumeInformationW(share_root, vol_name, 64, &serial, &max_comp,
                                   &flags, fs_name, 32)) {
            wcsncpy(profile.fs_name, fs_name, 31);
            wcsncpy(profile.volume_name, vol_name, 63);

            if (_wcsicmp(fs_name, L"NTFS") == 0) profile.fs_type = FsType::NTFS;
            else if (_wcsicmp(fs_name, L"ReFS") == 0) profile.fs_type = FsType::ReFS;

            profile.is_sparse_capable = (flags & FILE_SUPPORTS_SPARSE_FILES) != 0;
        }

        // Free space
        ULARGE_INTEGER free_avail = {}, total = {}, total_free = {};
        if (GetDiskFreeSpaceExW(share_root, &free_avail, &total, &total_free)) {
            profile.free_bytes = free_avail.QuadPart;
            profile.total_bytes = total.QuadPart;
        }

        return;
    }

    // Local path - check for mapped drive
    wchar_t root[8] = {};
    get_drive_root(full_path, root, 8);

    if (root[0] && GetDriveTypeW(root) == DRIVE_REMOTE) {
        profile.is_remote = true;
        profile.disk_type = DiskType::Network;
        // Could resolve UNC path here too, but keeping it simple
    }

    if (!root[0]) return;
    wcsncpy(profile.drive_root, root, 7);

    // Filesystem info
    wchar_t fs_name[32] = {};
    wchar_t vol_name[64] = {};
    DWORD serial = 0, max_comp = 0, flags = 0;
    if (GetVolumeInformationW(root, vol_name, 64, &serial, &max_comp,
                               &flags, fs_name, 32)) {
        wcsncpy(profile.fs_name, fs_name, 31);
        wcsncpy(profile.volume_name, vol_name, 63);

        if (_wcsicmp(fs_name, L"NTFS") == 0) profile.fs_type = FsType::NTFS;
        else if (_wcsicmp(fs_name, L"ReFS") == 0) profile.fs_type = FsType::ReFS;
        else if (_wcsicmp(fs_name, L"FAT32") == 0) profile.fs_type = FsType::FAT32;
        else if (_wcsicmp(fs_name, L"exFAT") == 0) profile.fs_type = FsType::ExFAT;

        profile.is_sparse_capable = (flags & FILE_SUPPORTS_SPARSE_FILES) != 0;
        profile.is_compressed = (flags & FILE_FILE_COMPRESSION) != 0;
    }

    // Cluster size / sector size
    DWORD spc = 0, bps = 0, free_clusters = 0, total_clusters = 0;
    if (GetDiskFreeSpaceW(root, &spc, &bps, &free_clusters, &total_clusters)) {
        profile.sector_size = bps;
        profile.cluster_size = spc * bps;
    }

    // Free space
    ULARGE_INTEGER free_avail = {}, total = {}, total_free = {};
    if (GetDiskFreeSpaceExW(root, &free_avail, &total, &total_free)) {
        profile.free_bytes = free_avail.QuadPart;
        profile.total_bytes = total.QuadPart;
    }

    // Disk type (SSD / HDD / NVMe)
    if (!profile.is_remote) {
        int drive_num = get_physical_drive(root);
        profile.disk_type = detect_disk_type_for_drive(drive_num);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// System Detection
// ═══════════════════════════════════════════════════════════════════════════

void detect_system(SystemProfile& profile) {
    memset(&profile, 0, sizeof(profile));

    // CPU count
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    profile.cpu_count = si.dwNumberOfProcessors;

    // NUMA nodes
    ULONG highest_node = 0;
    if (GetNumaHighestNodeNumber(&highest_node)) {
        profile.numa_nodes = highest_node + 1;
    }

    // RAM
    MEMORYSTATUSEX mem = {};
    mem.dwLength = sizeof(mem);
    if (GlobalMemoryStatusEx(&mem)) {
        profile.total_ram = mem.ullTotalPhys;
        profile.available_ram = mem.ullAvailPhys;
    }

    // Admin / elevated check
    HANDLE token = nullptr;
    if (OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token)) {
        TOKEN_ELEVATION elev = {};
        DWORD size = 0;
        if (GetTokenInformation(token, TokenElevation, &elev, sizeof(elev), &size)) {
            profile.is_elevated = (elev.TokenIsElevated != 0);
        }
        CloseHandle(token);
    }
    profile.is_admin = profile.is_elevated;

    // OS version via RtlGetVersion
    typedef LONG(WINAPI* RtlGetVersionFn)(PRTL_OSVERSIONINFOW);
    HMODULE ntdll = GetModuleHandleW(L"ntdll.dll");
    if (ntdll) {
        auto fn = reinterpret_cast<RtlGetVersionFn>(
            GetProcAddress(ntdll, "RtlGetVersion"));
        if (fn) {
            RTL_OSVERSIONINFOW ovi = {};
            ovi.dwOSVersionInfoSize = sizeof(ovi);
            if (fn(&ovi) == 0) {
                profile.os_build = ovi.dwBuildNumber;
                swprintf(profile.os_version, 64, L"Windows %u.%u.%u",
                         ovi.dwMajorVersion, ovi.dwMinorVersion, ovi.dwBuildNumber);
                profile.smb_compression_os = (ovi.dwBuildNumber >= 20348);
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Full Environment Detection
// ═══════════════════════════════════════════════════════════════════════════

void detect_environment(const wchar_t* source_path, const wchar_t* dest_path,
                        EnvironmentProfile& env) {
    memset(&env, 0, sizeof(env));
    detect_storage(source_path, env.source);
    detect_storage(dest_path, env.dest);
    detect_system(env.system);

    // Network detection on whichever side is remote
    if (env.source.is_remote) {
        probe_network(source_path, env.network);
    } else if (env.dest.is_remote) {
        probe_network(dest_path, env.network);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Auto-Configuration
// ═══════════════════════════════════════════════════════════════════════════

Config auto_configure(const Config& user_cfg, const EnvironmentProfile& env) {
    Config cfg = user_cfg;
    bool user_set_inflight  = user_cfg.inflight_user_set;
    bool user_set_conns     = user_cfg.connections_user_set;
    bool user_set_chunk     = user_cfg.chunk_size_user_set;
    bool user_set_threads   = user_cfg.threads_user_set;
    bool user_set_adaptive  = user_cfg.adaptive_user_set;

    bool src_remote = env.source.is_remote;
    bool dst_remote = env.dest.is_remote;
    bool any_remote = src_remote || dst_remote;

    // ── Auto-enable WAN mode if remote detected ──
    if (any_remote && !cfg.wan_mode) {
        cfg.wan_mode = true;
        if (!user_set_adaptive) cfg.adaptive = true;
    }

    // ── Auto-enable sparse if source supports it and is NTFS/ReFS ──
    if (!cfg.sparse && env.source.is_sparse_capable &&
        (env.source.fs_type == FsType::NTFS || env.source.fs_type == FsType::ReFS)) {
        cfg.sparse = true;
    }

    // ── Sector alignment ──
    // Use the larger of source/dest sector size for alignment
    uint32_t max_sector = env.source.sector_size;
    if (env.dest.sector_size > max_sector) max_sector = env.dest.sector_size;
    if (max_sector < SECTOR_SIZE) max_sector = SECTOR_SIZE;

    // Ensure chunk size is aligned to the actual sector size
    if (!user_set_chunk) {
        cfg.chunk_size = align_up(cfg.chunk_size, max_sector);
    }

    // ── IO threads ──
    if (!user_set_threads) {
        cfg.io_threads = static_cast<int>(env.system.cpu_count);
        if (cfg.io_threads > 16) cfg.io_threads = 16;
        if (cfg.io_threads < 2) cfg.io_threads = 2;
    }

    // ── Strategy based on storage types ──

    // Case 1: Local SSD/NVMe → Local SSD/NVMe (fastest possible)
    // Allow user to force SSD behavior on destination with --ssd.
    if (!any_remote) {
        DiskType src_dt = env.source.disk_type;
        DiskType dst_dt = env.dest.disk_type;
        if (cfg.force_ssd) {
            dst_dt = DiskType::SSD;
            src_dt = DiskType::SSD;
        }

        if ((src_dt == DiskType::NVMe || src_dt == DiskType::SSD) &&
            (dst_dt == DiskType::NVMe || dst_dt == DiskType::SSD)) {
            if (!user_set_inflight) cfg.inflight = 16;
            if (!user_set_chunk) cfg.chunk_size = 64 * 1024 * 1024; // 64 MB
        }
        // Case 2: HDD source → anything (sequential reads, moderate parallelism)
        else if (src_dt == DiskType::HDD && !cfg.force_ssd) {
            if (!user_set_inflight) cfg.inflight = 4; // less parallelism for HDD
            if (!user_set_chunk) cfg.chunk_size = 32 * 1024 * 1024; // 32 MB
        }
        // Case 3: anything → HDD dest (don't overwhelm the spindle)
        else if (dst_dt == DiskType::HDD && !cfg.force_ssd) {
            if (!user_set_inflight) cfg.inflight = 4;
        }

        // Local copies don't need multiple connections
        cfg.connections = 1;
        cfg.wan_mode = false;
        cfg.adaptive = false;
        return cfg;
    }

    // ── Network transfers ──
    // Tune based on detected link type. WiFi, VPN, Cellular, and Ethernet
    // each have fundamentally different characteristics that demand different
    // chunk sizes, connection counts, and pipeline depths.

    switch (env.network.link_type) {

    // ── WiFi ────────────────────────────────────────────────────────────
    // 802.11 is half-duplex at the radio layer: the AP and client take turns.
    // Multiple TCP connections compete for the SAME radio timeslots - more
    // connections = more contention overhead, not more throughput.
    // PHY rates (e.g. 433 Mbps for 802.11ac) are wildly optimistic; real
    // throughput is ~50% due to CSMA/CA, ACKs, beacons, retransmissions.
    // Buffer bloat in WiFi drivers causes latency spikes if we push too hard.
    case LinkType::WiFi:
        if (!user_set_chunk) {
            // WiFi sweet spot: 1 MB chunks. Small enough to complete quickly
            // (~5-20ms each) but large enough to amortize per-packet overhead.
            cfg.chunk_size = 1 * 1024 * 1024;
            // WiFi 6 (AX) with high PHY rates can handle a bit more
            if (env.network.link_speed_bps >= 1200000000ULL)
                cfg.chunk_size = 2 * 1024 * 1024;
        }
        if (!user_set_conns) {
            // 2 connections: just enough for SMB credit parallelism
            // without hammering the radio. WiFi can't multichannel.
            cfg.connections = 2;
        }
        if (!user_set_inflight) {
            // Conservative start - WiFi is jittery and half-duplex.
            // The adaptive controller will ramp up if throughput allows.
            cfg.inflight = 8;
        }
        if (!user_set_adaptive) cfg.adaptive = true;  // WiFi throughput varies constantly
        break;

    // ── VPN ─────────────────────────────────────────────────────────────
    // VPN is a single tunnel: all connections funnel through one encrypted
    // pipe. More connections != more throughput. High RTT (encryption +
    // double routing + tunnel overhead). MTU is smaller (IPsec: ~1400,
    // WireGuard: ~1420) - OS handles fragmentation but it costs CPU.
    // Throughput is often CPU-bound on the VPN encryption/decryption.
    case LinkType::VPN:
        if (!user_set_chunk) {
            cfg.chunk_size = 1 * 1024 * 1024;
        }
        if (!user_set_conns) {
            // 1-2 connections: they all go through the same tunnel anyway.
            cfg.connections = 2;
        }
        if (!user_set_inflight) {
            // VPN has consistent high latency - more pipeline depth to
            // keep the tunnel saturated. Adaptive controller will tune.
            cfg.inflight = 12;
        }
        if (!user_set_adaptive) cfg.adaptive = true;
        break;

    // ── Cellular ────────────────────────────────────────────────────────
    // Extremely variable: throughput swings 10x within seconds.
    // High RTT (50-200ms typical). Data caps may apply.
    case LinkType::Cellular:
        if (!user_set_chunk) {
            cfg.chunk_size = 512 * 1024;   // 512 KB - fast completion
        }
        if (!user_set_conns) {
            cfg.connections = 1;           // single radio
        }
        if (!user_set_inflight) {
            cfg.inflight = 8;             // conservative
        }
        if (!user_set_adaptive) cfg.adaptive = true;
        break;

    // ── Ethernet (wired) or unknown ─────────────────────────────────────
    // Stable throughput, low latency, multichannel works with multiple NICs.
    // Scale everything by link speed. SMB2 max write is typically 1-8 MB
    // per packet batch, so chunks > 8 MB serialize into multiple round-trips.
    default:
        if (!user_set_conns) {
            if (env.network.link_speed_bps >= 25000000000ULL) {
                cfg.connections = 16;
            } else if (env.network.link_speed_bps >= 10000000000ULL) {
                cfg.connections = 8;
            } else if (env.network.link_speed_bps >= 1000000000ULL) {
                cfg.connections = 4;
            } else {
                cfg.connections = 2;
            }

            // Multichannel with multiple Ethernet NICs → more connections
            if (env.network.smb_multichannel && env.network.nic_count >= 2) {
                cfg.connections = cfg.connections * static_cast<int>(env.network.nic_count);
                if (cfg.connections > MAX_CONNECTIONS) cfg.connections = MAX_CONNECTIONS;
            }
        }

        if (!user_set_inflight) {
            // Start conservative: connections * 4.  The adaptive controller
            // will ramp up if throughput improves.  Starting too high
            // floods the SMB server with outstanding writes before we know
            // the real throughput, which stalls non-Windows servers.
            cfg.inflight = cfg.connections * 4;
            if (cfg.inflight < 8) cfg.inflight = 8;
            if (cfg.inflight > WAN_MAX_INFLIGHT) cfg.inflight = WAN_MAX_INFLIGHT;
        }

        if (!user_set_chunk) {
            if (env.network.link_speed_bps >= 25000000000ULL) {
                cfg.chunk_size = 8 * 1024 * 1024;
            } else if (env.network.link_speed_bps >= 10000000000ULL) {
                cfg.chunk_size = 4 * 1024 * 1024;
            } else if (env.network.link_speed_bps >= 1000000000ULL) {
                cfg.chunk_size = 2 * 1024 * 1024;
            } else {
                cfg.chunk_size = 1 * 1024 * 1024;
            }
        }
        break;
    }

    // ── Common post-processing for all network link types ──

    // Write throttle: hard cap on outstanding write operations.
    // This is the PRIMARY defense against overwhelming SMB servers.
    // Only needed when the DESTINATION is remote — local writes (SSD/NVMe)
    // are fast enough that throttling just adds unnecessary contention.
    // For downloads (remote source → local dest), skip the throttle entirely.
    if (dst_remote) {
        int writes_per_conn = 6;  // Ethernet: 6 outstanding writes per connection
        switch (env.network.link_type) {
            case LinkType::WiFi:     writes_per_conn = 2; break;  // very sensitive
            case LinkType::VPN:      writes_per_conn = 3; break;
            case LinkType::Cellular: writes_per_conn = 2; break;
            default: break;
        }
        cfg.max_writes = cfg.connections * writes_per_conn;
    }

    // Adaptive controller ceiling
    {
        int max_per_conn = 12;  // Ethernet: 12 outstanding ops per connection
        switch (env.network.link_type) {
            case LinkType::WiFi:     max_per_conn = 8;  break;
            case LinkType::VPN:      max_per_conn = 16; break;
            case LinkType::Cellular: max_per_conn = 4;  break;
            default: break;
        }
        cfg.adaptive_max = cfg.connections * max_per_conn;
        if (cfg.adaptive_max < cfg.inflight) cfg.adaptive_max = cfg.inflight;
        if (cfg.adaptive_max > WAN_MAX_INFLIGHT) cfg.adaptive_max = WAN_MAX_INFLIGHT;
    }

    // SMB compression: auto-enable if OS supports it and destination is remote.
    // The engine will verify it actually works at runtime (non-Windows SMB servers
    // like macOS smbd will reject the FSCTL and we gracefully fall back).
    if (!cfg.compress && env.system.smb_compression_os && any_remote) {
        cfg.compress = true;
    }

    // Memory bounds check: ensure inflight * chunk_size < 75% available RAM
    uint64_t mem_needed = static_cast<uint64_t>(cfg.inflight) * cfg.chunk_size;
    uint64_t mem_limit = env.system.available_ram * 3 / 4;
    if (mem_limit > 0 && mem_needed > mem_limit) {
        cfg.inflight = static_cast<int>(mem_limit / cfg.chunk_size);
        if (cfg.inflight < WAN_MIN_INFLIGHT) cfg.inflight = WAN_MIN_INFLIGHT;
    }

    return cfg;
}

// ═══════════════════════════════════════════════════════════════════════════
// Pretty-Print
// ═══════════════════════════════════════════════════════════════════════════

const wchar_t* disk_type_str(DiskType t) {
    switch (t) {
        case DiskType::HDD:     return L"HDD (rotational)";
        case DiskType::SSD:     return L"SSD (SATA)";
        case DiskType::NVMe:    return L"NVMe SSD";
        case DiskType::Network: return L"Network/SMB";
        case DiskType::RAM:     return L"RAM Disk";
        default:                return L"Unknown";
    }
}

const wchar_t* fs_type_str(FsType t) {
    switch (t) {
        case FsType::NTFS:  return L"NTFS";
        case FsType::ReFS:  return L"ReFS";
        case FsType::FAT32: return L"FAT32";
        case FsType::ExFAT: return L"exFAT";
        default:            return L"Unknown";
    }
}

void print_environment(const EnvironmentProfile& env) {
    wchar_t buf[64];

    // Source
    lc_log(L"Source:  %s | %s | Cluster: %u B | Sector: %u B",
           disk_type_str(env.source.disk_type),
           env.source.fs_name[0] ? env.source.fs_name : L"?",
           env.source.cluster_size, env.source.sector_size);
    if (env.source.free_bytes > 0) {
        format_bytes(env.source.free_bytes, buf, 64);
        lc_log(L"         Free: %s", buf);
    }

    // Dest
    lc_log(L"Dest:    %s | %s | Cluster: %u B | Sector: %u B",
           disk_type_str(env.dest.disk_type),
           env.dest.fs_name[0] ? env.dest.fs_name : L"?",
           env.dest.cluster_size, env.dest.sector_size);
    if (env.dest.free_bytes > 0) {
        format_bytes(env.dest.free_bytes, buf, 64);
        lc_log(L"         Free: %s", buf);
    }

    // System
    format_bytes(env.system.total_ram, buf, 64);
    lc_log(L"System:  %u CPUs | %s RAM | %s | Admin: %s",
           env.system.cpu_count, buf, env.system.os_version,
           env.system.is_elevated ? L"Yes" : L"No");

    // Link type detail (for network transfers)
    if (env.network.is_remote && env.network.link_type != LinkType::Unknown) {
        const wchar_t* direction = L"<->";
        if (env.source.is_remote && !env.dest.is_remote) direction = L"download";
        else if (!env.source.is_remote && env.dest.is_remote) direction = L"upload";

        double eff_mbps = static_cast<double>(env.network.effective_bw_bps) / 1e6;
        wchar_t eff_buf[64];
        if (eff_mbps >= 1000.0)
            swprintf(eff_buf, 64, L"%.1f Gbps", eff_mbps / 1000.0);
        else
            swprintf(eff_buf, 64, L"%.0f Mbps", eff_mbps);

        lc_log(L"Link:    %s %s | ~%s effective throughput",
               link_type_str(env.network.link_type), direction, eff_buf);
    }
}
