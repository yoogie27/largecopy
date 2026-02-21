// largecopy - smb.cpp - SMB/network probing and auto-tuning


// winsock2.h MUST come before windows.h (included via common.h)
#include <winsock2.h>
#include <ws2tcpip.h>

#include "smb.h"

#include <iphlpapi.h>
#include <winioctl.h>
#include <winternl.h>
#include <cwchar>

#pragma comment(lib, "iphlpapi.lib")
#pragma comment(lib, "ws2_32.lib")

// Safety defines - may not be in older SDK headers
#ifndef IF_TYPE_IEEE80211
#define IF_TYPE_IEEE80211 71
#endif
#ifndef IF_TYPE_TUNNEL
#define IF_TYPE_TUNNEL 131
#endif
#ifndef IF_TYPE_PPP
#define IF_TYPE_PPP 23
#endif

// ═══════════════════════════════════════════════════════════════════════════
// Winsock init (once)
// ═══════════════════════════════════════════════════════════════════════════

static bool init_winsock() {
    static bool done = false;
    static bool ok = false;
    if (done) return ok;
    WSADATA wsa;
    ok = (WSAStartup(MAKEWORD(2, 2), &wsa) == 0);
    done = true;
    return ok;
}

// ═══════════════════════════════════════════════════════════════════════════
// UNC/path helpers
// ═══════════════════════════════════════════════════════════════════════════

static void extract_server_name(const wchar_t* path, wchar_t* server, size_t server_len) {
    server[0] = L'\0';
    if ((path[0] == L'\\' && path[1] == L'\\') || (path[0] == L'/' && path[1] == L'/')) {
        const wchar_t* start = path + 2;
        const wchar_t* end = start;
        while (*end && *end != L'\\' && *end != L'/') end++;
        size_t len = static_cast<size_t>(end - start);
        if (len >= server_len) len = server_len - 1;
        wmemcpy(server, start, len);
        server[len] = L'\0';
    }
}

static void extract_share_name(const wchar_t* path, wchar_t* share, size_t share_len) {
    share[0] = L'\0';
    if ((path[0] == L'\\' && path[1] == L'\\') || (path[0] == L'/' && path[1] == L'/')) {
        const wchar_t* p = path + 2;
        while (*p && *p != L'\\' && *p != L'/') p++;
        if (!*p) return;
        p++;
        const wchar_t* end = p;
        while (*end && *end != L'\\' && *end != L'/') end++;
        size_t len = static_cast<size_t>(end - path);
        if (len >= share_len) len = share_len - 1;
        wmemcpy(share, path, len);
        share[len] = L'\0';
    }
}

static bool is_unc_path(const wchar_t* path) {
    return path && path[0] == L'\\' && path[1] == L'\\';
}

static bool is_mapped_drive(const wchar_t* path) {
    if (!path || wcslen(path) < 2 || path[1] != L':') return false;
    wchar_t root[4] = { path[0], L':', L'\\', L'\0' };
    return GetDriveTypeW(root) == DRIVE_REMOTE;
}

// ═══════════════════════════════════════════════════════════════════════════
// Route-based adapter detection
// ═══════════════════════════════════════════════════════════════════════════

// Resolve server name (IP address or hostname) and find the local interface
// that the OS routing table would use to reach it. Returns interface index,
// or 0 on failure.
static DWORD find_route_interface(const wchar_t* server_name) {
    if (!server_name || !server_name[0]) return 0;
    if (!init_winsock()) return 0;

    char narrow[256] = {};
    WideCharToMultiByte(CP_UTF8, 0, server_name, -1, narrow, 256, nullptr, nullptr);

    // Try as numeric IP first (avoids DNS delay)
    ADDRINFOA hints = {};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_flags = AI_NUMERICHOST;

    ADDRINFOA* result = nullptr;
    int ret = getaddrinfo(narrow, "445", &hints, &result);
    if (ret != 0) {
        // Not a numeric IP - try DNS resolution
        hints.ai_flags = 0;
        ret = getaddrinfo(narrow, "445", &hints, &result);
    }
    if (ret != 0 || !result) {
        // Try IPv6 as last resort
        hints.ai_family = AF_INET6;
        hints.ai_flags = 0;
        ret = getaddrinfo(narrow, "445", &hints, &result);
        if (ret != 0 || !result) return 0;
    }

    // Ask the OS which interface would route to this destination
    DWORD if_index = 0;
    GetBestInterfaceEx(result->ai_addr, &if_index);

    freeaddrinfo(result);
    return if_index;
}

// ═══════════════════════════════════════════════════════════════════════════
// Adapter classification
// ═══════════════════════════════════════════════════════════════════════════

// Case-insensitive wide string search
static bool wcs_contains_ci(const wchar_t* haystack, const wchar_t* needle) {
    if (!haystack || !needle) return false;
    size_t hlen = wcslen(haystack);
    size_t nlen = wcslen(needle);
    if (nlen > hlen) return false;
    for (size_t i = 0; i <= hlen - nlen; i++) {
        bool match = true;
        for (size_t j = 0; j < nlen; j++) {
            if (towlower(haystack[i + j]) != towlower(needle[j])) {
                match = false;
                break;
            }
        }
        if (match) return true;
    }
    return false;
}

static LinkType classify_adapter(const IP_ADAPTER_ADDRESSES* a) {
    if (!a) return LinkType::Unknown;

    // ── WiFi (most specific check first) ──
    if (a->IfType == IF_TYPE_IEEE80211) return LinkType::WiFi;

    // ── Tunnel / PPP = VPN ──
    if (a->IfType == IF_TYPE_TUNNEL) return LinkType::VPN;
    if (a->IfType == IF_TYPE_PPP) return LinkType::VPN;

    // ── Check adapter description for known VPN products ──
    if (a->Description) {
        static const wchar_t* vpn_keywords[] = {
            L"tap-windows", L"tap-win32", L"tun ",
            L"vpn", L"wireguard", L"openvpn",
            L"cisco anyconnect", L"cisco systems vpn",
            L"forticlient", L"fortinet",
            L"pulse secure", L"juniper",
            L"globalprotect", L"palo alto",
            L"softether",
            L"nordlynx", L"protonvpn",
            L"cloudflare warp",
            L"zerotier", L"tailscale",
            nullptr
        };
        for (int i = 0; vpn_keywords[i]; i++) {
            if (wcs_contains_ci(a->Description, vpn_keywords[i]))
                return LinkType::VPN;
        }

        // Ethernet adapter with suspiciously low MTU = VPN overlay (TAP, WireGuard)
        if (a->IfType == IF_TYPE_ETHERNET_CSMACD && a->Mtu > 0 && a->Mtu < 1400) {
            return LinkType::VPN;
        }
    }

    // ── Cellular / WWAN ──
    if (a->IfType == 243 || a->IfType == 244)  // IF_TYPE_WWANPP / WWANPP2
        return LinkType::Cellular;

    // ── Standard Ethernet ──
    if (a->IfType == IF_TYPE_ETHERNET_CSMACD)
        return LinkType::Ethernet;

    // ── Loopback ──
    if (a->IfType == IF_TYPE_SOFTWARE_LOOPBACK)
        return LinkType::Loopback;

    return LinkType::Unknown;
}

// Estimate real-world usable bandwidth from PHY link rate and link type.
// WiFi PHY rates are wildly optimistic - real throughput is ~50% at best.
// VPN throughput is limited by encryption overhead and tunnel encapsulation.
static uint64_t estimate_effective_bandwidth(uint64_t phy_rate, LinkType type) {
    switch (type) {
        case LinkType::Ethernet:
            return phy_rate * 9 / 10;      // ~90% efficiency (header overhead)
        case LinkType::WiFi:
            return phy_rate / 2;            // ~50% of PHY rate (contention, overhead, half-duplex)
        case LinkType::VPN:
            return phy_rate * 6 / 10;       // ~60% (encryption CPU, tunnel encapsulation, double headers)
        case LinkType::Cellular:
            return phy_rate * 3 / 10;       // ~30% (high variability, tower sharing)
        default:
            return phy_rate * 8 / 10;       // conservative default
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Network Probing
// ═══════════════════════════════════════════════════════════════════════════

void probe_network(const wchar_t* path, NetworkProfile& profile) {
    memset(&profile, 0, sizeof(profile));
    profile.mtu = 1500;

    // ── Extract server/share from path ──
    if (is_unc_path(path)) {
        profile.is_remote = true;
        extract_server_name(path, profile.server_name, 256);
        extract_share_name(path, profile.share_name, 256);
    } else if (is_mapped_drive(path)) {
        profile.is_remote = true;
        wchar_t unc[512] = {};
        DWORD unc_len = 512;
        wchar_t drive[3] = { path[0], L':', L'\0' };
        if (WNetGetConnectionW(drive, unc, &unc_len) == NO_ERROR) {
            extract_server_name(unc, profile.server_name, 256);
            extract_share_name(unc, profile.share_name, 256);
        }
    }

    // ── Route-based adapter detection ──
    // Instead of picking the fastest NIC globally, find which adapter
    // actually routes to the server. This correctly handles WiFi-only
    // connections, VPN tunnels, and multi-NIC setups.
    DWORD route_if = 0;
    if (profile.server_name[0]) {
        route_if = find_route_interface(profile.server_name);
        profile.route_if_index = route_if;
    }

    // ── Enumerate adapters ──
    ULONG buf_size = 0;
    GetAdaptersAddresses(AF_UNSPEC, GAA_FLAG_INCLUDE_PREFIX, nullptr, nullptr, &buf_size);
    if (buf_size == 0) return;

    auto* addrs = static_cast<IP_ADAPTER_ADDRESSES*>(
        HeapAlloc(GetProcessHeap(), 0, buf_size));
    if (!addrs) return;

    if (GetAdaptersAddresses(AF_UNSPEC, GAA_FLAG_INCLUDE_PREFIX, nullptr, addrs, &buf_size)
        != NO_ERROR) {
        HeapFree(GetProcessHeap(), 0, addrs);
        return;
    }

    bool found_route_adapter = false;

    for (auto* a = addrs; a; a = a->Next) {
        if (a->IfType == IF_TYPE_SOFTWARE_LOOPBACK) continue;
        if (a->OperStatus != IfOperStatusUp) continue;

        // Count Ethernet NICs >= 1Gbps (for multichannel assessment)
        if (a->IfType == IF_TYPE_ETHERNET_CSMACD &&
            a->TransmitLinkSpeed >= 1000000000ULL) {
            profile.nic_count++;
        }

        // ── Match the routing adapter ──
        // Use IfIndex (IPv4) or Ipv6IfIndex to match GetBestInterfaceEx result
        if (route_if > 0 &&
            (a->IfIndex == route_if || a->Ipv6IfIndex == route_if)) {
            found_route_adapter = true;
            profile.link_speed_bps = a->TransmitLinkSpeed;
            profile.mtu = static_cast<uint32_t>(a->Mtu);
            profile.link_type = classify_adapter(a);
            if (a->Description) {
                wcsncpy(profile.adapter_desc, a->Description, 255);
            }
        }
    }

    // Fallback: if route detection failed, pick fastest active non-loopback adapter
    if (!found_route_adapter) {
        for (auto* a = addrs; a; a = a->Next) {
            if (a->IfType == IF_TYPE_SOFTWARE_LOOPBACK) continue;
            if (a->IfType == IF_TYPE_TUNNEL) continue;
            if (a->OperStatus != IfOperStatusUp) continue;

            if (a->TransmitLinkSpeed > profile.link_speed_bps) {
                profile.link_speed_bps = a->TransmitLinkSpeed;
                profile.mtu = static_cast<uint32_t>(a->Mtu);
                profile.link_type = classify_adapter(a);
                if (a->Description) {
                    wcsncpy(profile.adapter_desc, a->Description, 255);
                }
            }
        }
    }

    HeapFree(GetProcessHeap(), 0, addrs);

    // ── Effective bandwidth estimate ──
    profile.effective_bw_bps = estimate_effective_bandwidth(
        profile.link_speed_bps, profile.link_type);

    // ── SMB Multichannel - only useful with multiple Ethernet NICs ──
    HKEY key = nullptr;
    if (RegOpenKeyExW(HKEY_LOCAL_MACHINE,
                      L"SYSTEM\\CurrentControlSet\\Services\\LanmanWorkstation\\Parameters",
                      0, KEY_READ, &key) == ERROR_SUCCESS) {
        DWORD value = 0, size = sizeof(value);
        if (RegQueryValueExW(key, L"EnableMultiChannel", nullptr, nullptr,
                             reinterpret_cast<BYTE*>(&value), &size) == ERROR_SUCCESS) {
            profile.smb_multichannel = (value != 0);
        } else {
            profile.smb_multichannel = true; // default on modern Windows
        }
        RegCloseKey(key);
    } else {
        profile.smb_multichannel = true;
    }

    // Multichannel needs >=2 Ethernet NICs - WiFi/VPN don't benefit
    if (profile.nic_count < 2 || profile.link_type == LinkType::WiFi ||
        profile.link_type == LinkType::VPN || profile.link_type == LinkType::Cellular) {
        profile.smb_multichannel = false;
    }

    // ── SMB Compression (Windows 11 / Server 2022+) ──
    typedef LONG(WINAPI* RtlGetVersionFn)(PRTL_OSVERSIONINFOW);
    HMODULE ntdll = GetModuleHandleW(L"ntdll.dll");
    if (ntdll) {
        auto fn = reinterpret_cast<RtlGetVersionFn>(
            GetProcAddress(ntdll, "RtlGetVersion"));
        if (fn) {
            RTL_OSVERSIONINFOW ovi = {};
            ovi.dwOSVersionInfoSize = sizeof(ovi);
            if (fn(&ovi) == 0 && ovi.dwBuildNumber >= 20348) {
                profile.smb_compression = true;
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Auto-Tune (legacy - mostly superseded by detect.cpp auto_configure)
// ═══════════════════════════════════════════════════════════════════════════

void auto_tune(Config& cfg, const NetworkProfile& profile) {
    if (!profile.is_remote) return;

    if (profile.smb_multichannel && profile.nic_count >= 2) {
        if (cfg.inflight == DEFAULT_INFLIGHT)
            cfg.inflight = 16;
    }

    if (profile.link_speed_bps >= 25000000000ULL) {
        if (cfg.inflight <= 16)
            cfg.inflight = 24;
    } else if (profile.link_speed_bps >= 10000000000ULL) {
        if (cfg.inflight == DEFAULT_INFLIGHT)
            cfg.inflight = 12;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Pretty-Print
// ═══════════════════════════════════════════════════════════════════════════

const wchar_t* link_type_str(LinkType t) {
    switch (t) {
        case LinkType::Ethernet: return L"Ethernet";
        case LinkType::WiFi:     return L"WiFi";
        case LinkType::VPN:      return L"VPN";
        case LinkType::Cellular: return L"Cellular";
        case LinkType::Loopback: return L"Loopback";
        default:                 return L"Unknown";
    }
}

void print_network_profile(const NetworkProfile& profile) {
    if (!profile.is_remote) {
        fwprintf(stderr, L"Network: Local copy (no SMB)\n");
        return;
    }

    fwprintf(stderr, L"Network: ");

    if (profile.server_name[0])
        fwprintf(stderr, L"\\\\%s", profile.server_name);

    // Link type and adapter
    fwprintf(stderr, L" | %s", link_type_str(profile.link_type));
    if (profile.adapter_desc[0])
        fwprintf(stderr, L" (%s)", profile.adapter_desc);

    // Link speed
    double speed_gbps = static_cast<double>(profile.link_speed_bps) / 1e9;
    if (speed_gbps >= 1.0) {
        fwprintf(stderr, L" | %.0f Gbps", speed_gbps);
    } else {
        double speed_mbps = static_cast<double>(profile.link_speed_bps) / 1e6;
        fwprintf(stderr, L" | %.0f Mbps", speed_mbps);
    }

    // Effective bandwidth (if different from link speed)
    if (profile.effective_bw_bps > 0 && profile.effective_bw_bps != profile.link_speed_bps) {
        double eff_mbps = static_cast<double>(profile.effective_bw_bps) / 1e6;
        if (eff_mbps >= 1000.0)
            fwprintf(stderr, L" (eff: %.1f Gbps)", eff_mbps / 1000.0);
        else
            fwprintf(stderr, L" (eff: %.0f Mbps)", eff_mbps);
    }

    // MTU
    if (profile.mtu > 1500)
        fwprintf(stderr, L" | MTU %u (Jumbo)", profile.mtu);
    else
        fwprintf(stderr, L" | MTU %u", profile.mtu);

    // Multichannel
    if (profile.smb_multichannel)
        fwprintf(stderr, L" | Multichannel (%u NICs)", profile.nic_count);

    // Compression
    if (profile.smb_compression)
        fwprintf(stderr, L" | Compression: available");

    fwprintf(stderr, L"\n");
}

bool enable_smb_compression(HANDLE file_handle) {
    USHORT compression = COMPRESSION_FORMAT_DEFAULT;
    DWORD bytes_returned = 0;
    return DeviceIoControl(file_handle, FSCTL_SET_COMPRESSION,
                           &compression, sizeof(compression),
                           nullptr, 0, &bytes_returned, nullptr) != FALSE;
}
