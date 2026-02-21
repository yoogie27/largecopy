#pragma once
// largecopy - smb.h - SMB/network probing and auto-tuning


#include "common.h"

// ── Link type classification ────────────────────────────────────────────────
// Detected by examining the network adapter routing to the remote server.
// Each type has dramatically different performance characteristics.
enum class LinkType {
    Unknown,
    Ethernet,    // Wired Ethernet (low latency, stable throughput, multichannel OK)
    WiFi,        // IEEE 802.11 (jitter, half-duplex radio, no multichannel benefit)
    VPN,         // VPN tunnel (high latency, MTU overhead, single pipe)
    Cellular,    // Mobile broadband (high jitter, variable throughput)
    Loopback     // localhost
};

struct NetworkProfile {
    bool     is_remote          = false;
    bool     smb_multichannel   = false;
    bool     smb_compression    = false;
    LinkType link_type          = LinkType::Unknown;
    uint32_t mtu                = 1500;
    uint64_t link_speed_bps     = 0;         // NIC-reported link speed (PHY rate)
    uint64_t effective_bw_bps   = 0;         // estimated real-world usable bandwidth
    uint32_t nic_count          = 0;         // Ethernet NICs >= 1Gbps (for multichannel)
    uint32_t route_if_index     = 0;         // interface index of adapter routing to server
    wchar_t  server_name[256]   = {};
    wchar_t  share_name[256]    = {};
    wchar_t  adapter_desc[256]  = {};        // human-readable name of routing adapter
};

// Probe the network for a given path. Fills in the NetworkProfile.
// Uses route-based detection to find the actual adapter carrying traffic.
void probe_network(const wchar_t* path, NetworkProfile& profile);

// Auto-tune config based on network profile (legacy - prefer auto_configure).
void auto_tune(Config& cfg, const NetworkProfile& profile);

// Print network profile to console.
void print_network_profile(const NetworkProfile& profile);

// String representation of link type.
const wchar_t* link_type_str(LinkType t);

// Try to enable SMB compression on a file handle.
// Returns true if the server accepted it.
bool enable_smb_compression(HANDLE file_handle);
