#pragma once
// largecopy - netstats.h - TCP connection statistics (retransmissions, timeouts)


#include "common.h"

// ── TCP connection stats (aggregated across all SMB connections) ─────────────
struct NetStats {
    uint32_t retrans_pkts;     // cumulative retransmitted packets
    uint64_t retrans_bytes;    // cumulative retransmitted bytes
    uint32_t timeouts;         // retransmit timeout expirations
    uint32_t dup_acks;         // duplicate ACKs received
    uint32_t cong_signals;     // congestion signals
    
    // New metrics for bottleneck analysis
    uint32_t rtt_ms;           // smoothed RTT in ms
    uint32_t cwnd;             // congestion window (segments)
    uint64_t lim_rwin_ms;      // cumulative time limited by receive window
    uint64_t lim_cwnd_ms;      // cumulative time limited by congestion window
    uint64_t lim_sender_ms;    // cumulative time limited by sender (app/disk)

    int      conn_count;       // number of tracked TCP connections
    bool     available;        // true if EStats are working
};

// Find TCP connections to port 445 from our process and enable extended stats.
// Requires admin to enable collection; returns false (and sets available=false)
// without admin.  server_name = SMB server hostname (from NetworkProfile).
bool netstats_init(const wchar_t* server_name);

// Read current TCP path stats from all tracked connections.
// Returns aggregated cumulative counters.
void netstats_sample(NetStats& out);

// Disable EStats collection and release resources.
void netstats_cleanup();
