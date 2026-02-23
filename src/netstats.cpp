// largecopy - netstats.cpp - TCP connection statistics via Extended Stats API


// winsock2.h MUST come before windows.h (included via common.h)
#include <winsock2.h>
#include <ws2tcpip.h>

#include "netstats.h"
#include <iphlpapi.h>
#include <tcpestats.h>
#include <tcpmib.h>

// ── Tracked TCP connections ─────────────────────────────────────────────────
static constexpr int MAX_TRACKED = 64;
static MIB_TCPROW g_rows[MAX_TRACKED];
static int        g_row_count  = 0;
static bool       g_available  = false;

// ── Convert MIB_TCPROW2 → MIB_TCPROW (for EStats API) ──────────────────────
static MIB_TCPROW to_row(const MIB_TCPROW2& r2) {
    MIB_TCPROW r = {};
    r.dwState      = r2.dwState;
    r.dwLocalAddr  = r2.dwLocalAddr;
    r.dwLocalPort  = r2.dwLocalPort;
    r.dwRemoteAddr = r2.dwRemoteAddr;
    r.dwRemotePort = r2.dwRemotePort;
    return r;
}

// ── Enable path, congestion and receive stats on a single connection ─────────
static bool enable_stats(MIB_TCPROW* row) {
    struct { BOOLEAN EnableCollection; } rw = { 2 }; // TcpBoolOptEnabled = 2
    
    SetPerTcpConnectionEStats(row, TcpConnectionEstatsPath, reinterpret_cast<PUCHAR>(&rw), 0, sizeof(rw), 0);
    SetPerTcpConnectionEStats(row, TcpConnectionEstatsSndCong, reinterpret_cast<PUCHAR>(&rw), 0, sizeof(rw), 0);
    SetPerTcpConnectionEStats(row, TcpConnectionEstatsRec, reinterpret_cast<PUCHAR>(&rw), 0, sizeof(rw), 0);
    
    return true; // If one fails, we still try the others
}

// ── Disable stats on a single connection ────────────────────────────────────
static void disable_stats(MIB_TCPROW* row) {
    struct { BOOLEAN EnableCollection; } rw = { 1 }; // TcpBoolOptDisabled = 1
    SetPerTcpConnectionEStats(row, TcpConnectionEstatsPath, reinterpret_cast<PUCHAR>(&rw), 0, sizeof(rw), 0);
    SetPerTcpConnectionEStats(row, TcpConnectionEstatsSndCong, reinterpret_cast<PUCHAR>(&rw), 0, sizeof(rw), 0);
    SetPerTcpConnectionEStats(row, TcpConnectionEstatsRec, reinterpret_cast<PUCHAR>(&rw), 0, sizeof(rw), 0);
}

// ═══════════════════════════════════════════════════════════════════════════
// Public API
// ═══════════════════════════════════════════════════════════════════════════

bool netstats_init(const wchar_t* server_name) {
    g_row_count = 0;
    g_available = false;

    if (!server_name || !server_name[0]) return false;

    // Get TCP table
    ULONG size = 0;
    GetTcpTable2(nullptr, &size, FALSE);
    if (size == 0) return false;

    auto* table = static_cast<PMIB_TCPTABLE2>(
        HeapAlloc(GetProcessHeap(), 0, size));
    if (!table) return false;

    DWORD err = GetTcpTable2(table, &size, FALSE);
    if (err != NO_ERROR) {
        HeapFree(GetProcessHeap(), 0, table);
        return false;
    }

    DWORD pid = GetCurrentProcessId();
    DWORD smb_port = htons(445);

    // Find established connections from our PID to port 445
    for (DWORD i = 0; i < table->dwNumEntries && g_row_count < MAX_TRACKED; i++) {
        const MIB_TCPROW2& r = table->table[i];
        if (r.dwOwningPid == pid &&
            r.dwRemotePort == smb_port &&
            r.dwState == MIB_TCP_STATE_ESTAB) {
            g_rows[g_row_count] = to_row(r);
            g_row_count++;
        }
    }

    HeapFree(GetProcessHeap(), 0, table);

    if (g_row_count == 0) return false;

    // Try to enable stats on each connection (requires admin)
    bool any_ok = false;
    for (int i = 0; i < g_row_count; i++) {
        if (enable_stats(&g_rows[i])) {
            any_ok = true;
        }
    }

    g_available = any_ok;
    return any_ok;
}

void netstats_sample(NetStats& out) {
    out = {};
    out.conn_count = g_row_count;
    out.available  = g_available;

    if (!g_available || g_row_count == 0) return;

    for (int i = 0; i < g_row_count; i++) {
        // 1. Path stats (RTT, retrans, timeouts)
        TCP_ESTATS_PATH_ROD_v0 rod_path = {};
        DWORD err = GetPerTcpConnectionEStats(
            &g_rows[i], TcpConnectionEstatsPath,
            nullptr, 0, 0, nullptr, 0, 0,
            reinterpret_cast<PUCHAR>(&rod_path), 0, sizeof(rod_path));

        if (err == NO_ERROR) {
            out.retrans_pkts  += rod_path.PktsRetrans;
            out.retrans_bytes += rod_path.BytesRetrans;
            out.timeouts      += rod_path.Timeouts;
            out.dup_acks      += rod_path.DupAcksIn;
            out.cong_signals  += rod_path.CongSignals;
            out.rtt_ms        += rod_path.SampleRtt;
        }

        // 2. Congestion stats (CWND, limit timers)
        TCP_ESTATS_SND_CONG_ROD_v0 rod_cong = {};
        err = GetPerTcpConnectionEStats(
            &g_rows[i], TcpConnectionEstatsSndCong,
            nullptr, 0, 0, nullptr, 0, 0,
            reinterpret_cast<PUCHAR>(&rod_cong), 0, sizeof(rod_cong));

        if (err == NO_ERROR) {
            out.cwnd           += rod_cong.CurCwnd;
            out.lim_rwin_ms    += rod_cong.SndLimTimeRwin;
            out.lim_cwnd_ms    += rod_cong.SndLimTimeCwnd;
            out.lim_sender_ms  += rod_cong.SndLimTimeSnd;
        }

        // 3. Receive stats (RWIN sent)
        TCP_ESTATS_REC_ROD_v0 rod_rec = {};
        err = GetPerTcpConnectionEStats(
            &g_rows[i], TcpConnectionEstatsRec,
            nullptr, 0, 0, nullptr, 0, 0,
            reinterpret_cast<PUCHAR>(&rod_rec), 0, sizeof(rod_rec));

        if (err == NO_ERROR) {
            out.rwin_cur           += rod_rec.CurRwinSent;
            out.rcv_win_scale      =  rod_rec.WinScaleSent; // scale is fixed per conn
        }
    }

    if (g_row_count > 0) {
        out.rtt_ms /= g_row_count;
        out.cwnd   /= g_row_count;
        out.rwin_cur /= g_row_count;
    }
}

void netstats_cleanup() {
    if (g_available) {
        for (int i = 0; i < g_row_count; i++) {
            disable_stats(&g_rows[i]);
        }
    }
    g_row_count = 0;
    g_available = false;
}
