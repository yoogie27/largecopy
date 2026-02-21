// largecopy - privilege.cpp - SE_MANAGE_VOLUME_NAME privilege and SetFileValidData


#include "privilege.h"

bool acquire_volume_privilege() {
    HANDLE token = nullptr;
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_ADJUST_PRIVILEGES | TOKEN_QUERY, &token)) {
        return false;
    }

    TOKEN_PRIVILEGES tp = {};
    tp.PrivilegeCount = 1;
    tp.Privileges[0].Attributes = SE_PRIVILEGE_ENABLED;

    if (!LookupPrivilegeValueW(nullptr, SE_MANAGE_VOLUME_NAME, &tp.Privileges[0].Luid)) {
        CloseHandle(token);
        return false;
    }

    BOOL ok = AdjustTokenPrivileges(token, FALSE, &tp, sizeof(tp), nullptr, nullptr);
    DWORD err = GetLastError();
    CloseHandle(token);

    // AdjustTokenPrivileges returns TRUE even if not all privileges were assigned
    return ok && (err == ERROR_SUCCESS);
}

bool preallocate_destination(const wchar_t* path, uint64_t size, bool have_privilege) {
    // Open file with normal (buffered) I/O for pre-allocation
    // SetFileValidData and SetEndOfFile don't work well with FILE_FLAG_NO_BUFFERING
    HANDLE h = CreateFileW(
        path,
        GENERIC_WRITE,
        FILE_SHARE_READ,
        nullptr,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL,
        nullptr);

    if (h == INVALID_HANDLE_VALUE) {
        // File might not exist yet, or existing file has a stale lock -
        // use CREATE_ALWAYS to create or overwrite
        h = CreateFileW(
            path,
            GENERIC_WRITE,
            FILE_SHARE_READ,
            nullptr,
            CREATE_ALWAYS,
            FILE_ATTRIBUTE_NORMAL,
            nullptr);
    }

    if (h == INVALID_HANDLE_VALUE) {
        lc_error(L"Failed to open destination for pre-allocation: %u", GetLastError());
        return false;
    }

    // Move file pointer to desired size
    LARGE_INTEGER li;
    li.QuadPart = static_cast<LONGLONG>(size);
    if (!SetFilePointerEx(h, li, nullptr, FILE_BEGIN)) {
        lc_error(L"SetFilePointerEx failed: %u", GetLastError());
        CloseHandle(h);
        return false;
    }

    // Set end of file (allocates disk space)
    if (!SetEndOfFile(h)) {
        lc_error(L"SetEndOfFile failed: %u", GetLastError());
        CloseHandle(h);
        return false;
    }

    // If we have the privilege, use SetFileValidData to skip zero-filling
    if (have_privilege) {
        if (SetFileValidData(h, static_cast<LONGLONG>(size))) {
            // Success - the file is allocated without zeroing
        } else {
            lc_warn(L"SetFileValidData failed (err=%u), destination will be zero-filled",
                     GetLastError());
        }
    }

    CloseHandle(h);
    return true;
}
