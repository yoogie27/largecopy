#pragma once
// largecopy - privilege.h - SE_MANAGE_VOLUME_NAME privilege and SetFileValidData


#include "common.h"

// Attempt to enable SE_MANAGE_VOLUME_NAME privilege.
// Returns true if successfully enabled.
bool acquire_volume_privilege();

// Pre-allocate destination file.
// If privilege is available, uses SetFileValidData (fast, no zero-fill).
// Otherwise falls back to SetEndOfFile (slower, zeros the file).
// The handle must be opened WITHOUT FILE_FLAG_NO_BUFFERING for this call,
// then re-opened with the unbuffered flags.
bool preallocate_destination(const wchar_t* path, uint64_t size, bool have_privilege);
