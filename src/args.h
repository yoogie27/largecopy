#pragma once
// largecopy - args.h - Command-line argument parsing


#include "common.h"

// Parse command line arguments into Config. Returns true on success.
// On failure, prints usage/error and returns false.
bool parse_args(int argc, wchar_t* argv[], Config& cfg);

// Print full usage/help text.
void print_usage();
