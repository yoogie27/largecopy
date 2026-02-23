cmake_minimum_required(VERSION 3.20)
project(largecopy LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(largecopy
    src/main.cpp
    src/args.cpp
    src/engine.cpp
    src/ledger.cpp
    src/hasher.cpp
    src/smb.cpp
    src/privilege.cpp
    src/console.cpp
    src/buffer_pool.cpp
    src/wan.cpp
    src/detect.cpp
    src/netstats.cpp
)

target_include_directories(largecopy PRIVATE vendor)

target_compile_definitions(largecopy PRIVATE
    WIN32_LEAN_AND_MEAN
    UNICODE
    _UNICODE
    _CRT_SECURE_NO_WARNINGS
)

target_link_libraries(largecopy PRIVATE
    ws2_32
    iphlpapi
    advapi32
    mpr
)
