@echo off
setlocal

echo.
echo  largecopy - Build Script
echo  ========================
echo.

:: Setup Visual Studio build environment
call "C:\Program Files (x86)\Microsoft Visual Studio\18\BuildTools\VC\Auxiliary\Build\vcvarsall.bat" x64 >nul 2>&1
if errorlevel 1 (
    echo ERROR: Could not initialize Visual Studio build tools.
    echo Expected at: C:\Program Files ^(x86^)\Microsoft Visual Studio\18\BuildTools
    exit /b 1
)

:: Create build directory
if not exist build mkdir build

echo Compiling...

cl.exe /nologo /std:c++20 /O2 /EHsc /W4 ^
    /DWIN32_LEAN_AND_MEAN /DUNICODE /D_UNICODE /D_CRT_SECURE_NO_WARNINGS ^
    /I vendor ^
    /Fe:build\largecopy.exe ^
    /Fo:build\ ^
    src\main.cpp src\args.cpp src\engine.cpp src\ledger.cpp ^
    src\hasher.cpp src\smb.cpp src\privilege.cpp src\console.cpp ^
    src\buffer_pool.cpp src\wan.cpp src\detect.cpp ^
    /link /SUBSYSTEM:CONSOLE ws2_32.lib iphlpapi.lib advapi32.lib mpr.lib

if errorlevel 1 (
    echo.
    echo BUILD FAILED
    exit /b 1
)

echo.
echo  Build successful: build\largecopy.exe
echo.

endlocal
