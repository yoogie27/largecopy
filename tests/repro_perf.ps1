# largecopy Performance Reproduction Script
# Measures throughput with different flags to identify the 300 -> 40 Mbit/s drop.

$source = "C:\temp\large_test_file.dat"
$dest = "\localhost\c$\temp\large_test_file_copy.dat"

if (-not (Test-Path "c:\temp")) { New-Item -ItemType Directory -Path "c:\temp" }

# Create a 500MB test file if it doesn't exist
if (-not (Test-Path $source)) {
    Write-Host "Creating 500MB test file..."
    $file = [System.IO.File]::Create($source)
    $file.SetLength(500MB)
    $file.Close()
}

$exe = ".\build\largecopy.exe"
if (-not (Test-Path $exe)) {
    Write-Host "Error: largecopy.exe not found in .\build"
    exit 1
}

function Run-Test($name, $flags) {
    Write-Host "`n=== Testing: $name ($flags) ===" -ForegroundColor Cyan
    if (Test-Path $dest) { Remove-Item $dest }
    
    $start = Get-Date
    & $exe copy $flags $source $dest
    $end = Get-Date
    
    $elapsed = ($end - $start).TotalSeconds
    $mbit = (500 * 8) / $elapsed
    Write-Host "Result: $name -> $($mbit.ToString("F2")) Mbit/s" -ForegroundColor Yellow
}

# 1. Default (Direct I/O, no adaptive)
Run-Test "Default (Direct I/O)" ""

# 2. Buffered Mode (The new flag)
Run-Test "Buffered Mode" "--buffered"

# 3. Adaptive Inflight
Run-Test "Adaptive Inflight" "--adaptive"

# 4. WAN Mode (Adaptive + Multichannel)
Run-Test "WAN Mode" "--wan"

# 5. Buffered + Adaptive
Run-Test "Buffered + Adaptive" "--buffered --adaptive"
