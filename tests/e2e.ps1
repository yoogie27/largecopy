# largecopy - End-to-End Test Suite
# Usage: pwsh -File tests/e2e.ps1 [-Exe <path>]

param(
    [string]$Exe = ""
)

$ErrorActionPreference = 'Continue'
Set-StrictMode -Version Latest

# ── Resolve binary ───────────────────────────────────────────────────────────
if ($Exe -eq "") {
    $candidates = @("build\largecopy.exe", "largecopy.exe")
    foreach ($c in $candidates) {
        if (Test-Path $c) { $Exe = $c; break }
    }
    if ($Exe -eq "") {
        Write-Host "ERROR: largecopy.exe not found. Pass -Exe <path> or build first." -ForegroundColor Red
        exit 1
    }
}
$Exe = (Resolve-Path $Exe).Path
Write-Host "Binary: $Exe`n" -ForegroundColor Cyan

# ── Test infrastructure ──────────────────────────────────────────────────────
$script:pass = 0
$script:fail = 0
$script:tmpRoot = Join-Path ([System.IO.Path]::GetTempPath()) "largecopy_e2e_$PID"
New-Item -ItemType Directory -Path $script:tmpRoot -Force | Out-Null

function New-TestDir {
    $d = Join-Path $script:tmpRoot ([System.IO.Path]::GetRandomFileName())
    New-Item -ItemType Directory -Path $d -Force | Out-Null
    return $d
}

function New-TestFile([string]$Path, [int64]$Size, [byte]$Fill = 0xAA) {
    $parent = Split-Path $Path -Parent
    if (!(Test-Path $parent)) { New-Item -ItemType Directory -Path $parent -Force | Out-Null }
    if ($Size -eq 0) {
        [System.IO.File]::Create($Path).Dispose()
        return
    }
    $fs = [System.IO.File]::Create($Path)
    try {
        $chunk = 65536
        $buf = [byte[]]::new([Math]::Min($chunk, $Size))
        for ($i = 0; $i -lt $buf.Length; $i++) { $buf[$i] = $Fill }
        $remaining = $Size
        while ($remaining -gt 0) {
            $write = [Math]::Min($remaining, $buf.Length)
            $fs.Write($buf, 0, $write)
            $remaining -= $write
        }
    } finally {
        $fs.Dispose()
    }
}

function New-RandomFile([string]$Path, [int64]$Size) {
    $parent = Split-Path $Path -Parent
    if (!(Test-Path $parent)) { New-Item -ItemType Directory -Path $parent -Force | Out-Null }
    if ($Size -eq 0) {
        [System.IO.File]::Create($Path).Dispose()
        return
    }
    $fs = [System.IO.File]::Create($Path)
    try {
        $rng = [System.Random]::new(42)  # deterministic seed
        $chunk = 65536
        $buf = [byte[]]::new([Math]::Min($chunk, $Size))
        $remaining = $Size
        while ($remaining -gt 0) {
            $write = [Math]::Min($remaining, $buf.Length)
            $rng.NextBytes($buf)
            $fs.Write($buf, 0, $write)
            $remaining -= $write
        }
    } finally {
        $fs.Dispose()
    }
}

function Invoke-Lc {
    param([string[]]$Arguments)
    $stdoutFile = [System.IO.Path]::GetTempFileName()
    $stderrFile = [System.IO.Path]::GetTempFileName()
    try {
        $psi = @{
            FilePath               = $script:Exe
            RedirectStandardOutput = $stdoutFile
            RedirectStandardError  = $stderrFile
            NoNewWindow            = $true
            Wait                   = $true
            PassThru               = $true
        }
        if ($Arguments -and $Arguments.Count -gt 0) {
            $psi['ArgumentList'] = $Arguments
        }
        $proc = Start-Process @psi
        $stdout = [System.IO.File]::ReadAllText($stdoutFile)
        $stderr = [System.IO.File]::ReadAllText($stderrFile)
        return @{
            ExitCode = $proc.ExitCode
            Stdout   = $stdout
            Stderr   = $stderr
            Output   = $stderr + $stdout
        }
    } finally {
        Remove-Item $stdoutFile -ErrorAction SilentlyContinue
        Remove-Item $stderrFile -ErrorAction SilentlyContinue
    }
}

function FilesEqual([string]$A, [string]$B) {
    $ha = (Get-FileHash -Path $A -Algorithm SHA256).Hash
    $hb = (Get-FileHash -Path $B -Algorithm SHA256).Hash
    return $ha -eq $hb
}

# Strip ANSI escape codes for clean text matching
function Strip-Ansi([string]$Text) {
    return $Text -replace '\x1b\[[0-9;]*m', ''
}

function Test-Case {
    param([string]$Name, [scriptblock]$Body)
    try {
        & $Body
        $script:pass++
        Write-Host "  PASS  $Name" -ForegroundColor Green
    } catch {
        $script:fail++
        Write-Host "  FAIL  $Name" -ForegroundColor Red
        Write-Host "        $_" -ForegroundColor DarkRed
    }
}

function Assert-ExitCode($Result, [int]$Expected) {
    if ($Result.ExitCode -ne $Expected) {
        $clean = Strip-Ansi $Result.Output
        throw "Expected exit code $Expected, got $($Result.ExitCode).`nOutput: $($clean.Substring(0, [Math]::Min(500, $clean.Length)))"
    }
}

function Assert-ExitNonZero($Result) {
    if ($Result.ExitCode -eq 0) {
        throw "Expected nonzero exit code, got 0"
    }
}

function Assert-OutputContains($Result, [string]$Pattern) {
    $clean = Strip-Ansi $Result.Output
    if ($clean -notmatch $Pattern) {
        throw "Output does not match pattern '$Pattern'.`nOutput: $($clean.Substring(0, [Math]::Min(500, $clean.Length)))"
    }
}

function Assert-OutputNotContains($Result, [string]$Pattern) {
    $clean = Strip-Ansi $Result.Output
    if ($clean -match $Pattern) {
        throw "Output unexpectedly matches pattern '$Pattern'"
    }
}

function Assert-StdoutContains($Result, [string]$Pattern) {
    if ($Result.Stdout -notmatch $Pattern) {
        throw "Stdout does not match pattern '$Pattern'.`nStdout: $($Result.Stdout.Substring(0, [Math]::Min(300, $Result.Stdout.Length)))"
    }
}

# ═════════════════════════════════════════════════════════════════════════════
#  TESTS
# ═════════════════════════════════════════════════════════════════════════════

Write-Host "=== Help and Usage ===" -ForegroundColor Yellow

Test-Case "help command exits 0 and shows usage" {
    $r = Invoke-Lc @("help")
    Assert-ExitCode $r 0
    Assert-OutputContains $r "Usage"
}

Test-Case "--help flag exits 0" {
    $r = Invoke-Lc @("--help")
    Assert-ExitCode $r 0
    Assert-OutputContains $r "Usage"
}

Test-Case "-h flag exits 0" {
    $r = Invoke-Lc @("-h")
    Assert-ExitCode $r 0
    Assert-OutputContains $r "Usage"
}

Test-Case "no arguments exits 1" {
    $r = Invoke-Lc @()
    Assert-ExitCode $r 1
}

Test-Case "unknown command exits 1" {
    $r = Invoke-Lc @("frobnicate")
    Assert-ExitCode $r 1
    Assert-OutputContains $r "Unknown command"
}

Test-Case "unknown option exits 1" {
    $r = Invoke-Lc @("copy", "--bogus", "a", "b")
    Assert-ExitCode $r 1
    Assert-OutputContains $r "Unknown option"
}

Test-Case "copy missing args exits 1" {
    $r = Invoke-Lc @("copy")
    Assert-ExitCode $r 1
}

Test-Case "copy missing dest exits 1" {
    $d = New-TestDir
    $src = Join-Path $d "src.bin"
    New-TestFile $src 1024
    $r = Invoke-Lc @("copy", $src)
    Assert-ExitCode $r 1
}

# ─────────────────────────────────────────────────────────────────────────────
Write-Host "`n=== Copy - Happy Path ===" -ForegroundColor Yellow

Test-Case "copy 1 MB file" {
    $d = New-TestDir
    $src = Join-Path $d "src.bin"
    $dst = Join-Path $d "dst.bin"
    New-RandomFile $src (1MB)
    $r = Invoke-Lc @("copy", "--quiet", $src, $dst)
    Assert-ExitCode $r 0
    if (!(FilesEqual $src $dst)) { throw "Files differ" }
}

Test-Case "copy 10 MB file with 1 MB chunks" {
    $d = New-TestDir
    $src = Join-Path $d "src.bin"
    $dst = Join-Path $d "dst.bin"
    New-RandomFile $src (10MB)
    $r = Invoke-Lc @("copy", "--quiet", "--chunk-size", "1", $src, $dst)
    Assert-ExitCode $r 0
    if (!(FilesEqual $src $dst)) { throw "Files differ" }
}

Test-Case "copy with --no-checksum" {
    $d = New-TestDir
    $src = Join-Path $d "src.bin"
    $dst = Join-Path $d "dst.bin"
    New-RandomFile $src (1MB)
    $r = Invoke-Lc @("copy", "--quiet", "--no-checksum", $src, $dst)
    Assert-ExitCode $r 0
    if (!(FilesEqual $src $dst)) { throw "Files differ" }
}

Test-Case "copy with --verify-after" {
    $d = New-TestDir
    $src = Join-Path $d "src.bin"
    $dst = Join-Path $d "dst.bin"
    New-RandomFile $src (2MB)
    $r = Invoke-Lc @("copy", "--quiet", "--verify-after", "--chunk-size", "1", $src, $dst)
    Assert-ExitCode $r 0
    if (!(FilesEqual $src $dst)) { throw "Files differ" }
}

Test-Case "copy with --quiet produces minimal output" {
    $d = New-TestDir
    $src = Join-Path $d "src.bin"
    $dst = Join-Path $d "dst.bin"
    New-RandomFile $src (1MB)
    $r = Invoke-Lc @("copy", "--quiet", $src, $dst)
    Assert-ExitCode $r 0
    Assert-OutputNotContains $r "Detecting"
}

Test-Case "copy with --verbose" {
    $d = New-TestDir
    $src = Join-Path $d "src.bin"
    $dst = Join-Path $d "dst.bin"
    New-RandomFile $src (2MB)
    $r = Invoke-Lc @("copy", "--verbose", "--chunk-size", "1", $src, $dst)
    Assert-ExitCode $r 0
    if (!(FilesEqual $src $dst)) { throw "Files differ" }
}

Test-Case "copy with custom threads and inflight" {
    $d = New-TestDir
    $src = Join-Path $d "src.bin"
    $dst = Join-Path $d "dst.bin"
    New-RandomFile $src (2MB)
    $r = Invoke-Lc @("copy", "--quiet", "--threads", "2", "--inflight", "2", "--chunk-size", "1", $src, $dst)
    Assert-ExitCode $r 0
    if (!(FilesEqual $src $dst)) { throw "Files differ" }
}

# ─────────────────────────────────────────────────────────────────────────────
Write-Host "`n=== Copy - Edge Cases ===" -ForegroundColor Yellow

Test-Case "copy empty 0-byte file" {
    $d = New-TestDir
    $src = Join-Path $d "empty.bin"
    $dst = Join-Path $d "empty_dst.bin"
    New-TestFile $src 0
    $r = Invoke-Lc @("copy", "--quiet", $src, $dst)
    Assert-ExitCode $r 0
    if ((Get-Item $dst).Length -ne 0) { throw "Dest should be 0 bytes" }
}

Test-Case "copy 1-byte file" {
    $d = New-TestDir
    $src = Join-Path $d "one.bin"
    $dst = Join-Path $d "one_dst.bin"
    New-TestFile $src 1
    $r = Invoke-Lc @("copy", "--quiet", $src, $dst)
    Assert-ExitCode $r 0
    if (!(FilesEqual $src $dst)) { throw "Files differ" }
}

Test-Case "copy file exactly at chunk boundary" {
    $d = New-TestDir
    $src = Join-Path $d "exact.bin"
    $dst = Join-Path $d "exact_dst.bin"
    New-RandomFile $src (1MB)
    $r = Invoke-Lc @("copy", "--quiet", "--chunk-size", "1", $src, $dst)
    Assert-ExitCode $r 0
    if (!(FilesEqual $src $dst)) { throw "Files differ" }
}

Test-Case "copy file 1 byte over chunk boundary" {
    $d = New-TestDir
    $src = Join-Path $d "over.bin"
    $dst = Join-Path $d "over_dst.bin"
    New-RandomFile $src (1MB + 1)
    $r = Invoke-Lc @("copy", "--quiet", "--chunk-size", "1", $src, $dst)
    Assert-ExitCode $r 0
    if (!(FilesEqual $src $dst)) { throw "Files differ" }
}

Test-Case "copy file 1 byte under chunk boundary" {
    $d = New-TestDir
    $src = Join-Path $d "under.bin"
    $dst = Join-Path $d "under_dst.bin"
    New-RandomFile $src (1MB - 1)
    $r = Invoke-Lc @("copy", "--quiet", "--chunk-size", "1", $src, $dst)
    Assert-ExitCode $r 0
    if (!(FilesEqual $src $dst)) { throw "Files differ" }
}

# ─────────────────────────────────────────────────────────────────────────────
Write-Host "`n=== Copy - Force and Overwrite ===" -ForegroundColor Yellow

Test-Case "copy with --force overwrites existing destination" {
    $d = New-TestDir
    $srcA = Join-Path $d "a.bin"
    $srcB = Join-Path $d "b.bin"
    $dst = Join-Path $d "dst.bin"
    New-RandomFile $srcA (1MB)
    New-TestFile $srcB (1MB) 0xBB  # different content
    # First copy
    $r = Invoke-Lc @("copy", "--quiet", $srcA, $dst)
    Assert-ExitCode $r 0
    # Overwrite with --force
    $r = Invoke-Lc @("copy", "--quiet", "--force", $srcB, $dst)
    Assert-ExitCode $r 0
    if (!(FilesEqual $srcB $dst)) { throw "Dest should match srcB after --force" }
}

Test-Case "copy without --force rejects existing dest" {
    $d = New-TestDir
    $src = Join-Path $d "src.bin"
    $dst = Join-Path $d "dst.bin"
    New-RandomFile $src (1MB)
    $r = Invoke-Lc @("copy", "--quiet", $src, $dst)
    Assert-ExitCode $r 0
    # Second copy without --force should fail (ledger auto-deleted after success)
    $r = Invoke-Lc @("copy", "--quiet", $src, $dst)
    Assert-ExitNonZero $r
    Assert-OutputContains $r "already exists"
}

# ─────────────────────────────────────────────────────────────────────────────
Write-Host "`n=== Copy - Dry Run ===" -ForegroundColor Yellow

Test-Case "dry run does not create destination" {
    $d = New-TestDir
    $src = Join-Path $d "src.bin"
    $dst = Join-Path $d "dst.bin"
    New-RandomFile $src (1MB)
    $r = Invoke-Lc @("copy", "--dry-run", $src, $dst)
    Assert-ExitCode $r 0
    Assert-OutputContains $r "Dry run"
    if (Test-Path $dst) { throw "Destination should not exist after dry run" }
}

# ─────────────────────────────────────────────────────────────────────────────
Write-Host "`n=== Copy - Error Cases ===" -ForegroundColor Yellow

Test-Case "copy nonexistent source exits nonzero" {
    $d = New-TestDir
    $src = Join-Path $d "nonexistent.bin"
    $dst = Join-Path $d "dst.bin"
    $r = Invoke-Lc @("copy", "--quiet", $src, $dst)
    Assert-ExitNonZero $r
}

Test-Case "copy to invalid path exits nonzero" {
    $d = New-TestDir
    $src = Join-Path $d "src.bin"
    New-RandomFile $src 1024
    $r = Invoke-Lc @("copy", "--quiet", $src, "Z:\no\such\dir\file.bin")
    Assert-ExitNonZero $r
}

# ─────────────────────────────────────────────────────────────────────────────
Write-Host "`n=== Copy - Ledger cleanup ===" -ForegroundColor Yellow

Test-Case "ledger is auto-deleted after successful copy" {
    $d = New-TestDir
    $src = Join-Path $d "src.bin"
    $dst = Join-Path $d "dst.bin"
    New-RandomFile $src (1MB)
    $r = Invoke-Lc @("copy", "--quiet", $src, $dst)
    Assert-ExitCode $r 0
    $ledger = "$dst.lcledger"
    if (Test-Path $ledger) { throw "Ledger should be deleted after successful copy" }
}

# ─────────────────────────────────────────────────────────────────────────────
Write-Host "`n=== Compare ===" -ForegroundColor Yellow

Test-Case "compare identical files" {
    $d = New-TestDir
    $a = Join-Path $d "a.bin"
    $b = Join-Path $d "b.bin"
    New-RandomFile $a (2MB)
    Copy-Item $a $b
    $r = Invoke-Lc @("compare", "--quiet", $a, $b)
    Assert-ExitCode $r 0
    Assert-OutputContains $r "match"
}

Test-Case "compare different files" {
    $d = New-TestDir
    $a = Join-Path $d "a.bin"
    $b = Join-Path $d "b.bin"
    New-TestFile $a (1MB) 0xAA
    New-TestFile $b (1MB) 0xBB
    $r = Invoke-Lc @("compare", "--quiet", $a, $b)
    Assert-ExitNonZero $r
    Assert-OutputContains $r "DIFFER"
}

Test-Case "compare files with different sizes" {
    $d = New-TestDir
    $a = Join-Path $d "a.bin"
    $b = Join-Path $d "b.bin"
    New-RandomFile $a (1MB)
    New-RandomFile $b (2MB)
    $r = Invoke-Lc @("compare", "--quiet", $a, $b)
    Assert-ExitNonZero $r
    Assert-OutputContains $r "DIFFER"
}

Test-Case "compare verifies copy integrity" {
    $d = New-TestDir
    $src = Join-Path $d "src.bin"
    $dst = Join-Path $d "dst.bin"
    New-RandomFile $src (2MB)
    $r = Invoke-Lc @("copy", "--quiet", "--chunk-size", "1", $src, $dst)
    Assert-ExitCode $r 0
    $r = Invoke-Lc @("compare", "--quiet", $src, $dst)
    Assert-ExitCode $r 0
    Assert-OutputContains $r "match"
}

Test-Case "compare detects corrupted copy" {
    $d = New-TestDir
    $src = Join-Path $d "src.bin"
    $dst = Join-Path $d "dst.bin"
    New-RandomFile $src (2MB)
    $r = Invoke-Lc @("copy", "--quiet", "--chunk-size", "1", $src, $dst)
    Assert-ExitCode $r 0
    # Corrupt a byte in the middle of the destination
    $fs = [System.IO.File]::Open($dst, 'Open', 'ReadWrite')
    $fs.Seek(1MB, 'Begin') | Out-Null
    $fs.WriteByte(0x00)
    $fs.Dispose()
    $r = Invoke-Lc @("compare", "--quiet", $src, $dst)
    Assert-ExitNonZero $r
    Assert-OutputContains $r "DIFFER"
}

# ─────────────────────────────────────────────────────────────────────────────
Write-Host "`n=== Hash ===" -ForegroundColor Yellow

Test-Case "hash produces valid hex output on stdout" {
    $d = New-TestDir
    $f = Join-Path $d "data.bin"
    New-RandomFile $f (1MB)
    $r = Invoke-Lc @("hash", $f)
    Assert-ExitCode $r 0
    Assert-StdoutContains $r "[0-9A-Fa-f]{16,32}"
}

Test-Case "hash is deterministic" {
    $d = New-TestDir
    $f = Join-Path $d "data.bin"
    New-RandomFile $f (1MB)
    $r1 = Invoke-Lc @("hash", $f)
    $r2 = Invoke-Lc @("hash", $f)
    Assert-ExitCode $r1 0
    Assert-ExitCode $r2 0
    if ($r1.Stdout.Trim() -ne $r2.Stdout.Trim()) {
        throw "Hash not deterministic: '$($r1.Stdout.Trim())' vs '$($r2.Stdout.Trim())'"
    }
}

Test-Case "different files produce different hashes" {
    $d = New-TestDir
    $f1 = Join-Path $d "a.bin"
    $f2 = Join-Path $d "b.bin"
    New-TestFile $f1 (1MB) 0xAA
    New-TestFile $f2 (1MB) 0xBB
    $r1 = Invoke-Lc @("hash", $f1)
    $r2 = Invoke-Lc @("hash", $f2)
    Assert-ExitCode $r1 0
    Assert-ExitCode $r2 0
    $h1 = ($r1.Stdout.Trim() -split '\s+')[0]
    $h2 = ($r2.Stdout.Trim() -split '\s+')[0]
    if ($h1 -eq $h2) { throw "Different files should produce different hashes" }
}

Test-Case "hash nonexistent file exits nonzero" {
    $r = Invoke-Lc @("hash", "C:\nonexistent_file_12345.bin")
    Assert-ExitNonZero $r
}

# ─────────────────────────────────────────────────────────────────────────────
Write-Host "`n=== Bench ===" -ForegroundColor Yellow

Test-Case "bench runs and completes" {
    $d = New-TestDir
    $r = Invoke-Lc @("bench", $d)
    Assert-ExitCode $r 0
    Assert-OutputContains $r "Benchmark complete"
}

# ─────────────────────────────────────────────────────────────────────────────
Write-Host "`n=== Status ===" -ForegroundColor Yellow

Test-Case "status on nonexistent ledger exits nonzero" {
    $r = Invoke-Lc @("status", "C:\nonexistent_ledger.lcledger")
    Assert-ExitNonZero $r
    Assert-OutputContains $r "Cannot open ledger"
}

Test-Case "status on dest without ledger exits nonzero" {
    $d = New-TestDir
    $f = Join-Path $d "file.bin"
    New-TestFile $f 1024
    $r = Invoke-Lc @("status", $f)
    Assert-ExitNonZero $r
}

# ─────────────────────────────────────────────────────────────────────────────
Write-Host "`n=== Resume ===" -ForegroundColor Yellow

Test-Case "resume without ledger exits nonzero" {
    $d = New-TestDir
    $f = Join-Path $d "file.bin"
    New-TestFile $f 1024
    $r = Invoke-Lc @("resume", "--quiet", $f)
    Assert-ExitNonZero $r
}

# ─────────────────────────────────────────────────────────────────────────────
Write-Host "`n=== Large File - multi-chunk ===" -ForegroundColor Yellow

Test-Case "copy 20 MB file with 1 MB chunks" {
    $d = New-TestDir
    $src = Join-Path $d "big.bin"
    $dst = Join-Path $d "big_dst.bin"
    New-RandomFile $src (20MB)
    $r = Invoke-Lc @("copy", "--quiet", "--chunk-size", "1", $src, $dst)
    Assert-ExitCode $r 0
    if (!(FilesEqual $src $dst)) { throw "Files differ" }
}

Test-Case "copy + compare round-trip on multi-chunk file" {
    $d = New-TestDir
    $src = Join-Path $d "src.bin"
    $dst = Join-Path $d "dst.bin"
    New-RandomFile $src (5MB)
    $r = Invoke-Lc @("copy", "--quiet", "--chunk-size", "1", $src, $dst)
    Assert-ExitCode $r 0
    $r = Invoke-Lc @("compare", "--quiet", $src, $dst)
    Assert-ExitCode $r 0
    Assert-OutputContains $r "match"
}

# ═════════════════════════════════════════════════════════════════════════════
#  Summary
# ═════════════════════════════════════════════════════════════════════════════

# Cleanup
try {
    Remove-Item $script:tmpRoot -Recurse -Force -ErrorAction SilentlyContinue
} catch {}

Write-Host ""
Write-Host "=======================================" -ForegroundColor Cyan
$total = $script:pass + $script:fail
Write-Host "  Results: $script:pass/$total passed" -ForegroundColor $(if ($script:fail -eq 0) { "Green" } else { "Red" })
if ($script:fail -gt 0) {
    Write-Host "  $($script:fail) test(s) FAILED" -ForegroundColor Red
}
Write-Host "=======================================" -ForegroundColor Cyan

exit $(if ($script:fail -gt 0) { 1 } else { 0 })
