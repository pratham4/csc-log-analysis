# Stop Backend and Frontend Servers
# This script stops all running backend and frontend servers

Write-Host "=== Stopping CSC-Agentic Servers ===" -ForegroundColor Red
Write-Host ""

# Function to kill processes by name
function Stop-ProcessesByName {
    param($ProcessName)
    $processes = Get-Process -Name $ProcessName -ErrorAction SilentlyContinue
    if ($processes) {
        Write-Host "Stopping $($processes.Count) $ProcessName process(es)..." -ForegroundColor Yellow
        $processes | Stop-Process -Force
        Write-Host "✓ Stopped $ProcessName processes" -ForegroundColor Green
    } else {
        Write-Host "No $ProcessName processes found" -ForegroundColor Gray
    }
}

# Function to kill processes by port
function Stop-ProcessesByPort {
    param($Port)
    $connections = netstat -ano | findstr ":$Port "
    if ($connections) {
        Write-Host "Stopping processes on port $Port..." -ForegroundColor Yellow
        $pids = $connections | ForEach-Object { 
            ($_ -split '\s+')[-1] 
        } | Sort-Object -Unique
        
        $stopped = 0
        foreach ($pid in $pids) {
            if ($pid -and $pid -ne "0") {
                try {
                    $process = Get-Process -Id $pid -ErrorAction SilentlyContinue
                    if ($process) {
                        Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue
                        $stopped++
                    }
                } catch {
                    # Ignore errors for processes that might already be stopped
                }
            }
        }
        if ($stopped -gt 0) {
            Write-Host "✓ Stopped $stopped process(es) on port $Port" -ForegroundColor Green
        }
    } else {
        Write-Host "No processes found on port $Port" -ForegroundColor Gray
    }
}

# Stop PowerShell background jobs
$jobs = Get-Job | Where-Object { $_.Name -like "*Server*" -or $_.Name -like "*Backend*" -or $_.Name -like "*Frontend*" }
if ($jobs) {
    Write-Host "Stopping background jobs..." -ForegroundColor Yellow
    $jobs | Stop-Job -PassThru | Remove-Job
    Write-Host "✓ Stopped $($jobs.Count) background job(s)" -ForegroundColor Green
} else {
    Write-Host "No background server jobs found" -ForegroundColor Gray
}

# Stop Node.js processes (frontend)
Write-Host ""
Write-Host "Checking for Node.js processes (Frontend)..." -ForegroundColor Cyan
Stop-ProcessesByName "node"

# Stop Python processes on port 8000 (backend)
Write-Host ""
Write-Host "Checking for Backend processes (Port 8000)..." -ForegroundColor Cyan
Stop-ProcessesByPort 8000

# Stop any processes on port 3000 (frontend)
Write-Host ""
Write-Host "Checking for Frontend processes (Port 3000)..." -ForegroundColor Cyan
Stop-ProcessesByPort 3000

Write-Host ""
Write-Host "=== Final Status Check ===" -ForegroundColor Cyan

# Check if ports are still in use
$port8000 = netstat -ano | findstr ":8000 "
$port3000 = netstat -ano | findstr ":3000 "

if (-not $port8000) {
    Write-Host "✓ Port 8000 (Backend): Available" -ForegroundColor Green
} else {
    Write-Host "⚠ Port 8000 (Backend): Still in use" -ForegroundColor Yellow
}

if (-not $port3000) {
    Write-Host "✓ Port 3000 (Frontend): Available" -ForegroundColor Green
} else {
    Write-Host "⚠ Port 3000 (Frontend): Still in use" -ForegroundColor Yellow
}

# Check for remaining jobs
$remainingJobs = Get-Job | Where-Object { $_.Name -like "*Server*" }
if (-not $remainingJobs) {
    Write-Host "✓ No background server jobs running" -ForegroundColor Green
} else {
    Write-Host "⚠ Some background jobs still running:" -ForegroundColor Yellow
    $remainingJobs | Format-Table Name, State -AutoSize
}

Write-Host ""
Write-Host "All servers have been stopped." -ForegroundColor Green
Write-Host "You can now run './start-servers.ps1' to restart them." -ForegroundColor Cyan