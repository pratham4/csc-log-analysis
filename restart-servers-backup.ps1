# Restart Backend and Frontend Servers
# This script stops existing servers (ignoring errors) and starts fresh instances

Write-Host "=== CSC-Agentic Server Restart ===" -ForegroundColor Cyan
Write-Host ""

# Function to kill processes by name (ignore errors)
function Stop-ProcessesByName {
    param($ProcessName)
    try {
        $processes = Get-Process -Name $ProcessName -ErrorAction SilentlyContinue
        if ($processes) {
            Write-Host "Stopping $($processes.Count) $ProcessName process(es)..." -ForegroundColor Yellow
            $processes | Stop-Process -Force -ErrorAction SilentlyContinue
            Start-Sleep -Seconds 1
        }
    } catch {
        # Ignore all errors during stop
    }
}

# Function to kill processes by port (ignore errors)
function Stop-ProcessesByPort {
    param($Port)
    try {
        $connections = netstat -ano 2>$null | findstr ":$Port " 2>$null
        if ($connections) {
            Write-Host "Stopping processes on port $Port..." -ForegroundColor Yellow
            $pids = $connections | ForEach-Object { 
                ($_ -split '\s+')[-1] 
            } | Sort-Object -Unique
            
            foreach ($pid in $pids) {
                if ($pid -and $pid -ne "0") {
                    try {
                        Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue
                    } catch {
                        # Ignore errors
                    }
                }
            }
            Start-Sleep -Seconds 1
        }
    } catch {
        # Ignore all errors during stop
    }
}

Write-Host "Step 1: Stopping existing servers (ignoring errors)..." -ForegroundColor Yellow

# Stop existing background jobs (ignore errors)
try {
    Get-Job -ErrorAction SilentlyContinue | Where-Object { $_.Name -like "*Server*" -or $_.Name -like "*Backend*" -or $_.Name -like "*Frontend*" } | Stop-Job -PassThru -ErrorAction SilentlyContinue | Remove-Job -ErrorAction SilentlyContinue
} catch {
    # Ignore errors
}

# Stop Node.js processes (frontend)
Stop-ProcessesByName "node"

# Stop Python processes on port 8000 (backend)
Stop-ProcessesByPort 8000

# Stop any processes on port 3000 (frontend)
Stop-ProcessesByPort 3000

# Additional cleanup - try to stop any python.exe processes
Stop-ProcessesByName "python"

Write-Host "[OK] Stop phase completed (errors ignored)" -ForegroundColor Green
Write-Host ""

Write-Host "Step 2: Starting Backend Server..." -ForegroundColor Green

# Start backend server
try {
    Start-Job -Name "BackendServer" -ScriptBlock {
        Set-Location "E:\Code\CSC-Agentic\backend"
        python main.py
    } | Out-Null
    Write-Host "[OK] Backend server job started" -ForegroundColor Green
} catch {
    Write-Host "[ERROR] Failed to start backend server job" -ForegroundColor Red
}

Start-Sleep -Seconds 3

Write-Host "Step 3: Starting Frontend Server..." -ForegroundColor Green

# Start frontend server
try {
    Start-Job -Name "FrontendServer" -ScriptBlock {
        Set-Location "E:\Code\CSC-Agentic\frontend"
        npm run dev
    } | Out-Null
    Write-Host "[OK] Frontend server job started" -ForegroundColor Green
} catch {
    Write-Host "[ERROR] Failed to start frontend server job" -ForegroundColor Red
}

Write-Host ""
Write-Host "Step 4: Waiting for servers to initialize..." -ForegroundColor Cyan
Start-Sleep -Seconds 5

Write-Host ""
Write-Host "=== Server Status ===" -ForegroundColor Cyan

# Check backend status
try {
    $backendResponse = Invoke-WebRequest -Uri "http://localhost:8000/health" -Method GET -TimeoutSec 5 -ErrorAction SilentlyContinue
    if ($backendResponse.StatusCode -eq 200) {
        Write-Host "[OK] Backend Server: Running on http://localhost:8000" -ForegroundColor Green
    } else {
        Write-Host "[WARNING] Backend Server: Starting... (may take a few more seconds)" -ForegroundColor Yellow
    }
} catch {
    try {
        # Try a simple connection test instead
        $tcpClient = New-Object System.Net.Sockets.TcpClient
        $tcpClient.ConnectAsync("localhost", 8000).Wait(3000)
        if ($tcpClient.Connected) {
            Write-Host "[OK] Backend Server: Port 8000 is active" -ForegroundColor Green
            $tcpClient.Close()
        } else {
            Write-Host "[WARNING] Backend Server: Still starting..." -ForegroundColor Yellow
        }
    } catch {
        Write-Host "⚠ Backend Server: Still starting..." -ForegroundColor Yellow
    }
}

# Check frontend status
try {
    $tcpClient = New-Object System.Net.Sockets.TcpClient
    $tcpClient.ConnectAsync("localhost", 3000).Wait(3000)
    if ($tcpClient.Connected) {
        Write-Host "[OK] Frontend Server: Running on http://localhost:3000" -ForegroundColor Green
        $tcpClient.Close()
    } else {
        Write-Host "[WARNING] Frontend Server: Still starting..." -ForegroundColor Yellow
    }
} catch {
    Write-Host "⚠ Frontend Server: Still starting..." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "=== Job Status ===" -ForegroundColor Cyan
try {
    Get-Job -ErrorAction SilentlyContinue | Where-Object { $_.Name -like "*Server*" } | Format-Table Name, State, HasMoreData -AutoSize
} catch {
    Write-Host "Unable to retrieve job status" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "=== Quick Commands ===" -ForegroundColor Cyan
Write-Host "View backend logs:  Receive-Job -Name BackendServer -Keep" -ForegroundColor Gray
Write-Host "View frontend logs: Receive-Job -Name FrontendServer -Keep" -ForegroundColor Gray
Write-Host "Stop all servers:   Get-Job | Stop-Job -PassThru | Remove-Job" -ForegroundColor Gray
Write-Host "Check server ports: netstat -ano | findstr ':8000 :3000'" -ForegroundColor Gray

Write-Host ""
Write-Host "Restart completed! Servers should be available in 10-15 seconds:" -ForegroundColor Green
Write-Host "Frontend: http://localhost:3000" -ForegroundColor Cyan
Write-Host "Backend:  http://localhost:8000" -ForegroundColor Cyan