# Start/Restart Backend and Frontend Servers
# This script stops any existing servers and starts fresh instances

Write-Host "=== CSC-Agentic Server Management ===" -ForegroundColor Cyan
Write-Host ""

# Function to kill processes by name
function Stop-ProcessesByName {
    param($ProcessName)
    $processes = Get-Process -Name $ProcessName -ErrorAction SilentlyContinue
    if ($processes) {
        Write-Host "Stopping $($processes.Count) $ProcessName process(es)..." -ForegroundColor Yellow
        $processes | Stop-Process -Force
        Start-Sleep -Seconds 2
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
        
        foreach ($pid in $pids) {
            if ($pid -and $pid -ne "0") {
                try {
                    Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue
                } catch {
                    # Ignore errors for processes that might already be stopped
                }
            }
        }
        Start-Sleep -Seconds 2
    }
}

Write-Host "Step 1: Stopping existing servers..." -ForegroundColor Yellow

# Stop existing background jobs
Get-Job | Where-Object { $_.Name -like "*Server*" -or $_.Name -like "*Backend*" -or $_.Name -like "*Frontend*" } | Stop-Job -PassThru | Remove-Job

# Stop Node.js processes (frontend)
Stop-ProcessesByName "node"

# Stop Python processes on port 8000 (backend)
Stop-ProcessesByPort 8000

# Stop any processes on port 3000 (frontend)
Stop-ProcessesByPort 3000

Write-Host "Step 2: Starting Backend Server..." -ForegroundColor Green

# Start backend server
Start-Job -Name "BackendServer" -ScriptBlock {
    Set-Location "E:\Code\CSC-Agentic\backend"
    python main.py
} | Out-Null

Write-Host "Backend server starting..." -ForegroundColor Gray
Start-Sleep -Seconds 3

Write-Host "Step 3: Starting Frontend Server..." -ForegroundColor Green

# Start frontend server
Start-Job -Name "FrontendServer" -ScriptBlock {
    Set-Location "E:\Code\CSC-Agentic\frontend"
    npm run dev
} | Out-Null

Write-Host "Frontend server starting..." -ForegroundColor Gray
Start-Sleep -Seconds 5

Write-Host ""
Write-Host "=== Server Status ===" -ForegroundColor Cyan

# Check backend status
try {
    $backendResponse = Invoke-WebRequest -Uri "http://localhost:8000/health" -Method GET -TimeoutSec 5 -ErrorAction SilentlyContinue
    if ($backendResponse.StatusCode -eq 200) {
        Write-Host "✓ Backend Server: Running on http://localhost:8000" -ForegroundColor Green
    } else {
        Write-Host "⚠ Backend Server: Starting... (may take a few more seconds)" -ForegroundColor Yellow
    }
} catch {
    Write-Host "⚠ Backend Server: Starting... (may take a few more seconds)" -ForegroundColor Yellow
}

# Check frontend status (just check if process is running since it takes time to start)
$frontendJob = Get-Job -Name "FrontendServer" -ErrorAction SilentlyContinue
if ($frontendJob -and $frontendJob.State -eq "Running") {
    Write-Host "✓ Frontend Server: Starting on http://localhost:3000" -ForegroundColor Green
} else {
    Write-Host "✗ Frontend Server: Failed to start" -ForegroundColor Red
}

Write-Host ""
Write-Host "=== Job Status ===" -ForegroundColor Cyan
Get-Job | Where-Object { $_.Name -like "*Server*" } | Format-Table Name, State, HasMoreData -AutoSize

Write-Host ""
Write-Host "=== Quick Commands ===" -ForegroundColor Cyan
Write-Host "View backend logs:  Receive-Job -Name BackendServer -Keep" -ForegroundColor Gray
Write-Host "View frontend logs: Receive-Job -Name FrontendServer -Keep" -ForegroundColor Gray
Write-Host "Stop all servers:   Get-Job | Stop-Job -PassThru | Remove-Job" -ForegroundColor Gray
Write-Host "Check server status: netstat -ano | findstr ':8000 :3000'" -ForegroundColor Gray

Write-Host ""
Write-Host "Servers are starting up. Please wait 10-15 seconds for full initialization." -ForegroundColor Cyan
Write-Host "Frontend will be available at: http://localhost:3000" -ForegroundColor Green
Write-Host "Backend API will be available at: http://localhost:8000" -ForegroundColor Green