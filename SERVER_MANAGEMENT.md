# CSC-Agentic Server Management Scripts

This folder contains PowerShell scripts to manage the backend and frontend servers for the CSC-Agentic application.

## Scripts

### `restart-servers.ps1` ⭐ **Recommended**
Single script that stops existing servers (ignoring errors) and starts fresh instances.

**Usage:**
```powershell
.\restart-servers.ps1
```

**What it does:**
- Stops any existing backend/frontend processes (ignores all errors)
- Always proceeds to start servers regardless of stop phase results
- Starts backend server on http://localhost:8000
- Starts frontend server on http://localhost:3000
- Shows server status and helpful commands
- **Robust**: Works even if ports are stuck or processes are unresponsive

### `start-servers.ps1`
Stops any existing servers and starts fresh instances of both backend and frontend servers.

**Usage:**
```powershell
.\start-servers.ps1
```

**What it does:**
- Stops any existing backend/frontend processes
- Starts backend server on http://localhost:8000
- Starts frontend server on http://localhost:3000
- Shows server status and helpful commands

### `stop-servers.ps1`
Stops all running backend and frontend servers.

**Usage:**
```powershell
.\stop-servers.ps1
```

**What it does:**
- Stops all PowerShell background jobs for servers
- Terminates Node.js processes (frontend)
- Terminates processes on ports 8000 and 3000
- Shows final status

## Quick Start

1. **Restart both servers (Recommended):**
   ```powershell
   .\restart-servers.ps1
   ```

2. **Start both servers:**
   ```powershell
   .\start-servers.ps1
   ```

3. **Stop both servers:**
   ```powershell
   .\stop-servers.ps1
   ```

4. **Manual restart:**
   ```powershell
   .\stop-servers.ps1
   .\start-servers.ps1
   ```

## Manual Commands

If you need to manage servers manually:

### View server logs:
```powershell
# Backend logs
Receive-Job -Name BackendServer -Keep

# Frontend logs
Receive-Job -Name FrontendServer -Keep
```

### Check running jobs:
```powershell
Get-Job
```

### Stop all jobs:
```powershell
Get-Job | Stop-Job -PassThru | Remove-Job
```

### Check port usage:
```powershell
netstat -ano | findstr ":8000 :3000"
```

## URLs

- **Frontend:** http://localhost:3000
- **Backend API:** http://localhost:8000
- **Backend Health Check:** http://localhost:8000/health

## Troubleshooting

### If servers won't start or ports are stuck:
**First try:** `.\restart-servers.ps1` (ignores errors and forces restart)

### If restart script doesn't work:
1. Run `.\stop-servers.ps1` again
2. Check for any remaining processes: `netstat -ano | findstr ":8000 :3000"`
3. Manually kill processes if needed: `taskkill /F /PID <process_id>`
4. Try `.\restart-servers.ps1` again

### If scripts fail to execute:
Make sure PowerShell execution policy allows script execution:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### If servers don't start:
1. Check that you're in the correct directory (CSC-Agentic root)
2. Ensure all dependencies are installed:
   - Backend: `cd backend && pip install -r requirements.txt`
   - Frontend: `cd frontend && npm install`
3. Check the logs using the commands above

### Common Issues:
- **Port already in use**: Use `.\restart-servers.ps1` - it handles this automatically
- **Servers not responding**: The restart script ignores errors and forces a clean restart
- **Background jobs stuck**: The restart script cleans up all jobs before starting fresh