@echo off
setlocal

cd /d "%~dp0"

echo ============================================================
echo Starting QR Dispatch Platform in background
echo ============================================================

where python >nul 2>nul
if errorlevel 1 echo ERROR: Python was not found in PATH. & pause & exit /b 1

if not exist ".venv" echo Creating virtual environment...
if not exist ".venv" python -m venv .venv

call .venv\Scripts\Activate.bat

if exist ".venv\.deps_ok" (
  echo Dependencies already prepared. Skipping install.
) else (
  echo Installing dependencies - first run...
  python -m pip install --disable-pip-version-check --no-input -r requirements.txt
  if errorlevel 1 echo ERROR: Dependency install failed. & pause & exit /b 1
  type nul > ".venv\.deps_ok"
)

set PORT=8010

set LOGFILE=server.log

echo.
echo Launching server (background)...
start "uvicorn" /B cmd /c "python -m uvicorn app:app --host 127.0.0.1 --port %PORT% > %LOGFILE% 2>&1"

echo.
echo Waiting for server to be reachable...
powershell -NoProfile -Command "$url='http://127.0.0.1:%PORT%/'; for($i=0;$i -lt 40;$i++){ try{Invoke-WebRequest -UseBasicParsing -TimeoutSec 1 $url | Out-Null; exit 0} catch { Start-Sleep -Milliseconds 250 } }; exit 1"

start "" "http://127.0.0.1:%PORT%/"

echo.
echo Last logs:
powershell -NoProfile -Command "if (Test-Path '%LOGFILE%') { Get-Content '%LOGFILE%' -Tail 40 }"

echo.
pause

