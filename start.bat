@echo off
setlocal

cd /d "%~dp0"

echo ============================================================
echo Starting QR Dispatch Platform (Liquid Glass UI)
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

echo.
echo Server starting at http://127.0.0.1:%PORT%
echo Opening browser...
start "" "http://127.0.0.1:%PORT%/"
echo.

echo Tip: Stop server with Ctrl+C.
python -m uvicorn app:app --host 127.0.0.1 --port %PORT%

