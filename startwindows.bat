@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

echo.
echo ========================================================
echo   Checking dependencies...
echo ========================================================
echo.

python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [X] Python not found!
    echo.
    echo Install Python 3.11+ from https://www.python.org/downloads/
    echo IMPORTANT: check "Add Python to PATH" during install
    echo.
    pause
    exit /b 1
)

for /f "tokens=2" %%v in ('python --version 2^>^&1') do set PYTHON_VERSION=%%v
echo [OK] Python: %PYTHON_VERSION%

python -m pip --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [X] pip not found!
    echo Reinstall Python with "pip" option enabled
    pause
    exit /b 1
)
echo [OK] pip

git --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [--] git not found (optional, needed for auto-update)
    set HAS_GIT=0
) else (
    echo [OK] git
    set HAS_GIT=1
)

echo.

:: == Auto-update via git pull =================================================
if %HAS_GIT% == 1 (
    git rev-parse --git-dir >nul 2>&1
    if !errorlevel! == 0 (
        echo Checking for updates...
        git fetch origin main --quiet 2>nul
        for /f %%a in ('git rev-parse HEAD 2^>nul') do set LOCAL_REV=%%a
        for /f %%a in ('git rev-parse origin/main 2^>nul') do set REMOTE_REV=%%a
        if not "!LOCAL_REV!" == "!REMOTE_REV!" (
            echo [!] New version available - updating...
            git pull origin main --quiet 2>nul
            if !errorlevel! == 0 (
                echo [OK] Updated successfully
            ) else (
                echo [--] Update failed, continuing with current version
            )
        ) else (
            echo [OK] Already up to date
        )
    ) else (
        echo [--] Not a git repo, skipping auto-update
    )
)

echo.

:: == Read version from pyproject.toml =========================================
set VERSION=unknown
for /f "tokens=3 delims= " %%v in ('findstr /r "^version" pyproject.toml 2^>nul') do (
    set VERSION=%%~v
)

:: == Read last git commit =====================================================
set COMMIT=
set COMMIT_MSG=
for /f "delims=" %%c in ('git rev-parse --short HEAD 2^>nul') do set COMMIT=%%c
for /f "delims=" %%m in ('git log -1 --format^="%%s" 2^>nul') do set COMMIT_MSG=%%m

echo ========================================================
echo   SIGMA Framework  v%VERSION%
echo   Open Data and Digital City Management
if defined COMMIT (
    echo   Commit: %COMMIT%  %COMMIT_MSG%
)
echo ========================================================
echo.

:: == Stop old server if running ===============================================
python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/topics', timeout=2)" >nul 2>&1
if %errorlevel% == 0 (
    echo Stopping old server on port 8000...
    for /f "tokens=5" %%p in ('netstat -ano ^| findstr ":8000.*LISTENING"') do (
        taskkill /PID %%p /F >nul 2>&1
    )
    timeout /t 2 /nobreak >nul
    echo   Done.
    echo.
)

:: == Install package (editable) ===============================================
echo Installing dependencies...
python -m pip install -e . -q 2>nul
if %errorlevel% neq 0 (
    echo.
    echo [X] Install failed.
    echo Try manually: python -m pip install -e .
    pause
    exit /b 1
)
echo   Done.
echo.

:: == Start server =============================================================
echo Starting server...
echo.

start /b python -m uvicorn src.api:app --host 127.0.0.1 --port 8000

:: == Wait for server to come up (max 20 sec) ==================================
set ready=0
for /l %%i in (1,1,20) do (
    if !ready! == 0 (
        timeout /t 1 /nobreak >nul
        python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/topics', timeout=2)" >nul 2>&1
        if !errorlevel! == 0 set ready=1
        if !ready! == 0 echo   Waiting... %%i
    )
)

if !ready! == 0 (
    echo.
    echo [X] Server did not start in 20 seconds.
    echo Try manually: python -m uvicorn src.api:app --host 127.0.0.1 --port 8000
    pause
    exit /b 1
)

echo.
echo ========================================================
echo   Server running: http://127.0.0.1:8000
echo   Version: v%VERSION%
echo.
echo   Close this window to stop the server.
echo   Ctrl+C to stop manually.
echo ========================================================
echo.

:: == Open browser =============================================================
start http://127.0.0.1:8000

:: == Keep window open =========================================================
:wait_loop
timeout /t 5 /nobreak >nul
goto wait_loop
