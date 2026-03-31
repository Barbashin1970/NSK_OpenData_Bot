@echo off
chcp 65001 >nul 2>nul
setlocal enabledelayedexpansion

:: Переходим в каталог, где лежит этот .bat файл
cd /d "%~dp0"

:: == Проверка зависимостей ====================================================
echo.
echo ========================================================
echo   Проверка зависимостей...
echo ========================================================
echo.

python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [X] Python не найден!
    echo.
    echo Установите Python 3.11+ с https://www.python.org/downloads/
    echo Важно: поставьте галку "Add Python to PATH" при установке
    echo.
    pause
    exit /b 1
)

for /f "tokens=2" %%v in ('python --version 2^>^&1') do set PYTHON_VERSION=%%v
echo [OK] Python: %PYTHON_VERSION%

pip --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [X] pip не найден!
    echo Переустановите Python с опцией "Add pip"
    pause
    exit /b 1
)
echo [OK] pip

git --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [--] git не найден (опционально)
) else (
    echo [OK] git
)

echo.

:: == Читаем версию из pyproject.toml ==========================================
set VERSION=
for /f "tokens=3 delims= " %%v in ('findstr /r "^version" pyproject.toml') do (
    set VERSION=%%~v
)

:: == Читаем последний git-коммит ==============================================
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

:: == Проверяем, не запущен ли уже сервер ======================================
curl -s http://127.0.0.1:8000/topics >nul 2>&1
if %errorlevel% == 0 (
    echo Server already running - stopping old instance...
    for /f "tokens=5" %%p in ('netstat -ano ^| findstr ":8000.*LISTENING"') do (
        taskkill /PID %%p /F >nul 2>&1
    )
    timeout /t 2 /nobreak >nul
    echo   Done.
    echo.
)

:: == Переустанавливаем пакет (editable) =======================================
echo Installing dependencies (editable mode)...
pip install --force-reinstall -e . -q 2>nul
if %errorlevel% neq 0 (
    echo.
    echo [X] Install failed. Make sure Python 3.11+ and pip are available.
    echo Try manually: pip install -e .
    pause
    exit /b 1
)
echo   Done.
echo.

:: == Запускаем сервер =========================================================
echo Starting server...
echo Close this window to stop the server.
echo.

start /b bot serve

:: == Ждём пока сервер поднимется (максимум 15 сек) ============================
set ready=0
for /l %%i in (1,1,15) do (
    if !ready! == 0 (
        timeout /t 1 /nobreak >nul
        curl -s http://127.0.0.1:8000/topics >nul 2>&1
        if !errorlevel! == 0 set ready=1
        if !ready! == 0 echo   Waiting... ^(%%i^)
    )
)

if !ready! == 0 (
    echo.
    echo [X] Server failed to start in 15 seconds.
    echo Try: pip install -e .
    pause
    exit /b 1
)

echo.
echo ========================================================
echo   Server running: http://127.0.0.1:8000
echo   Version: v%VERSION%
if defined COMMIT (
    echo   Commit: %COMMIT%  %COMMIT_MSG%
)
echo.
echo   Close this window to stop the server.
echo   Ctrl+C to stop manually.
echo ========================================================
echo.

:: == Открываем браузер с cache-bust параметром ================================
for /f %%t in ('powershell -nologo -command "[int](Get-Date -UFormat %%s)"') do set TS=%%t
start http://127.0.0.1:8000?_=%TS%

:: == Держим окно открытым =====================================================
:wait_loop
timeout /t 5 /nobreak >nul
goto wait_loop
