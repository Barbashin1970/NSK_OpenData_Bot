@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

:: Переходим в каталог, где лежит этот .bat файл (работает из любого места)
cd /d "%~dp0"

:: ── Читаем версию из pyproject.toml ──────────────────────────────────────────
set VERSION=
for /f "tokens=3 delims= " %%v in ('findstr /r "^version" pyproject.toml') do (
    set VERSION=%%~v
)

:: ── Читаем последний git-коммит ───────────────────────────────────────────────
set COMMIT=
set COMMIT_MSG=
for /f "delims=" %%c in ('git rev-parse --short HEAD 2^>nul') do set COMMIT=%%c
for /f "delims=" %%m in ('git log -1 --format^="%%s" 2^>nul') do set COMMIT_MSG=%%m

echo.
echo ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo   Фреймворк Сигма  v%VERSION%
echo   Открытые данные и цифровые регламенты городской среды
if defined COMMIT (
    echo   Коммит: %COMMIT%  %COMMIT_MSG%
)
echo ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo.

:: ── Проверяем, не запущен ли уже сервер ──────────────────────────────────────
curl -s http://127.0.0.1:8000/topics >nul 2>&1
if %errorlevel% == 0 (
    echo Сервер уже запущен — останавливаю старый...
    for /f "tokens=5" %%p in ('netstat -ano ^| findstr ":8000.*LISTENING"') do (
        taskkill /PID %%p /F >nul 2>&1
    )
    timeout /t 2 /nobreak >nul
    echo   Готово.
    echo.
)

:: ── Переустанавливаем пакет (editable) ───────────────────────────────────────
echo Обновляю установку (editable mode)...
pip install --force-reinstall -e . -q 2>nul
if %errorlevel% neq 0 (
    echo.
    echo Ошибка установки. Убедитесь что Python 3.11+ и pip установлены.
    echo Попробуйте: pip install -e .
    pause
    exit /b 1
)
echo   Готово.
echo.

:: ── Запускаем сервер ─────────────────────────────────────────────────────────
echo Запускаю сервер...
echo Закройте это окно — сервер остановится.
echo.

start /b bot serve

:: ── Ждём пока сервер поднимется (максимум 15 сек) ────────────────────────────
set ready=0
for /l %%i in (1,1,15) do (
    if !ready! == 0 (
        timeout /t 1 /nobreak >nul
        curl -s http://127.0.0.1:8000/topics >nul 2>&1
        if !errorlevel! == 0 set ready=1
        if !ready! == 0 echo   Ожидание... ^(%%i^)
    )
)

if !ready! == 0 (
    echo.
    echo Не удалось запустить сервер за 15 секунд.
    echo Убедитесь что установлены зависимости: pip install -e .
    pause
    exit /b 1
)

echo.
echo Сервер запущен: http://127.0.0.1:8000
echo   Версия: v%VERSION%
if defined COMMIT (
    echo   Коммит: %COMMIT%  %COMMIT_MSG%
)
echo.
echo   Закройте это окно — сервер остановится.
echo   Ctrl+C — остановить сервер вручную.
echo.

:: ── Открываем браузер с cache-bust параметром ────────────────────────────────
for /f %%t in ('powershell -nologo -command "[int](Get-Date -UFormat %%s)"') do set TS=%%t
start http://127.0.0.1:8000?_=%TS%

:: ── Держим окно открытым ─────────────────────────────────────────────────────
:wait_loop
timeout /t 5 /nobreak >nul
goto wait_loop
