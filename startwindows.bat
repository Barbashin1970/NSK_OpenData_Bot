@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

cd /d %USERPROFILE%\NSK_OpenData_Bot

:: Проверяем, не запущен ли уже сервер
curl -s http://127.0.0.1:8000/topics >nul 2>&1
if %errorlevel% == 0 (
    echo Бот уже запущен — открываю браузер...
    start http://127.0.0.1:8000
    exit /b 0
)

echo ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo   NSK OpenData Bot
echo   Открытые данные мэрии Новосибирска
echo ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo.
echo Запускаю сервер...
echo Закройте это окно — сервер остановится.
echo.

start /b bot serve

:: Ждём пока сервер поднимется (максимум 10 сек)
set ready=0
for /l %%i in (1,1,10) do (
    if !ready! == 0 (
        timeout /t 1 /nobreak >nul
        curl -s http://127.0.0.1:8000/topics >nul 2>&1
        if !errorlevel! == 0 set ready=1
        if !ready! == 0 echo   Ожидание... ^(%%i^)
    )
)

if !ready! == 0 (
    echo.
    echo Не удалось запустить сервер за 10 секунд.
    echo Убедитесь что установлены зависимости: pip install -e .
    pause
    exit /b 1
)

echo.
echo Бот запущен: http://127.0.0.1:8000
echo.
start http://127.0.0.1:8000

:: Держим окно открытым — закрытие окна остановит сервер
:wait_loop
timeout /t 5 /nobreak >nul
goto wait_loop
