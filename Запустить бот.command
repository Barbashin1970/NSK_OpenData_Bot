#!/bin/bash
# Двойной клик в Finder запускает бот и открывает браузер

cd ~/NSK_OpenData_Bot

# Проверяем, не запущен ли уже сервер
if curl -s http://127.0.0.1:8000/topics > /dev/null 2>&1; then
    echo "✓ Бот уже запущен — открываю браузер..."
    open http://127.0.0.1:8000
    exit 0
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  NSK OpenData Bot"
echo "  Открытые данные мэрии Новосибирска"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "↓ Запускаю сервер..."

bot serve &
SERVER_PID=$!

# Ждём пока сервер поднимется (максимум 10 сек)
for i in {1..10}; do
    sleep 1
    if curl -s http://127.0.0.1:8000/topics > /dev/null 2>&1; then
        break
    fi
    echo "  ожидание... ($i)"
done

echo ""
echo "✓ Бот запущен: http://127.0.0.1:8000"
echo ""
echo "  Закройте это окно — сервер остановится."
echo "  Ctrl+C — остановить сервер вручную."
echo ""

open http://127.0.0.1:8000

wait $SERVER_PID
