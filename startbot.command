#!/bin/bash
# Двойной клик в Finder запускает бот и открывает браузер

cd ~/NSK_OpenData_Bot

# ── Читаем версию из pyproject.toml ──────────────────────────────────────────
VERSION=$(python3 -c "
import tomllib
with open('pyproject.toml', 'rb') as f:
    print(tomllib.load(f)['project']['version'])
" 2>/dev/null || sed -n 's/^version = \"\(.*\)\"/\1/p' pyproject.toml)

# ── Читаем последний git-коммит ───────────────────────────────────────────────
COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "")
COMMIT_MSG=$(git log -1 --format="%s" 2>/dev/null || echo "")

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Фреймворк Сигма  v${VERSION}"
echo "  Открытые данные и цифровые регламенты городской среды"
if [ -n "$COMMIT" ]; then
  echo "  Коммит: ${COMMIT}  ${COMMIT_MSG}"
fi
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# ── Останавливаем старый сервер если запущен (чтобы не было старого кода) ────
if curl -s http://127.0.0.1:8000/topics > /dev/null 2>&1; then
    echo "⏹  Останавливаю старый сервер..."
    kill $(lsof -ti:8000) 2>/dev/null
    sleep 1
    echo "  Готово."
    echo ""
fi

# ── Переустанавливаем пакет (editable) чтобы всегда использовать свежий код ──
echo "↓ Обновляю установку (editable mode)..."
pip install --force-reinstall -e . -q
echo "  Готово."
echo ""

echo "↓ Запускаю сервер..."
bot serve &
SERVER_PID=$!

# ── Ждём пока сервер поднимется (максимум 15 сек) ────────────────────────────
for i in {1..15}; do
    sleep 1
    if curl -s http://127.0.0.1:8000/topics > /dev/null 2>&1; then
        break
    fi
    echo "  ожидание... ($i)"
done

echo ""
echo "✓ Бот запущен: http://127.0.0.1:8000"
echo "  Версия: v${VERSION}"
if [ -n "$COMMIT" ]; then
  echo "  Коммит: ${COMMIT}  ${COMMIT_MSG}"
fi
echo ""
echo "  Закройте это окно — сервер остановится."
echo "  Ctrl+C — остановить сервер вручную."
echo ""

# Открываем браузер с timestamp чтобы гарантированно обойти навигационный кэш
open "http://127.0.0.1:8000?_=$(date +%s)"

wait $SERVER_PID
