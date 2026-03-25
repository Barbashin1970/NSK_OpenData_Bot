FROM python:3.11-slim

WORKDIR /app

# Зависимости устанавливаем отдельным слоем — кешируются при git pull без изменения deps
COPY pyproject.toml SKILL.md ./
RUN pip install --no-cache-dir -e . 2>/dev/null || true

# Копируем весь код
COPY src/ ./src/
COPY config/ ./config/
COPY pyproject.toml SKILL.md ./

# Финальная установка с полным кодом
RUN pip install --no-cache-dir -e .

# Seed-данные: копируем в отдельную директорию внутри образа.
# При старте скрипт синхронизирует недостающие файлы из _seed → /app/data (Volume).
# Это решает проблему "пустой Volume при первом деплое".
COPY data/ ./_seed_data/

EXPOSE 8000

CMD bot serve --host 0.0.0.0 --port ${PORT:-8000}
