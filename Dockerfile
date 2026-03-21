FROM python:3.11-slim

WORKDIR /app

# Зависимости устанавливаем отдельным слоем — кешируются при git pull без изменения deps
COPY pyproject.toml SKILL.md ./
RUN pip install --no-cache-dir -e . 2>/dev/null || true

# Копируем весь код (кроме data/ — он монтируется как volume)
COPY src/ ./src/
COPY config/ ./config/
COPY pyproject.toml SKILL.md ./

# Финальная установка с полным кодом
RUN pip install --no-cache-dir -e .

# Статические данные (api_keys, cities GeoJSON/JSON, emissions)
# На Railway нет volume — включаем в образ; docker-compose может перекрыть volume'ом
COPY data/ ./data/

EXPOSE 8000

# Railway задаёт $PORT динамически; локально fallback на 8000
CMD bot serve --host 0.0.0.0 --port ${PORT:-8000}
