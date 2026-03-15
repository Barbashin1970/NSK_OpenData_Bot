FROM python:3.11-slim

WORKDIR /app

# Зависимости устанавливаем отдельным слоем — кешируются при git pull без изменения deps
COPY pyproject.toml ./
RUN pip install --no-cache-dir -e . 2>/dev/null || true

# Копируем весь код (кроме data/ — он монтируется как volume)
COPY src/ ./src/
COPY config/ ./config/
COPY pyproject.toml ./

# Финальная установка с полным кодом
RUN pip install --no-cache-dir -e .

# data/ монтируется снаружи (docker-compose volumes) — не включаем в образ
# Это позволяет обновлять данные без пересборки образа

EXPOSE 8000

CMD ["bot", "serve", "--host", "0.0.0.0", "--port", "8000"]
