# Руководство аналитика: от получения лицензии до запуска на сервере мэрии

## Что вы получаете

City OpenData Bot — готовый веб-интерфейс для работы с открытыми данными города.
После настройки жители могут задавать вопросы на русском языке и получать актуальные
ответы: сколько школ в каком районе, где ближайшая аптека, текущие отключения ЖКХ.

Система **не использует ИИ в реальном времени** — только правила и SQL-запросы.
Это означает: быстрая работа, предсказуемые ответы, минимальные требования к серверу.

---

## Часть 1. Установка на локальный компьютер

### Что нужно заранее

| Программа | Версия | Где взять |
|---|---|---|
| Python | 3.11 или 3.12 | python.org |
| Git | любая | git-scm.com |
| Текстовый редактор | — | VS Code, Notepad++ |

Интернет-соединение для первой загрузки данных.

### Шаг 1 — Скачать репозиторий

Вы получили ссылку на репозиторий. Откройте терминал (macOS: Терминал, Windows: Git Bash
или PowerShell) и выполните:

```bash
git clone <ссылка-на-репозиторий> city-bot
cd city-bot
```

### Шаг 2 — Установить бот

```bash
pip install -e .
```

Команда установит все зависимости и сделает доступной команду `bot`.
Проверка:
```bash
bot --help
```

Должен появиться список доступных команд.

### Шаг 3 — Первый запуск (macOS)

Дважды кликните `startbot.command` в Finder.
Браузер откроется автоматически на `http://127.0.0.1:8000`.

Или из терминала:
```bash
bot serve
```

### Шаг 3 — Первый запуск (Windows)

```bash
bot serve
```

Откройте браузер: `http://127.0.0.1:8000`

---

## Часть 2. Настройка вашего города

### Шаг 4 — Создать профиль города

В папке `config/` уже есть пример для Омска: `city_profile_omsk.yaml`.
Скопируйте его под свой город:

```bash
cp config/city_profile_omsk.yaml config/city_profile_kemerovo.yaml
```

Откройте файл в редакторе и заполните:

```yaml
city:
  id: "kemerovo"                    # латиница, без пробелов
  name: "Кемерово"
  name_genitive: "Кемерово"         # родительный падеж: «данные Кемерово»
  name_prepositional: "Кемерове"    # предложный: «в Кемерове»
  slug: "kemerovo"
  timezone: "Asia/Krasnoyarsk"      # IANA timezone
  utc_offset: 7

  center:
    lat: 55.3333
    lon: 86.0833

  bbox:                             # граница города для поиска на карте
    lat_min: 55.25
    lat_max: 55.42
    lon_min: 85.88
    lon_max: 86.25
```

Заполните также разделы `districts` (районы города), `district_coords`
(координаты центров районов), `ecology_stations` (по одной точке на район).

Подсказка по координатам: откройте maps.google.com, найдите центр нужного
района, нажмите правой кнопкой → «Что здесь?» — появятся координаты.

### Шаг 5 — Запустить с вашим профилем

```bash
CITY_PROFILE=city_profile_kemerovo bot serve
```

macOS/Linux используют `CITY_PROFILE=...` перед командой.
Windows PowerShell:
```powershell
$env:CITY_PROFILE="city_profile_kemerovo"; bot serve
```

Откройте браузер — бот теперь настроен на ваш город.

### Шаг 6 — Проверка профиля тестами

```bash
python -m pytest tests/test_city_profile.py -v
```

Тесты проверят структуру вашего YAML: все обязательные поля, правильность
координат, соответствие районов и станций мониторинга.

---

## Часть 3. Загрузка данных

### Типы данных и откуда их брать

| Тип | Источник | Формат |
|---|---|---|
| Открытые данные города | Портал opendata вашего города (если есть) | CSV |
| Выбросы 2-ТП Воздух | rpn.gov.ru → ваш регион | Требует конвертации в JSON |
| Тепловые источники | Оператор теплоснабжения (СГК, ТГК и др.) | GeoJSON |
| Метро | Официальный сайт метро / Wikipedia | JSON (ввод вручную) |
| Аэропорт | Сайт аэропорта | JSON (ввод вручную) |

### Шаг 7 — Загрузить данные выбросов (пример)

Скачайте отчёт 2-ТП Воздух для вашего региона с rpn.gov.ru.
Конвертируйте в формат системы (смотрите `config/canonical_schemas.yaml`
раздел `emissions`):

```json
{
  "year": 2024,
  "form": "2-ТП Воздух",
  "source": "Росприроднадзор",
  "municipalities": [
    {
      "id": "kemerovo_city",
      "name": "г. Кемерово",
      "lat": 55.333,
      "lon": 86.083,
      "vsego_t": 85000,
      "so2_t": 12000,
      "nox_t": 8500,
      "co_t": 45000,
      "data_status": "report"
    }
  ]
}
```

Сохраните в `data/cities/kemerovo/emissions_2tp.json`.

Включите в `city_profile_kemerovo.yaml`:
```yaml
static_datasets:
  emissions:
    enabled: true
    file: "data/cities/kemerovo/emissions_2tp.json"
    scope: "Кемеровская область"
    year: 2024
```

Перезапустите бот — данные появятся автоматически.

### Шаг 8 — Подключить CSV-данные открытого портала

Если у вашего города есть портал открытых данных с CSV-файлами,
откройте `config/datasets.yaml` и добавьте источник для своего города:

```yaml
datasets:
  schools:
    name: "Школы"
    data_url: "https://opendata.kemerovo.ru/schools.csv"  # URL вашего портала
    format: csv
    fields:
      district_col: Rayon    # название колонки «район» в вашем CSV
      name_col: SchName      # название колонки «название» в вашем CSV
      display_cols:
        - SchName
        - Rayon
        - Address
```

Список канонических имён полей — в `config/canonical_schemas.yaml`.

---

## Часть 4. Тестирование перед запуском на сервере

### Шаг 9 — Полная проверка

```bash
# Тест профиля города
CITY_PROFILE=city_profile_kemerovo python -m pytest tests/test_city_profile.py -v

# Все тесты системы
python -m pytest tests/ -q
```

### Шаг 10 — Проверка бота в браузере

Запустите бот локально, откройте `http://127.0.0.1:8000` и проверьте:

- [ ] Название вашего города отображается в интерфейсе
- [ ] Запрос «сколько школ» возвращает данные
- [ ] Карта центрирована на ваш город
- [ ] Нет ошибок в консоли браузера (F12 → Console)

---

## Часть 5. Деплой на сервер мэрии

### Технические требования к серверу

| Параметр | Минимум | Рекомендуется |
|---|---|---|
| CPU | 1 ядро | 2 ядра |
| RAM | 512 МБ | 2 ГБ |
| Диск | 10 ГБ | 50 ГБ |
| ОС | Ubuntu 20.04 | Ubuntu 22.04 LTS |
| Сеть | 10 Мбит/с | 100 Мбит/с |
| Доступ | SSH + исходящий HTTPS | + входящий 80/443 |

Бот не использует ML-вычисления и работает на минимальном железе.
DuckDB встроен — отдельный сервер БД не нужен.

Типичная нагрузка: 50–200 запросов в день → 1 ядро, 512 МБ RAM хватит
с большим запасом. При 1000+ запросов в сутки рекомендуется 2 ядра, 2 ГБ.

### Шаг 11 — Установить Docker на сервер

```bash
# Ubuntu 22.04
sudo apt update && sudo apt install -y docker.io docker-compose
sudo systemctl enable docker && sudo systemctl start docker
sudo usermod -aG docker $USER
```

Выйдите и войдите снова чтобы применились права группы.

### Шаг 12 — Скопировать файлы на сервер

```bash
# С вашего компьютера:
scp -r city-bot/ user@server-ip:/opt/city-bot/
```

Или клонировать репозиторий прямо на сервере:
```bash
git clone <ссылка> /opt/city-bot
cd /opt/city-bot
```

### Шаг 13 — Настроить окружение

Создайте файл `/opt/city-bot/.env`:
```env
CITY_PROFILE=city_profile_kemerovo
TWOGIS_API_KEY=ваш_ключ_2gis
```

Ключ 2GIS нужен для карты и геокодирования. Получить бесплатно: platform.2gis.ru

### Шаг 14 — Запустить через Docker

```bash
cd /opt/city-bot
docker-compose up -d
```

Проверка что работает:
```bash
docker-compose ps          # статус контейнеров
docker-compose logs -f     # логи в реальном времени
curl http://localhost:8000/topics  # API отвечает
```

### Шаг 15 — Настроить Nginx (HTTPS)

Установить Nginx и Certbot:
```bash
sudo apt install -y nginx certbot python3-certbot-nginx
```

Создать конфигурацию `/etc/nginx/sites-available/city-bot`:
```nginx
server {
    server_name bot.kemerovo.ru;  # ваш домен

    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_read_timeout 30s;
    }
}
```

Включить и получить сертификат:
```bash
sudo ln -s /etc/nginx/sites-available/city-bot /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
sudo certbot --nginx -d bot.kemerovo.ru
```

После этого бот доступен по `https://bot.kemerovo.ru`.

### Шаг 16 — Автозапуск при перезагрузке сервера

Docker Compose уже настроен на `restart: unless-stopped`.
Проверьте что Docker запускается при старте ОС:
```bash
sudo systemctl is-enabled docker  # должно быть "enabled"
```

### Шаг 17 — Обновление данных и кода

Обновление данных (добавили новый CSV-датасет):
```bash
cd /opt/city-bot
# скопировать новый файл данных в data/
docker-compose restart bot
```

Обновление кода (вышла новая версия):
```bash
cd /opt/city-bot
git pull
docker-compose build --no-cache
docker-compose up -d
```

---

## Часть 6. Хранение данных и резервные копии

Все данные бота находятся в папке `data/`:
- `data/cache.db` — кеш DuckDB с загруженными CSV-данными
- `data/cities/<city_id>/` — ваши статические датасеты
- `data/api_keys.json` — ключи API

Резервная копия (выполнять по расписанию через cron):
```bash
# Добавить в crontab: crontab -e
0 3 * * * tar -czf /backup/city-bot-$(date +%Y%m%d).tar.gz /opt/city-bot/data/
```

Файлы `data/cities/` и `config/city_profile_*.yaml` храните также в git-репозитории
вашего города — это гарантия восстановления после сбоя.

---

## Быстрая проверка работоспособности

```bash
# Сервер отвечает?
curl https://bot.kemerovo.ru/topics

# Данные загружены?
curl "https://bot.kemerovo.ru/ask?q=сколько+школ"

# Статус кеша?
curl https://bot.kemerovo.ru/status
```

---

## Помощь и поддержка

- Проблема с профилем города → проверьте: `python -m pytest tests/test_city_profile.py -v`
- Данные не обновляются → проверьте логи: `docker-compose logs bot`
- Вопросы по структуре данных → смотрите `config/canonical_schemas.yaml`
- Технические вопросы → issues в репозитории проекта
