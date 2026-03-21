# BACKLOG — Индекс мобильных выбросов (Вариант 2, полная реализация)

> Основа: PDF «Алгоритм индекса городского загрязнения с учётом транспорта и метеоусловий»
> Текущий статус: Вариант 1 (демо) реализован — JS-расчёт в index.html, третья вкладка экорейтинга
> Дата создания: 2026-03-21

---

## Цель

Перенести расчёт I_mobile на бэкенд (Python), подключить живые данные (traffic_index, ecology),
добавить API endpoint, расширить данные по городам.

---

## Шаг 1 — Данные городов (`data/cities_mobility.json`)

Создать JSON-файл с полными параметрами для 21 города:

```json
{
  "version": "1.0",
  "source": "ГИБДД, Росстат, Яндекс.Пробки, OSM",
  "cities": [
    {
      "id": "novosibirsk",
      "city": "Новосибирск",
      "population": 1620000,
      "car_count": 550800,
      "avg_jam_score": 5.2,
      "has_metro": true,
      "has_tram": true,
      "has_trolleybus": true,
      "ept_route_km_per_100k": 8.5,
      "bus_lane_km_per_100k": 2.1,
      "avg_wind_speed": 3.2,
      "calm_days_share": 0.12,
      "is_basin": false
    }
  ]
}
```

**Новые поля (vs Вариант 1):**
- `car_count` — абсолютное число авто (сейчас только `cars_per_1000`)
- `ept_route_km_per_100k` — длина линий электро-ОТ на 100к жителей
- `bus_lane_km_per_100k` — длина выделенных полос на 100к жителей
- `calm_days_share` — доля штилевых дней (V < 2 м/с)

**Источники данных:**
- Население: Росстат, муниципальные справочники
- Автомобили: ГИБДД/Росстат (авто на 1000 жителей × население)
- Пробки: Яндекс.Пробки средний дневной балл (усредн. по будням)
- Электро-ОТ: OSM (Overpass API) — длины линий трамвая/метро/троллейбуса
- Выделенные полосы: OSM `lanes:bus=*` или `highway=busway`
- Климат: Open-Meteo Historical API — средняя скорость ветра + штили

---

## Шаг 2 — Python-модуль (`src/mobile_index.py`)

```python
from dataclasses import dataclass

@dataclass
class CityMobilityInputs:
    name: str
    population: int
    car_count: int
    avg_jam_score: float        # 0..10
    has_metro: bool
    has_tram: bool
    has_trolleybus: bool
    ept_route_km_per_100k: float
    bus_lane_km_per_100k: float
    avg_wind_speed: float       # м/с
    calm_days_share: float      # 0..1
    is_basin: bool

@dataclass
class GlobalNorms:
    d_cars_min: float
    d_cars_max: float
    v_wind_min: float
    v_wind_max: float
    p_calm_min: float
    p_calm_max: float
    p_raw_min: float
    p_raw_max: float

@dataclass
class WeightsConfig:
    # Загружается из config/rules/mobile_index_rules.yaml
    ...

@dataclass
class MobileIndexResult:
    I_mobile: float           # 0-100
    I_total: float            # 0-100
    S_auto: float             # 0-1
    S_PT: float               # 0-1
    F_PT: float
    F_meteo: float
    P_raw: float
    breakdown: dict           # детализация по факторам

def load_cities() -> list[CityMobilityInputs]:
    """Загрузка из data/cities_mobility.json, lru_cache"""
    ...

def precompute_norms(cities: list[CityMobilityInputs]) -> GlobalNorms:
    """Предвычисление min/max по всей выборке"""
    ...

def compute_mobile_index(
    city: CityMobilityInputs,
    norms: GlobalNorms,
    weights: WeightsConfig
) -> MobileIndexResult:
    """Чистая функция, без побочных эффектов. Возвращает I_mobile 0-100."""
    ...

def compute_total_index(
    I_stationary: float,
    I_mobile: float,
    w_s: float = 0.6,
    w_m: float = 0.4
) -> float:
    """I_total = w_s * I_stationary + w_m * I_mobile"""
    ...
```

---

## Шаг 3 — Расширение регламента

Файл `config/rules/mobile_index_rules.yaml` уже создан (Вариант 1).
Для Варианта 2 добавить:

```yaml
# Дополнительные параметры для полного расчёта
public_transport:
  beta_ePT: 0.25            # вес длины линий электро-ОТ
  beta_bus: 0.20             # вес выделенных полос

meteo:
  p_calm_min: 0.05           # минимум доли штилей по выборке
  p_calm_max: 0.35           # максимум доли штилей
```

---

## Шаг 4 — API endpoints

В `src/routes/ecology.py` или новый `src/routes/eco_index.py`:

```
GET /eco-index/mobile          → I_mobile для всех городов (JSON array)
GET /eco-index/total           → I_total для всех городов (JSON array)
GET /eco-index/city/{city_id}  → Детальная декомпозиция одного города
GET /eco-index/city/{city_id}/live → I_mobile с живым J (traffic_index) и ветром (ecology)
```

**Живой расчёт для текущего города:**
- J (балл пробок) → берётся из `traffic_index.compute_traffic_index()` (уже есть)
- Ветер → берётся из `ecology_cache.query_current()` (уже есть)
- Подставляются вместо статичных `avg_jam` и `avg_wind_speed`

---

## Шаг 5 — Фронтенд (расширение)

1. Вместо JS-расчёта → fetch `/eco-index/total`
2. Клик по городу → popup/modal с декомпозицией (бар-чарт по факторам)
3. Для "своего" города → подсветка + живой I_mobile (обновляется каждые 15 мин)
4. Tooltip на прогресс-барах с пояснением формулы

---

## Шаг 6 — Калибровка

1. Взять 8-10 городов с известным рейтингом Росгидромета
2. Подобрать веса (w_s, w_m, alpha, beta, gamma, delta) так чтобы I_total
   качественно совпадал с официальными рейтингами
3. Зафиксировать в WeightsConfig (YAML)
4. Добавить unit-тесты с reference-значениями

---

## Оценка трудозатрат

| Шаг | Описание | Объём |
|-----|----------|-------|
| 1   | JSON данные 21 города | ~2ч (сбор данных) |
| 2   | Python-модуль | ~150 строк |
| 3   | Расширение YAML | ~20 строк |
| 4   | API endpoints | ~80 строк |
| 5   | Фронтенд доработка | ~100 строк JS |
| 6   | Калибровка + тесты | ~2ч |

---

## Зависимости

- `traffic_index.py` — уже существует, даёт живой J
- `ecology_cache.py` — уже существует, даёт живой ветер
- `config/rules/mobile_index_rules.yaml` — уже создан (Вариант 1)
- `ECO_INDEX_CITIES` в index.html — уже расширен транспортными данными (Вариант 1)
