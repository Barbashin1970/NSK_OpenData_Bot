# Артефакты отчёта о НИР СИМА.НИР.001-2026

Опись файлов, на которых стоят числа отчёта `docs/NIR-SIMA-PROGNOZ-BRIGAD-2026.md` (ред. 1.2).
Контрольные суммы — `MANIFEST.sha256`; проверка: `(cd docs/nir-sima-2026/artefakty && shasum -a 256 -c MANIFEST.sha256)`.

| Файл | Что это | Как получен |
|---|---|---|
| `sima-zhkh-dataset-2026-11-16.tar.gz` | Набор данных: суточная таблица «район × услуга» (11 850 строк), городская (1 185), сырые снимки (36 276), описание набора; внутри — `SHA256SUMS` четырёх файлов | `scripts/zhkh_calibration/build_dataset.py` из трёх бэкапов СИМА (16.11, 19.08, 31.03.2026); архив собран без времени и владельцев файлов |
| `power-forecast-prod-2026-11-17T1432Z.json` | Ответ прода `GET /power/forecast` — источник табл. 10 отчёта (поле `backtest`) | `curl -s https://nsk-opendata-bot.up.railway.app/power/forecast`, 2026-11-17 14:32 UTC; поля `backtest` совпадают с ответом 12:29 UTC, по которому составлена таблица |
| `protokoly/*.txt` | Вывод каждого скрипта `scripts/zhkh_calibration/` на архиве набора | команды — в `scripts/zhkh_calibration/README.md`; Python 3.12.0, pandas 2.3.3, numpy 2.4.3 |

Содержимое архива (суммы — в `SHA256SUMS` внутри архива):

| Файл | SHA-256 |
|---|---|
| `README.md` | `dcb91465c95fa371207504e501d77427bb192073bf4e36b1c581bb9ce09e4e1f` |
| `zhkh_daily_district.csv` | `7df52e973e8a3d3a45023101ace153edb11f65e27b40da770fb47730e55d7504` |
| `zhkh_daily_city.csv` | `832c80e91a54665267f46d01116cce2f940f77e49dbfb56c427b23f88eaec61e` |
| `zhkh_snapshots.csv` | `f8b7b622e7eb3243c51de0aa28d93b79eb78f0607b1649697ab491df4050a01f` |

**Чего здесь нет.** Бэкапов СИМА: в них служебные данные сервера, они хранятся у руководителя
работы. Выпусков прогноза: они сохраняются с 17.11.2026 в базе СИМА (таблицы
`power_forecast_issues`, `power_forecast_issue_cells`) и войдут в следующую редакцию вместе
с проверкой по выданному (бэклог 13).

Данные — открытая сводка отключений мэрии Новосибирска (`051.novo-sibirsk.ru`), сгруппированная
по районам; адресов и персональных данных в наборе нет.
