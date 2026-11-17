# Эксперименты калибровки прогноза ЖКХ

Разбор и выводы — `docs/ZHKH-KALIBROVKA-RAZBOR.md`. Вход — таблица набора данных
`zhkh_daily_district.csv` (и `zhkh_snapshots.csv` для `weekday_check.py`), собранная
`build_dataset.py` из бэкапов СИМА. Нужны pandas и numpy.

```bash
D=~/Downloads/sima-zhkh-dataset-2026-11-16
python3 -W ignore scripts/zhkh_calibration/calibration_experiment.py $D   # Брайер по сезонам и месяцам, бутстреп
python3 -W ignore scripts/zhkh_calibration/calibration_followup.py   $D   # память в пределах режима для тепла, подъём перегруза
python3 -W ignore scripts/zhkh_calibration/calibration_selection.py  $D   # отбор на неделях до 12.10, проверка на 19.10–9.11
python3 -W ignore scripts/zhkh_calibration/weekday_check.py          $D   # профиль недели, сутки UTC против НСК, знаменатель
python3 -W ignore scripts/zhkh_calibration/reliability.py            $D   # калибровка вероятностей: обещано / сбылось
python3 -W ignore scripts/zhkh_calibration/regime_variants.py        $D   # режимы: температура / даты мэрии / четыре
```

Даты режимов (28.04 и 11.10.2026) получены в `calibration_experiment.py` правилом
«5 суток подряд выше / ниже +8 °C» по медиане температуры районов из экологического архива.
