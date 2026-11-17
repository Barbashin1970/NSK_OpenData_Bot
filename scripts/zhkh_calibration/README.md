# Эксперименты калибровки прогноза ЖКХ

Разбор и выводы — `docs/ZHKH-KALIBROVKA-RAZBOR.md`, отчёт — `docs/NIR-SIMA-PROGNOZ-BRIGAD-2026.md`.
Вход — таблица набора данных `zhkh_daily_district.csv` (и `zhkh_snapshots.csv` для
`weekday_check.py`), собранная `build_dataset.py` из бэкапов СИМА. Набор 2026-11-16 лежит
архивом с контрольными суммами в `docs/nir-sima-2026/artefakty/`; вывод каждого скрипта на нём —
`docs/nir-sima-2026/artefakty/protokoly/`. Нужны pandas и numpy (замеры: Python 3.12.0,
pandas 2.3.3, numpy 2.4.3).

```bash
A=docs/nir-sima-2026/artefakty
(cd $A && shasum -a 256 -c MANIFEST.sha256)
D=$(mktemp -d) && tar -xzf $A/sima-zhkh-dataset-2026-11-16.tar.gz -C $D && D=$D/sima-zhkh-dataset-2026-11-16
(cd $D && shasum -a 256 -c SHA256SUMS)
python3 -W ignore scripts/zhkh_calibration/calibration_experiment.py $D   # Брайер по сезонам и месяцам, бутстреп (по неделям и блоками по 2 недели)
python3 -W ignore scripts/zhkh_calibration/calibration_followup.py   $D   # память в пределах режима для тепла, подъём перегруза
python3 -W ignore scripts/zhkh_calibration/calibration_selection.py  $D   # отбор на выпусках до 12.10, проверка на выпусках 19.10 и 26.10
python3 -W ignore scripts/zhkh_calibration/weekday_check.py          $D   # профиль недели, сутки UTC против НСК, знаменатель
python3 -W ignore scripts/zhkh_calibration/reliability.py            $D   # калибровка вероятностей: обещано / сбылось
python3 -W ignore scripts/zhkh_calibration/heldout_reliability.py    $D   # надёжность v1 и v2 на отложенных выпусках 19.10 и 26.10
python3 -W ignore scripts/zhkh_calibration/regime_variants.py        $D   # режимы: температура / даты мэрии / четыре
```

Даты режимов (28.04 и 11.10.2026) получены в `calibration_experiment.py` правилом
«5 суток подряд выше / ниже +8 °C» по медиане температуры районов из экологического архива.

Пересборка набора из бэкапов (бэкапы есть только у руководителя работы):

```bash
python3 -I scripts/zhkh_calibration/build_dataset.py <папка вывода> \
    <бэкап 2026-11-16> <бэкап 2026-08-19> <бэкап 2026-03-31>
```
