"""Форматирование и вывод результатов (rich + tabulate)."""

from datetime import datetime, timezone
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

from .fetcher import load_meta
from .planner import Plan
from .registry import get_dataset, load_registry
from .cache import get_table_info
from .fetcher import is_stale

console = Console()

# Человекочитаемые названия операций
OP_NAMES = {
    "COUNT": "Подсчёт",
    "TOP_N": "Топ-N",
    "GROUP": "Группировка по районам",
    "FILTER": "Фильтрация/список",
    "INFO": "Справка о темах",
    "POWER_STATUS": "Текущий статус отключений",
    "POWER_TODAY": "Отключения сегодня",
    "POWER_PLANNED": "Плановые отключения",
    "POWER_HISTORY": "История отключений",
}

_GROUP_LABELS = {
    "planned": "[yellow]Плановые[/yellow]",
    "active":  "[red]Активные[/red]",
}
_GROUP_LABELS_PLAIN = {
    "planned": "Плановые",
    "active":  "Активные",
}

# Переводы названий колонок для отображения
COL_LABELS: dict[str, str] = {
    "AdrDistr": "Район",
    "AdrStreet": "Адрес",
    "_district": "Район",
    "_street": "Улица",
    "_name": "Название",
    "OstName": "Остановка",
    "OuName": "Организация",
    "Pavilion": "Павильон",
    "Marshryt": "Маршруты",
    "ParkType": "Тип парковки",
    "NumMashMest": "Мест",
    "NumMashMestInv": "Мест (инв.)",
    "Regim": "Режим",
    "ParkOhrana": "Охрана",
    "ParkStatus": "Статус",
    "FullName": "Полное название",
    "ShortName": "Краткое название",
    "Rayon": "Район",
    "Ulica": "Улица",
    "Dom": "Дом",
    "TelUch": "Телефон",
    "Site": "Сайт",
    "Phone": "Телефон",
    "AdrDom": "Дом",
    "Regimrab": "Режим работы",
    "Regimraboti": "Режим работы",
    "Gruppy": "Классы",
    "Mesta": "Мест",
    "BiblName": "Краткое название",
    "BiblFName": "Полное название",
    "ParkName": "Парк",
    "ParkShortName": "Краткое название",
    "District": "Район",
    "Street": "Улица",
    "House": "Дом",
    "VidSport": "Вид спорта",
    "Comment": "Примечание",
    "Type": "Тип",
    "Name": "Название",
    "AdrStr": "Улица",
    "район": "Район",
    "количество": "Кол-во",
    "cnt": "Итого",
    "NazvUch": "Организация",
    "TelUch": "Телефон",
    "FIORuk": "Руководитель",
    "RukPhone": "Тел. руковод.",
    "VidSporta": "Вид спорта",
    "Regim": "Режим",
    "Rayon": "Район",
    "Ulica": "Улица",
    "Dom": "Дом",
    "Index": "Индекс",
}


def _label(col: str) -> str:
    return COL_LABELS.get(col, col)


def _truncate(val: Any, max_len: int = 55) -> str:
    s = str(val) if val is not None else ""
    if len(s) > max_len:
        return s[: max_len - 1] + "…"
    return s


def render_topics() -> None:
    """Выводит таблицу поддерживаемых тем."""
    registry = load_registry()
    meta = load_meta()

    table = Table(
        title="Поддерживаемые темы NSK OpenData Bot",
        box=box.ROUNDED,
        show_lines=True,
    )
    table.add_column("Тема", style="bold cyan", no_wrap=True)
    table.add_column("Название", style="white")
    table.add_column("Строк", justify="right", style="green")
    table.add_column("Обновлён", justify="center")
    table.add_column("Статус кэша", justify="center")
    table.add_column("Источник (passport)", style="dim")

    for topic_id, ds in registry.items():
        m = meta.get(topic_id, {})
        rows_count = m.get("rows", "—")
        last_updated = m.get("last_updated", "")
        if last_updated:
            try:
                dt = datetime.fromisoformat(last_updated)
                last_updated_str = dt.strftime("%d.%m.%Y %H:%M")
            except Exception:
                last_updated_str = last_updated[:16]
        else:
            last_updated_str = "нет данных"

        stale = is_stale(topic_id, ds.get("ttl_hours", 24))
        status = "[red]Устарел[/red]" if stale else "[green]Актуален[/green]"

        table.add_row(
            topic_id,
            ds.get("name", ""),
            str(rows_count),
            last_updated_str,
            status,
            ds.get("passport_url", "")[:60],
        )

    console.print(table)
    console.print("\n[dim]Используйте: bot update --all  для обновления всех данных[/dim]")


def render_ask_result(
    query_text: str,
    route_result: Any,
    plan: Plan,
    exec_result: dict,
    cache_info: dict,
) -> None:
    """Выводит результат команды ask в форматированном виде."""
    # Заголовок
    console.rule("[bold blue]Ответ NSK OpenData Bot[/bold blue]")

    # Метаданные запроса
    ds_name = ""
    if plan.topic:
        ds = get_dataset(plan.topic)
        ds_name = ds.get("name", plan.topic) if ds else plan.topic

    meta_lines = [
        f"[bold]Вопрос:[/bold]  {query_text}",
        f"[bold]Тема:[/bold]    {ds_name} [dim]({plan.topic})[/dim]"
        + (f"  [dim]← уверенность {route_result.confidence:.0%}[/dim]" if route_result else ""),
    ]
    _audience_labels = {"children": "дети", "adults": "взрослые"}
    audience = plan.extra_filters.get("audience") if plan.extra_filters else None
    if plan.district or plan.street or audience:
        filters = []
        if plan.district:
            if plan.sub_district:
                filters.append(f"подрайон={plan.sub_district} (→ {plan.district})")
            else:
                filters.append(f"район={plan.district}")
        if plan.street:
            filters.append(f"улица={plan.street}")
        if audience:
            filters.append(f"аудитория={_audience_labels.get(audience, audience)}")
        meta_lines.append(f"[bold]Параметры:[/bold] {', '.join(filters)}")
    if plan.limit and plan.operation in ("TOP_N", "FILTER"):
        meta_lines.append(f"[bold]Лимит:[/bold]   {plan.limit} строк")

    meta_lines.append(f"[bold]Операция:[/bold] {OP_NAMES.get(plan.operation, plan.operation)}")

    # Информация о кэше
    last_upd = cache_info.get("last_updated", "")
    if last_upd:
        try:
            dt = datetime.fromisoformat(last_upd)
            last_upd_str = dt.strftime("%d.%m.%Y %H:%M UTC")
        except Exception:
            last_upd_str = last_upd[:16]
    else:
        last_upd_str = "нет данных"
    rows_count = cache_info.get("rows", "?")
    meta_lines.append(f"[bold]Источник:[/bold] локальный кэш | обновлён {last_upd_str} | {rows_count} строк")

    console.print("\n".join(meta_lines))
    console.print()

    # Ошибка
    if "error" in exec_result:
        console.print(f"[red]Ошибка: {exec_result['error']}[/red]")
        return

    # Ответ
    op = exec_result.get("operation")
    rows = exec_result.get("rows", [])
    cols = exec_result.get("columns", [])
    count = exec_result.get("count", 0)
    limit = exec_result.get("limit", 0)

    if op == "COUNT":
        console.print(Panel(
            f"[bold green]Итого: {count}[/bold green]",
            title="Результат",
            border_style="green",
        ))

    elif op == "GROUP":
        console.print(f"[bold green]Всего объектов: {count}[/bold green]")
        _render_table(rows, cols)

    elif op in ("TOP_N", "FILTER"):
        shown = len(rows)
        if count > limit:
            console.print(f"[bold green]Найдено: {count}[/bold green] (показано: {shown})")
        else:
            console.print(f"[bold green]Найдено: {count}[/bold green]")
        _render_table(rows, cols)

    note = exec_result.get("note")
    if note:
        console.print(f"\n[yellow]ℹ {note}[/yellow]")

    if plan.sub_district:
        console.print(
            f"\n[dim]ℹ {plan.sub_district} входит в {plan.district}."
            f" Данные в открытых источниках хранятся на уровне района.[/dim]"
        )

    console.rule()


def _render_table(rows: list[dict], columns: list[str]) -> None:
    """Рисует таблицу результатов с помощью rich."""
    if not rows:
        console.print("[yellow]Данные не найдены[/yellow]")
        return

    table = Table(box=box.SIMPLE_HEAD, show_lines=False)
    for col in columns:
        table.add_column(_label(col), overflow="fold", max_width=55)

    for row in rows:
        values = [_truncate(row.get(col, "")) for col in columns]
        table.add_row(*values)

    console.print(table)


def render_update_start(topic: str, name: str, url: str) -> None:
    console.print(f"  [cyan]↓[/cyan] Загрузка [bold]{name}[/bold] ({topic})...")
    console.print(f"    URL: [dim]{url}[/dim]")


def render_update_done(topic: str, rows: int) -> None:
    console.print(f"  [green]✓[/green] Готово: {rows} строк загружено в кэш\n")


def render_update_error(topic: str, error: str) -> None:
    console.print(f"  [red]✗[/red] Ошибка для '{topic}': {error}\n")


_TOPIC_EXAMPLES: dict[str, list[str]] = {
    "parking":       [
        "сколько парковок в Центральном районе",
        "парковки по районам",
        "топ-5 парковок по числу мест",
    ],
    "stops":         [
        "остановки в Советском районе",
        "сколько автобусных остановок",
    ],
    "culture":       [
        "театры Новосибирска",
        "список организаций культуры",
    ],
    "schools":       [
        "сколько школ в Кировском районе",
        "школы по районам",
    ],
    "kindergartens": [
        "детские сады в Октябрьском районе",
        "сколько дошкольных организаций",
    ],
    "libraries":     [
        "библиотеки для детей",
        "покажи все библиотеки",
        "библиотеки в Ленинском районе",
    ],
    "parks":         [
        "парки культуры и отдыха",
        "все парки Новосибирска",
    ],
    "sport_grounds": [
        "спортплощадки в Калининском районе",
        "хоккейные коробки по районам",
        "детские спортплощадки",
    ],
    "pharmacies":    [
        "аптеки в Советском районе",
        "аптеки на улице Ленина",
    ],
    "sport_orgs":    [
        "детские спортивные организации",
        "спортивные организации для взрослых",
        "спортшколы по районам",
    ],
}


def render_help() -> None:
    """Расширенная справка по возможностям бота."""
    from .registry import load_registry

    console.rule("[bold blue]NSK OpenData Bot — возможности[/bold blue]")
    console.print()

    # === Открытые данные ===
    console.print("[bold]Открытые данные[/bold] [dim](opendata.novo-sibirsk.ru, TTL 24 ч)[/dim]\n")
    registry = load_registry()
    data_table = Table(box=box.SIMPLE_HEAD, show_lines=False, padding=(0, 1))
    data_table.add_column("Тема", style="bold cyan", no_wrap=True, min_width=20)
    data_table.add_column("Примеры запросов", style="white")
    for topic_id, ds in registry.items():
        examples = _TOPIC_EXAMPLES.get(topic_id, [])
        ex_text = "\n".join(f'"{e}"' for e in examples)
        data_table.add_row(ds.get("name", topic_id), ex_text)
    console.print(data_table)

    # === ЖКХ ===
    console.print("[bold]Отключения ЖКХ[/bold] [dim](051.novo-sibirsk.ru, TTL 30 мин, реальное время)[/dim]\n")
    power_table = Table(box=box.SIMPLE_HEAD, show_lines=False, padding=(0, 1))
    power_table.add_column("Пример запроса", style="white")
    power_table.add_column("Команда", style="dim", no_wrap=True)
    for q, cmd in [
        ("отключения электричества сегодня",      "bot ask"),
        ("есть ли свет в Дзержинском районе",     "bot ask"),
        ("плановые отключения на этой неделе",    "bot ask"),
        ("история отключений за неделю",          "bot ask"),
        ("текущий статус всех систем ЖКХ",        "bot power status --all-utilities"),
        ("плановые по Советскому району",         "bot power planned --district ..."),
    ]:
        power_table.add_row(f'"{q}"' if cmd == "bot ask" else q, cmd)
    console.print(power_table)

    # === Операции ===
    console.print("[bold]Типы операций[/bold]\n")
    ops_table = Table(box=box.SIMPLE_HEAD, show_lines=False, padding=(0, 1))
    ops_table.add_column("Операция", style="bold green", no_wrap=True, min_width=14)
    ops_table.add_column("Ключевые слова", min_width=30)
    ops_table.add_column("Пример", style="dim")
    for op, kw, ex in [
        ("Подсчёт",       "сколько, количество, число",          '"сколько школ в Ленинском районе"'),
        ("Группировка",   "по районам, по каждому",              '"парковки по районам"'),
        ("Топ-N",         "топ-5, первые N, наибольших",         '"топ-10 парковок по числу мест"'),
        ("Список/фильтр", "покажи, найди, список, все",          '"покажи библиотеки в Ленинском районе"'),
        ("Аудитория",     "для детей / детские / для взрослых",  '"детские спортивные организации"'),
    ]:
        ops_table.add_row(op, kw, ex)
    console.print(ops_table)

    # === Районы ===
    console.print(
        "[bold]Районы:[/bold] [dim]Дзержинский, Железнодорожный, Заельцовский, Калининский,"
        " Кировский, Ленинский, Октябрьский, Первомайский, Советский, Центральный[/dim]\n"
    )

    console.print(
        "[dim]Другие команды: [bold]bot topics[/bold] | [bold]bot update --all[/bold]"
        " | [bold]bot power update[/bold] | [bold]bot serve[/bold][/dim]"
    )
    console.rule()


def render_districts() -> None:
    """Показывает список районов Новосибирска с вариантами написания в запросах."""
    from .router import DISTRICTS, SUB_DISTRICTS_INFO

    console.rule("[bold blue]Районы Новосибирска[/bold blue]")
    console.print()

    table = Table(box=box.SIMPLE_HEAD, show_lines=False, padding=(0, 1))
    table.add_column("Район", style="bold cyan", no_wrap=True, min_width=26)
    table.add_column("Как писать в запросе", style="white")

    _examples = {
        "Дзержинский район":     '"в Дзержинском районе", "дзержинский"',
        "Железнодорожный район": '"в Железнодорожном районе", "железнодорожный"',
        "Заельцовский район":    '"в Заельцовском районе", "заельцов"',
        "Калининский район":     '"в Калининском районе", "калининский"',
        "Кировский район":       '"в Кировском районе", "кировский"',
        "Ленинский район":       '"в Ленинском районе", "ленинский"',
        "Октябрьский район":     '"в Октябрьском районе", "октябрьский"',
        "Первомайский район":    '"в Первомайском районе", "первомайский"',
        "Советский район":       '"в Советском районе", "советский", "Академгородок", "Шлюз", "мкр. Щ"',
        "Центральный район":     '"в Центральном районе", "центральный", "в центре"',
    }

    for district in DISTRICTS:
        table.add_row(district, _examples.get(district, ""))

    console.print(table)

    # Подрайоны
    console.print("\n[bold]Подрайоны и микрорайоны[/bold] [dim](автоматически маппятся на родительский район)[/dim]\n")
    sub_table = Table(box=box.SIMPLE_HEAD, show_lines=False, padding=(0, 1))
    sub_table.add_column("Подрайон", style="bold yellow", no_wrap=True, min_width=18)
    sub_table.add_column("Район", style="cyan", no_wrap=True, min_width=22)
    sub_table.add_column("Варианты написания", style="dim")
    for name, (parent, examples) in SUB_DISTRICTS_INFO.items():
        sub_table.add_row(name, parent, ", ".join(f'"{e}"' for e in examples))
    console.print(sub_table)

    console.print(
        '[dim]Пример: bot ask "аптеки в Академгородке" → фильтрует по Советскому району[/dim]'
    )
    console.rule()


def render_no_topic(query_text: str, alternatives: list) -> None:
    console.print(f"[yellow]Не удалось определить тему для запроса:[/yellow] «{query_text}»\n")
    console.print("Я умею отвечать на вопросы о:")
    registry = load_registry()
    for topic_id, ds in registry.items():
        kws = ds.get("keywords", [])[:3]
        console.print(f"  [cyan]•[/cyan] [bold]{ds.get('name')}[/bold] ({', '.join(kws)}...)")
    console.print(f"  [cyan]•[/cyan] [bold]Отключения электроснабжения[/bold] (свет, электричество, отключения...)")
    console.print(
        "\n[dim]Попробуйте: bot ask \"сколько парковок\" или bot topics для справки[/dim]"
    )


def render_power_result(
    query_text: str,
    plan: "Plan",
    exec_result: dict,
    meta: dict,
) -> None:
    """Отображает результат запроса об отключениях электроснабжения."""
    from .power_cache import get_power_meta

    console.rule("[bold red]Отключения электроснабжения[/bold red]")

    # Шапка
    last_scraped = meta.get("last_scraped", "")
    if last_scraped:
        try:
            dt = datetime.fromisoformat(last_scraped)
            last_str = dt.strftime("%d.%m.%Y %H:%M UTC")
        except Exception:
            last_str = last_scraped[:16]
    else:
        last_str = "нет данных"

    op_name = OP_NAMES.get(plan.operation, plan.operation)
    console.print(f"[bold]Запрос:[/bold]   {query_text}")
    console.print(f"[bold]Операция:[/bold] {op_name}")
    if plan.district:
        if plan.sub_district:
            console.print(f"[bold]Подрайон:[/bold] {plan.sub_district} (→ {plan.district})")
        else:
            console.print(f"[bold]Район:[/bold]    {plan.district}")
    console.print(f"[bold]Данные:[/bold]   обновлены {last_str} | всего записей {meta.get('total_records', '?')}")
    console.print()

    if "error" in exec_result:
        console.print(f"[red]Ошибка: {exec_result['error']}[/red]")
        console.rule()
        return

    rows = exec_result.get("rows", [])
    cols = exec_result.get("columns", [])
    op = exec_result.get("operation", "")

    if not rows:
        console.print(Panel(
            "[green]✓ Отключений не зафиксировано[/green]",
            border_style="green",
            title="Статус",
        ))
        console.rule()
        return

    # Сводка активных / плановых из meta
    active_h = meta.get("active_houses", 0)
    planned_h = meta.get("planned_houses", 0)
    if active_h > 0 or planned_h > 0:
        summary = []
        if active_h:
            summary.append(f"[red]Отключено: {active_h} домов[/red]")
        if planned_h:
            summary.append(f"[yellow]Запланировано: {planned_h} домов[/yellow]")
        console.print("  " + "  •  ".join(summary))
        console.print()

    if op == "POWER_HISTORY":
        _render_power_history(rows)
    else:
        _render_power_table(rows, cols, op)

    console.rule()


def _render_power_table(rows: list[dict], cols: list[str], op: str) -> None:
    """Таблица статуса/плановых отключений."""
    COL_POWER = {
        "utility":    "Система",
        "group_type": "Статус",
        "district":   "Район",
        "houses":     "Домов",
        "scraped_at": "Обновлено",
    }

    table = Table(box=box.SIMPLE_HEAD, show_lines=False)
    display_cols = [c for c in cols if c != "source_url"]
    for c in display_cols:
        table.add_column(COL_POWER.get(c, c), overflow="fold", max_width=40)

    for row in rows:
        values = []
        for c in display_cols:
            val = str(row.get(c, "") or "")
            if c == "group_type":
                val = _GROUP_LABELS_PLAIN.get(val, val)
            elif c == "scraped_at" and val:
                try:
                    dt = datetime.fromisoformat(val)
                    val = dt.strftime("%d.%m %H:%M")
                except Exception:
                    val = val[:16]
            elif c == "houses":
                val = val if val != "0" else "—"
            values.append(val)
        table.add_row(*values)

    console.print(table)


def _render_power_history(rows: list[dict]) -> None:
    """Таблица исторической сводки по дням."""
    table = Table(
        title="История отключений по дням",
        box=box.ROUNDED,
        show_lines=True,
    )
    table.add_column("Дата", style="bold cyan", no_wrap=True)
    table.add_column("Тип", justify="center")
    table.add_column("Домов (макс.)", justify="right", style="white")
    table.add_column("Снимков", justify="right", style="dim")

    for row in rows:
        day = str(row.get("day", ""))
        try:
            day = datetime.strptime(day, "%Y-%m-%d").strftime("%d.%m.%Y")
        except Exception:
            pass
        group = row.get("group_type", "")
        group_label = _GROUP_LABELS.get(group, group)
        houses = str(row.get("total_houses", 0))
        snaps = str(row.get("snapshots", 0))
        table.add_row(day, group_label, houses, snaps)

    console.print(table)
