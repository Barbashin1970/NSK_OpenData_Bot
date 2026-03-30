"""Отправка email из Пространства задач.

SMTP-конфиг хранится в data/smtp_config.json.
Поддерживает вложения (multipart/mixed).

Стратегия подключения к Gmail:
  1. Порт 465 — SMTP_SSL (прямое SSL-соединение, работает почти везде)
  2. Порт 587 — STARTTLS (иногда блокируется провайдерами)
  3. Автоопределение: пробуем 465, если таймаут — пробуем 587
"""

import json
import logging
import smtplib
import ssl
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from pathlib import Path

log = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).parent.parent / "data" / "smtp_config.json"
_TIMEOUT = 12

_DEFAULTS = {
    "host": "smtp.gmail.com",
    "port": 465,
    "username": "",
    "password": "",
    "from_name": "Пространство задач — Сигма",
    "from_email": "",
}


def get_smtp_config() -> dict:
    """Возвращает текущий SMTP-конфиг (без пароля в открытом виде)."""
    cfg = _load_config()
    safe = dict(cfg)
    if safe.get("password"):
        safe["password"] = "••••••••"
    safe["configured"] = bool(cfg.get("username") and cfg.get("password"))
    return safe


def save_smtp_config(data: dict) -> dict:
    """Сохраняет SMTP-конфиг."""
    cfg = _load_config()
    for key in ("host", "port", "username", "from_name", "from_email"):
        if key in data:
            cfg[key] = data[key]
    if data.get("password") and "••" not in data["password"]:
        cfg["password"] = data["password"]
    # Убираем устаревшее поле
    cfg.pop("use_tls", None)
    _save_config(cfg)
    return get_smtp_config()


def _connect_smtp(host: str, port: int, username: str, password: str):
    """Подключается к SMTP и авторизуется. Возвращает server объект.

    Порт 465 → SMTP_SSL (прямой SSL).
    Порт 587 → SMTP + STARTTLS.
    Другие   → SMTP без шифрования.
    """
    ctx = ssl.create_default_context()
    port = int(port)

    if port == 465:
        server = smtplib.SMTP_SSL(host, port, timeout=_TIMEOUT, context=ctx)
    elif port == 587:
        server = smtplib.SMTP(host, port, timeout=_TIMEOUT)
        server.ehlo()
        server.starttls(context=ctx)
        server.ehlo()
    else:
        server = smtplib.SMTP(host, port, timeout=_TIMEOUT)
        server.ehlo()

    server.login(username, password)
    return server


def _connect_with_fallback(cfg: dict):
    """Пробует подключиться. Если таймаут на заданном порту — пробует другой."""
    host = cfg["host"]
    port = int(cfg.get("port", 465))
    user = cfg["username"]
    pwd = cfg["password"]

    # Основная попытка
    try:
        return _connect_smtp(host, port, user, pwd), port
    except (TimeoutError, OSError) as first_err:
        # Fallback: если был 587, пробуем 465 и наоборот
        alt_port = 465 if port == 587 else 587
        log.info("SMTP: порт %d таймаут, пробую %d...", port, alt_port)
        try:
            return _connect_smtp(host, alt_port, user, pwd), alt_port
        except Exception:
            # Оба порта не работают — поднимаем оригинальную ошибку
            raise first_err


def test_smtp_connection() -> dict:
    """Проверяет подключение к SMTP без отправки письма."""
    cfg = _load_config()
    if not cfg.get("username") or not cfg.get("password"):
        return {"ok": False, "error": "Логин или пароль не заполнены."}

    try:
        server, used_port = _connect_with_fallback(cfg)
        try:
            server.noop()
        finally:
            try:
                server.quit()
            except Exception:
                pass

        # Если подключились через fallback порт — сохраняем его
        configured_port = int(cfg.get("port", 465))
        note = ""
        if used_port != configured_port:
            cfg["port"] = used_port
            _save_config(cfg)
            note = f" (порт изменён с {configured_port} на {used_port})"

        return {
            "ok": True,
            "message": f"Подключение к {cfg['host']}:{used_port} успешно. Авторизация пройдена.{note}",
        }
    except smtplib.SMTPAuthenticationError as e:
        code = getattr(e, "smtp_code", "")
        return {
            "ok": False,
            "error": f"Ошибка авторизации ({code}). Для Gmail нужен App Password — "
                     f"обычный пароль не подойдёт. Создайте на myaccount.google.com/apppasswords",
        }
    except TimeoutError:
        return {
            "ok": False,
            "error": f"Таймаут подключения к {cfg['host']} (порты 465 и 587). "
                     f"Возможно, SMTP заблокирован в вашей сети.",
        }
    except OSError as e:
        return {"ok": False, "error": f"Сетевая ошибка: {e}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def send_email(
    to: str,
    subject: str,
    body: str,
    attachments: list[tuple[str, bytes]] | None = None,
    cc: str = "",
) -> dict:
    """Отправляет email. attachments — список (filename, content_bytes)."""
    cfg = _load_config()
    if not cfg.get("username") or not cfg.get("password"):
        return {"sent": False, "error": "SMTP не настроен. Откройте настройки почты."}

    from_email = cfg.get("from_email") or cfg["username"]
    from_name = cfg.get("from_name", "Сигма")

    msg = MIMEMultipart()
    msg["From"] = f"{from_name} <{from_email}>"
    msg["To"] = to
    if cc:
        msg["Cc"] = cc
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    for fname, fdata in (attachments or []):
        part = MIMEBase("application", "octet-stream")
        part.set_payload(fdata)
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f'attachment; filename="{fname}"')
        msg.attach(part)

    recipients = [to]
    if cc:
        recipients.extend([a.strip() for a in cc.split(",") if a.strip()])

    try:
        server, used_port = _connect_with_fallback(cfg)
        try:
            server.sendmail(from_email, recipients, msg.as_string())
        finally:
            try:
                server.quit()
            except Exception:
                pass

        # Запоминаем рабочий порт
        if used_port != int(cfg.get("port", 465)):
            cfg["port"] = used_port
            _save_config(cfg)

        log.info("Email sent to %s via :%d — %s", to, used_port, subject)
        return {"sent": True}

    except smtplib.SMTPAuthenticationError:
        return {
            "sent": False,
            "error": "Ошибка авторизации. Для Gmail нужен App Password (myaccount.google.com/apppasswords).",
        }
    except smtplib.SMTPException as e:
        log.warning("SMTP error: %s", e)
        return {"sent": False, "error": f"SMTP ошибка: {e}"}
    except TimeoutError:
        return {
            "sent": False,
            "error": "Таймаут подключения (порты 465 и 587). SMTP может быть заблокирован в сети.",
        }
    except OSError as e:
        return {"sent": False, "error": f"Сетевая ошибка: {e}"}
    except Exception as e:
        log.warning("Email send error: %s", e)
        return {"sent": False, "error": f"Ошибка: {e}"}


def _load_config() -> dict:
    try:
        if _CONFIG_PATH.exists():
            return json.loads(_CONFIG_PATH.read_text("utf-8"))
    except Exception:
        pass
    return dict(_DEFAULTS)


def _save_config(cfg: dict) -> None:
    _CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    _CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), "utf-8")
