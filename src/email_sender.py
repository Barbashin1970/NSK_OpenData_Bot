"""Отправка email из Пространства задач.

SMTP-конфиг хранится в data/smtp_config.json.
Поддерживает вложения (multipart/mixed).
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
from typing import Any

log = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).parent.parent / "data" / "smtp_config.json"

# Значения по умолчанию
_DEFAULTS = {
    "host": "smtp.gmail.com",
    "port": 587,
    "use_tls": True,
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
    """Сохраняет SMTP-конфиг. Пароль обновляется только если передан непустой."""
    cfg = _load_config()
    for key in ("host", "port", "use_tls", "username", "from_name", "from_email"):
        if key in data:
            cfg[key] = data[key]
    # Пароль обновляем только если передано реальное значение (не маска)
    if data.get("password") and "••" not in data["password"]:
        cfg["password"] = data["password"]
    _save_config(cfg)
    return get_smtp_config()


def send_email(
    to: str,
    subject: str,
    body: str,
    attachments: list[tuple[str, bytes]] | None = None,
    cc: str = "",
) -> dict:
    """Отправляет email. attachments — список (filename, content_bytes).

    Возвращает {"sent": True} или {"sent": False, "error": "..."}.
    """
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

    _TIMEOUT = 15  # секунд на подключение

    try:
        if cfg.get("use_tls", True):
            ctx = ssl.create_default_context()
            server = smtplib.SMTP(cfg["host"], int(cfg.get("port", 587)), timeout=_TIMEOUT)
            try:
                server.ehlo()
                server.starttls(context=ctx)
                server.ehlo()
                server.login(cfg["username"], cfg["password"])
                server.sendmail(from_email, recipients, msg.as_string())
            finally:
                server.quit()
        else:
            server = smtplib.SMTP(cfg["host"], int(cfg.get("port", 25)), timeout=_TIMEOUT)
            try:
                server.ehlo()
                server.login(cfg["username"], cfg["password"])
                server.sendmail(from_email, recipients, msg.as_string())
            finally:
                server.quit()
        log.info("Email sent to %s: %s", to, subject)
        return {"sent": True}
    except smtplib.SMTPAuthenticationError as e:
        log.warning("SMTP auth failed: %s", e)
        return {"sent": False, "error": "Ошибка авторизации SMTP. Проверьте логин/пароль (для Gmail нужен App Password)."}
    except smtplib.SMTPException as e:
        log.warning("SMTP error: %s", e)
        return {"sent": False, "error": f"SMTP ошибка: {e}"}
    except TimeoutError:
        return {"sent": False, "error": "Таймаут подключения к SMTP-серверу (15 сек). Проверьте host/port."}
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
