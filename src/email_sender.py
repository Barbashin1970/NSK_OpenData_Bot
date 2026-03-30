"""Отправка email из Пространства задач.

Два метода:
  1. Brevo API (HTTPS) — работает везде, включая Railway/Render/Heroku
  2. SMTP — для серверов без ограничений на исходящие SMTP-порты

Конфиг хранится в data/smtp_config.json.
"""

import base64
import json
import logging
import smtplib
import socket
import ssl
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from pathlib import Path

import requests as http_requests

log = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).parent.parent / "data" / "smtp_config.json"
_TIMEOUT = 12

_DEFAULTS = {
    "method": "brevo",          # "brevo" | "smtp"
    "brevo_api_key": "",
    "host": "smtp.gmail.com",
    "port": 465,
    "username": "",
    "password": "",
    "from_name": "Пространство задач — Сигма",
    "from_email": "",
}


# ── Config ────────────────────────────────────────────────────────────────

def get_smtp_config() -> dict:
    cfg = _load_config()
    safe = dict(cfg)
    if safe.get("password"):
        safe["password"] = "••••••••"
    if safe.get("brevo_api_key"):
        key = safe["brevo_api_key"]
        safe["brevo_api_key"] = key[:8] + "••••••••"
    safe["configured"] = bool(
        (cfg.get("method") == "brevo" and cfg.get("brevo_api_key") and cfg.get("from_email"))
        or (cfg.get("method") == "smtp" and cfg.get("username") and cfg.get("password"))
    )
    return safe


def save_smtp_config(data: dict) -> dict:
    cfg = _load_config()
    for key in ("host", "port", "username", "from_name", "from_email", "method"):
        if key in data:
            cfg[key] = data[key]
    if data.get("password") and "••" not in data["password"]:
        cfg["password"] = data["password"]
    if data.get("brevo_api_key") and "••" not in data["brevo_api_key"]:
        cfg["brevo_api_key"] = data["brevo_api_key"]
    cfg.pop("use_tls", None)
    _save_config(cfg)
    return get_smtp_config()


# ── Brevo API (HTTPS, порт 443) ──────────────────────────────────────────

def _send_via_brevo(cfg, to, subject, body, attachments=None, cc=""):
    api_key = cfg.get("brevo_api_key", "")
    from_email = cfg.get("from_email") or cfg.get("username", "")
    from_name = cfg.get("from_name", "Сигма")

    if not api_key:
        return {"sent": False, "error": "Brevo API-ключ не указан. Откройте настройки почты."}
    if not from_email:
        return {"sent": False, "error": "Email отправителя не указан."}

    payload = {
        "sender": {"name": from_name, "email": from_email},
        "to": [{"email": to.strip()}],
        "subject": subject,
        "textContent": body,
    }
    if cc:
        payload["cc"] = [{"email": c.strip()} for c in cc.split(",") if c.strip()]
    if attachments:
        payload["attachment"] = [
            {"name": fname, "content": base64.b64encode(fdata).decode()}
            for fname, fdata in attachments
        ]

    try:
        r = http_requests.post(
            "https://api.brevo.com/v3/smtp/email",
            headers={"api-key": api_key, "Content-Type": "application/json"},
            json=payload,
            timeout=15,
        )
        if r.status_code in (200, 201, 202):
            log.info("Email sent via Brevo to %s: %s", to, subject)
            return {"sent": True}
        # Парсим ошибку
        try:
            err = r.json().get("message", r.text[:200])
        except Exception:
            err = r.text[:200]
        return {"sent": False, "error": f"Brevo ({r.status_code}): {err}"}
    except http_requests.Timeout:
        return {"sent": False, "error": "Таймаут подключения к Brevo API."}
    except Exception as e:
        return {"sent": False, "error": f"Ошибка Brevo: {e}"}


def _test_brevo(cfg) -> dict:
    api_key = cfg.get("brevo_api_key", "")
    from_email = cfg.get("from_email") or cfg.get("username", "")
    if not api_key:
        return {"ok": False, "error": "Brevo API-ключ не указан."}
    if not from_email:
        return {"ok": False, "error": "Email отправителя не указан."}
    try:
        r = http_requests.get(
            "https://api.brevo.com/v3/account",
            headers={"api-key": api_key},
            timeout=10,
        )
        if r.status_code == 200:
            data = r.json()
            plan = data.get("plan", [{}])
            plan_name = plan[0].get("type", "?") if plan else "?"
            email = data.get("email", "?")
            return {"ok": True, "message": f"Brevo подключён. Аккаунт: {email}, план: {plan_name}. Отправитель: {from_email}"}
        elif r.status_code == 401:
            return {"ok": False, "error": "Неверный API-ключ Brevo."}
        else:
            return {"ok": False, "error": f"Brevo ({r.status_code}): {r.text[:150]}"}
    except Exception as e:
        return {"ok": False, "error": f"Ошибка: {e}"}


# ── SMTP (с принудительным IPv4) ──────────────────────────────────────────

def _connect_smtp_ipv4(host, port, username, password):
    ctx = ssl.create_default_context()
    port = int(port)
    ipv4_addrs = []
    try:
        for info in socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM):
            ipv4_addrs.append(info[4][0])
    except Exception:
        pass
    target = ipv4_addrs[0] if ipv4_addrs else host

    if port == 465:
        server = smtplib.SMTP_SSL(target, port, timeout=_TIMEOUT, context=ctx)
    elif port == 587:
        server = smtplib.SMTP(target, port, timeout=_TIMEOUT)
        server.ehlo(host)
        server.starttls(context=ctx)
        server.ehlo(host)
    else:
        server = smtplib.SMTP(target, port, timeout=_TIMEOUT)
        server.ehlo(host)
    server.login(username, password)
    return server


def _connect_with_fallback(cfg):
    host = cfg["host"]
    port = int(cfg.get("port", 465))
    user = cfg["username"]
    pwd = cfg["password"]
    try:
        return _connect_smtp_ipv4(host, port, user, pwd), port
    except (TimeoutError, OSError) as first_err:
        alt_port = 465 if port == 587 else 587
        try:
            return _connect_smtp_ipv4(host, alt_port, user, pwd), alt_port
        except Exception:
            raise first_err


def _build_mime(from_name, from_email, to, subject, body, attachments=None, cc=""):
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
    return msg


def _send_via_smtp(cfg, to, subject, body, attachments=None, cc=""):
    if not cfg.get("username") or not cfg.get("password"):
        return {"sent": False, "error": "SMTP логин/пароль не указаны."}
    from_email = cfg.get("from_email") or cfg["username"]
    from_name = cfg.get("from_name", "Сигма")
    msg = _build_mime(from_name, from_email, to, subject, body, attachments, cc)
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
        if used_port != int(cfg.get("port", 465)):
            cfg["port"] = used_port
            _save_config(cfg)
        log.info("Email sent via SMTP to %s:%d — %s", to, used_port, subject)
        return {"sent": True}
    except smtplib.SMTPAuthenticationError:
        return {"sent": False, "error": "Ошибка авторизации SMTP. Для Gmail нужен App Password."}
    except smtplib.SMTPException as e:
        return {"sent": False, "error": f"SMTP ошибка: {e}"}
    except TimeoutError:
        return {"sent": False, "error": "Таймаут SMTP (порты 465/587 заблокированы). Используйте метод Brevo."}
    except OSError as e:
        return {"sent": False, "error": f"Сетевая ошибка: {e}. Попробуйте метод Brevo."}
    except Exception as e:
        return {"sent": False, "error": f"Ошибка: {e}"}


def _test_smtp(cfg) -> dict:
    if not cfg.get("username") or not cfg.get("password"):
        return {"ok": False, "error": "Логин или пароль SMTP не заполнены."}
    try:
        server, used_port = _connect_with_fallback(cfg)
        try:
            server.noop()
        finally:
            try:
                server.quit()
            except Exception:
                pass
        if used_port != int(cfg.get("port", 465)):
            cfg["port"] = used_port
            _save_config(cfg)
        return {"ok": True, "message": f"SMTP {cfg['host']}:{used_port} — подключение и авторизация OK."}
    except smtplib.SMTPAuthenticationError as e:
        return {"ok": False, "error": f"Ошибка авторизации. Для Gmail нужен App Password."}
    except TimeoutError:
        return {"ok": False, "error": f"Таймаут SMTP. Порты 465/587 заблокированы в этой сети. Используйте Brevo."}
    except OSError as e:
        return {"ok": False, "error": f"Сетевая ошибка: {e}. Используйте метод Brevo."}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ── Public API ────────────────────────────────────────────────────────────

def test_smtp_connection() -> dict:
    cfg = _load_config()
    method = cfg.get("method", "brevo")
    if method == "brevo":
        return _test_brevo(cfg)
    return _test_smtp(cfg)


def send_email(to, subject, body, attachments=None, cc="") -> dict:
    cfg = _load_config()
    method = cfg.get("method", "brevo")
    if method == "brevo":
        return _send_via_brevo(cfg, to, subject, body, attachments, cc)
    return _send_via_smtp(cfg, to, subject, body, attachments, cc)


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
