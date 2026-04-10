"""HTTP: список stadiu-dosar, скачивание PDF — без лишнего Chromium где возможно."""

from __future__ import annotations

import hashlib
import ssl
import urllib.error
import urllib.request
from pathlib import Path


def sha256_file(path: Path, *, chunk_size: int = 65536) -> str:
    """Хеш файла потоком — без чтения всего PDF в память."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def fetch_stadiu_list_html_via_http(
    url: str,
    user_agent: str,
    timeout: float,
) -> str | None:
    """
    Если ответ уже содержит вкладку Art. 11 и ссылки .pdf — можно обойтись без Selenium.
    """
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": user_agent,
                "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            },
            method="GET",
        )
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            raw = resp.read()
    except (urllib.error.URLError, urllib.error.HTTPError, OSError, ValueError):
        return None

    try:
        html = raw.decode("utf-8", errors="replace")
    except Exception:
        return None

    low = html.lower()
    if "articolul-11-tab" not in low or ".pdf" not in low:
        return None
    return html


def download_pdf_via_http(
    url: str,
    dest_dir: Path,
    *,
    user_agent: str,
    timeout: float,
) -> Path:
    """
    Потоковая загрузка в файл. Бросает urllib.error.HTTPError / URLError / ValueError.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    tag = hashlib.sha256(url.encode()).hexdigest()[:16]
    out = dest_dir / f"st_{tag}.pdf"
    out.unlink(missing_ok=True)

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept": "application/pdf,*/*;q=0.9",
        },
        method="GET",
    )
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            with open(out, "wb") as f:
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    f.write(chunk)
    except urllib.error.HTTPError as e:
        out.unlink(missing_ok=True)
        raise
    except urllib.error.URLError:
        out.unlink(missing_ok=True)
        raise

    if not out.exists() or out.stat().st_size == 0:
        out.unlink(missing_ok=True)
        raise ValueError("пустой ответ")

    with open(out, "rb") as f:
        head = f.read(5)
    if not head.startswith(b"%PDF"):
        snippet = head + f.read(40)
        out.unlink(missing_ok=True)
        raise ValueError(f"не PDF (начало: {snippet!r})")

    return out
