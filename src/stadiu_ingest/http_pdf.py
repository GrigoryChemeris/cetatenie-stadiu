"""HTTP: список stadiu-dosar, скачивание PDF — без лишнего Chromium где возможно."""

from __future__ import annotations

import hashlib
import random
import ssl
import time
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


def _http_get_bytes(
    url: str,
    *,
    user_agent: str,
    timeout: float,
    extra_headers: dict[str, str] | None = None,
) -> bytes | None:
    h = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ro-RO,ro;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    if extra_headers:
        h.update(extra_headers)
    try:
        req = urllib.request.Request(url, headers=h, method="GET")
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            return resp.read()
    except (urllib.error.URLError, urllib.error.HTTPError, OSError, ValueError):
        return None


def fetch_stadiu_list_html_via_http(
    url: str,
    user_agent: str,
    timeout: float,
    *,
    attempts: int = 1,
    base_delay_sec: float = 2.0,
) -> str | None:
    """
    Если ответ уже содержит вкладку Art. 11 и ссылки .pdf — можно обойтись без Selenium.
    """
    last_raw: bytes | None = None
    for i in range(max(1, attempts)):
        raw = _http_get_bytes(url, user_agent=user_agent, timeout=timeout)
        if raw is not None:
            last_raw = raw
            try:
                html = raw.decode("utf-8", errors="replace")
            except Exception:
                html = ""
            low = html.lower()
            if "articolul-11-tab" in low and ".pdf" in low:
                return html
        if i + 1 < attempts:
            d = base_delay_sec * (2**i) + random.uniform(0, 0.75)
            time.sleep(min(d, 30.0))

    if last_raw is None:
        return None
    try:
        html = last_raw.decode("utf-8", errors="replace")
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
    referer: str | None = None,
) -> Path:
    """
    Потоковая загрузка в файл. Бросает urllib.error.HTTPError / URLError / ValueError.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    tag = hashlib.sha256(url.encode()).hexdigest()[:16]
    out = dest_dir / f"st_{tag}.pdf"
    out.unlink(missing_ok=True)

    headers: dict[str, str] = {
        "User-Agent": user_agent,
        "Accept": "application/pdf,*/*;q=0.9",
        "Accept-Language": "ro-RO,ro;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    if referer:
        headers["Referer"] = referer

    req = urllib.request.Request(url, headers=headers, method="GET")
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            with open(out, "wb") as f:
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    f.write(chunk)
    except urllib.error.HTTPError:
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


def download_pdf_via_http_retry(
    url: str,
    dest_dir: Path,
    *,
    user_agent: str,
    timeout: float,
    referer: str | None,
    attempts: int,
    base_delay_sec: float,
) -> Path:
    """Повторы с экспонендой (Connection refused, таймауты, 5xx)."""
    last: BaseException | None = None
    n = max(1, attempts)
    for i in range(n):
        try:
            return download_pdf_via_http(
                url,
                dest_dir,
                user_agent=user_agent,
                timeout=timeout,
                referer=referer,
            )
        except BaseException as e:
            last = e
            if i + 1 >= n:
                break
            d = base_delay_sec * (2**i) + random.uniform(0, 1.25)
            time.sleep(min(d, 60.0))
    assert last is not None
    raise last
