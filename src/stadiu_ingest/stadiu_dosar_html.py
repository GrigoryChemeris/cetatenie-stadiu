"""
Разбор сохранённой страницы https://cetatenie.just.ro/stadiu-dosar/

Вкладки — виджет Essential Addons «advance tabs» (Elementor). Каждый артикул —
отдельная панель в DOM, даже если визуально скрыта (class inactive).

Ссылки на PDF для **Art. 11** лежат только внутри элемента с id ``articolul-11-tab``.
Остальные артикулы: ``articolul-8-tab``, ``articolul-81-tab``, и т.д. — не трогаем.

При будущем Selenium: после загрузки страницы достаточно ``page_source`` и того же
селектора — клик по вкладке не обязателен, контент уже в HTML.
"""

from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

STADIU_DOSAR_PAGE_URL = "https://cetatenie.just.ro/stadiu-dosar/"
ART11_PANEL_ID = "articolul-11-tab"


def extract_art11_pdf_links_from_html(
    html: str,
    *,
    base_url: str = STADIU_DOSAR_PAGE_URL,
) -> list[dict[str, str]]:
    """
    Прямые ссылки на .pdf из вкладки ARTICOLUL 11.

    Возвращает список { "year": текст рядом со ссылкой (если есть), "url": абсолютный URL }.
    Год обычно в тексте <a>2010</a>; если ссылки нет (битый год), строка пропускается.
    """
    soup = BeautifulSoup(html, "html.parser")
    panel = soup.find(id=ART11_PANEL_ID)
    if not panel:
        return []

    seen: set[str] = set()
    out: list[dict[str, str]] = []
    for a in panel.find_all("a", href=True):
        href = str(a.get("href", "")).strip()
        if not href.lower().split("?", 1)[0].endswith(".pdf"):
            continue
        full = urljoin(base_url, href)
        if full in seen:
            continue
        seen.add(full)
        year = (a.get_text(strip=True) or "").strip()
        if not re.fullmatch(r"\d{4}", year):
            ym = re.search(r"(?:Art[._-]*11[_-]*|art[._-]*11[_-]*)(\d{4})", full, re.I)
            year = ym.group(1) if ym else ""
        out.append({"year": year, "url": full})

    return out
