#!/usr/bin/env python3
"""
Вывести ссылки на PDF списков подачи только для ARTICOLUL 11 из сохранённого HTML.

  PYTHONPATH=src python list_art11_pdfs_from_html.py ~/Downloads/stadiu.html

Полезно до появления Selenium в этом сервисе: «Сохранить как…» страницу stadiu-dosar.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from stadiu_ingest.stadiu_dosar_html import (  # noqa: E402
    ART11_PANEL_ID,
    STADIU_DOSAR_PAGE_URL,
    extract_art11_pdf_links_from_html,
)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Извлечь ссылки PDF только для вкладки ARTICOLUL 11 (stadiu-dosar)"
    )
    ap.add_argument("html_file", type=Path, help="Сохранённая страница .html")
    ap.add_argument(
        "--base-url",
        default=STADIU_DOSAR_PAGE_URL,
        help="База для относительных href",
    )
    ap.add_argument("--json", action="store_true", help="JSON в stdout")
    args = ap.parse_args()

    path = args.html_file.expanduser().resolve()
    if not path.is_file():
        print(f"Нет файла: {path}", file=sys.stderr)
        return 1

    html = path.read_text(encoding="utf-8", errors="replace")
    items = extract_art11_pdf_links_from_html(html, base_url=str(args.base_url))

    if args.json:
        print(json.dumps(items, ensure_ascii=False, indent=2))
    else:
        for it in items:
            y = it.get("year") or "?"
            print(f"{y}\t{it['url']}")
    print(f"# всего ссылок Art.11: {len(items)}", file=sys.stderr)
    if not items:
        print(
            f"# Не найден id=#{ART11_PANEL_ID!r} — возможно, другая вёрстка или неполное сохранение.",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
