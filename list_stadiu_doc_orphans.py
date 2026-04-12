#!/usr/bin/env python3
"""
Документы stadiu_list_documents без ни одной строки в stadiu_list_lines.

По умолчанию — «подозрительные»: parsed_ok или row_count > 0, а строк 0
(как с Art-11 URL и завышенным row_count).

  PYTHONPATH=src python3 list_stadiu_doc_orphans.py
  PYTHONPATH=src python3 list_stadiu_doc_orphans.py --limit 50
  PYTHONPATH=src python3 list_stadiu_doc_orphans.py --all   # любой документ с 0 строк
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from stadiu_ingest import db  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Список документов stadiu без строк в stadiu_list_lines"
    )
    ap.add_argument("--limit", type=int, default=500, help="Максимум строк вывода")
    ap.add_argument(
        "--all",
        action="store_true",
        help="Все документы с 0 строк (в т.ч. без успешного парса)",
    )
    args = ap.parse_args()
    db.init_db()
    rows = db.list_stadiu_documents_suspicious_zero_lines(
        limit=max(1, min(args.limit, 50_000)),
        include_all_zero_line_docs=args.all,
    )
    print(f"Найдено записей: {len(rows)}")
    for r in rows:
        print(r)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
