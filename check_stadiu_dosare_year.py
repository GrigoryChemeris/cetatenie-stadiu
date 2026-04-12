#!/usr/bin/env python3
"""
Dosar (Stadiu): сводка по stadiu_list_lines за год подачи.

Использует БД из .env репозитория cetatenie-stadiu: DATABASE_URL (Postgres)
или SQLITE_PATH (SQLite).

  PYTHONPATH=src python3 check_stadiu_dosare_year.py
  PYTHONPATH=src python3 check_stadiu_dosare_year.py --year 2018
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from stadiu_ingest import db  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Stadiu: проверка объёма строк по dossier_year (Dosar)"
    )
    ap.add_argument("--year", type=int, default=2017, help="Год подачи (dossier_year)")
    args = ap.parse_args()
    year = int(args.year)
    ys = str(year)
    hist: list[tuple[Any, int]] = []

    db.init_db()

    print(f"=== Dosar (Stadiu), dossier_year = {year} ===")

    with db.get_conn() as conn:
        if db._USE_PG:  # noqa: SLF001
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)::bigint FROM stadiu_list_lines
                    WHERE dossier_year = %s
                    """,
                    (year,),
                )
                n_year = int(cur.fetchone()[0])

                cur.execute(
                    """
                    SELECT COUNT(*)::bigint FROM stadiu_list_lines
                    WHERE dossier_year IS NULL
                    """
                )
                n_null_year = int(cur.fetchone()[0])

                cur.execute(
                    """
                    SELECT COUNT(*)::bigint FROM stadiu_list_lines
                    WHERE dossier_ref ILIKE ('%%/RD/' || %s::text)
                      AND (dossier_year IS DISTINCT FROM %s::int)
                    """,
                    (ys, year),
                )
                n_ref_rd_mismatch = int(cur.fetchone()[0])

                cur.execute("SELECT COUNT(*)::bigint FROM stadiu_list_lines")
                n_total = int(cur.fetchone()[0])

                cur.execute(
                    """
                    SELECT COUNT(*)::bigint FROM stadiu_list_documents
                    WHERE COALESCE(parsed_ok, false)
                    """
                )
                n_docs_ok = int(cur.fetchone()[0])

                cur.execute(
                    """
                    SELECT dossier_year::text, COUNT(*)::bigint
                    FROM stadiu_list_lines
                    GROUP BY dossier_year
                    ORDER BY dossier_year NULLS LAST
                    LIMIT 30
                    """
                )
                hist[:] = [(r[0], int(r[1])) for r in cur.fetchall()]
        else:
            cur = conn.execute(
                "SELECT COUNT(*) FROM stadiu_list_lines WHERE dossier_year = ?",
                (year,),
            )
            n_year = int(cur.fetchone()[0])

            cur = conn.execute(
                "SELECT COUNT(*) FROM stadiu_list_lines WHERE dossier_year IS NULL"
            )
            n_null_year = int(cur.fetchone()[0])

            cur = conn.execute(
                """
                SELECT COUNT(*) FROM stadiu_list_lines
                WHERE dossier_ref LIKE '%/RD/' || ?
                  AND (dossier_year IS NOT ? OR dossier_year IS NULL)
                """,
                (ys, year),
            )
            n_ref_rd_mismatch = int(cur.fetchone()[0])

            cur = conn.execute("SELECT COUNT(*) FROM stadiu_list_lines")
            n_total = int(cur.fetchone()[0])

            cur = conn.execute(
                """
                SELECT COUNT(*) FROM stadiu_list_documents
                WHERE COALESCE(parsed_ok, 0) != 0
                """
            )
            n_docs_ok = int(cur.fetchone()[0])

            cur = conn.execute(
                """
                SELECT dossier_year, COUNT(*)
                FROM stadiu_list_lines
                GROUP BY dossier_year
                ORDER BY dossier_year
                LIMIT 30
                """
            )
            hist[:] = [(r[0], int(r[1])) for r in cur.fetchall()]

    print(f"  stadiu_list_lines с dossier_year = {year}:     {n_year}")
    print(f"  stadiu_list_lines с dossier_year IS NULL:      {n_null_year}")
    print(f"  ref …/RD/{ys}, но год в колонке ≠ {year} или NULL: {n_ref_rd_mismatch}")
    print(f"  всего строк stadiu_list_lines:                 {n_total}")
    print(f"  документов с parsed_ok:                         {n_docs_ok}")

    print("\n  Распределение dossier_year (до 30 значений):")
    for yv, cnt in hist:
        print(f"    {yv!s:>8}  {cnt}")

    if n_null_year:
        print(
            f"\n(!) Есть строки без dossier_year — проверьте парсер/миграции.",
            file=sys.stderr,
        )
    if n_ref_rd_mismatch:
        print(
            f"\n(!) Несовпадение ref …/RD/{ys} и dossier_year — стоит выборочно посмотреть.",
            file=sys.stderr,
        )
    if n_year > 0 and n_null_year == 0 and n_ref_rd_mismatch == 0:
        print("\nСводка выглядит согласованно для этого года.")
    elif n_year == 0:
        print(
            f"\nСтрок с dossier_year={year} в этой БД нет — "
            "часто значит, что в выгрузке Stadiu другой диапазон лет (см. таблицу выше)."
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
