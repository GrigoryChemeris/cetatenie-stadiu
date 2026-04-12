#!/usr/bin/env python3
"""
Локальный импорт PDF списков подачи Art. 11 (stadiu dosar) в БД.

  export DATABASE_URL=…   # тот же Postgres, что у cetatenie-mvp, или отдельный
  PYTHONPATH=src python ingest_stadiu_pdfs.py ~/Downloads/Art-11-*.pdf

Ключ записи: local:<sha256>. Повторный прогон пропускает; --force — обновить.
Таблицы: stadiu_list_documents, stadiu_list_lines (не пересекаются с pdf_documents).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from stadiu_ingest import db  # noqa: E402
from stadiu_ingest.config import STADIU_PARSE_PDF_SUBPROCESS  # noqa: E402
from stadiu_ingest.http_pdf import sha256_file  # noqa: E402
from stadiu_ingest.parser_art11 import (  # noqa: E402
    parse_art11_submission_pdf,
    parse_art11_submission_pdf_isolated,
)


def iter_pdf_paths(paths: list[Path], recursive: bool) -> list[Path]:
    out: list[Path] = []
    for raw in paths:
        p = raw.expanduser().resolve()
        if p.is_file():
            if p.suffix.lower() == ".pdf":
                out.append(p)
        elif p.is_dir():
            glob = p.rglob("*.pdf") if recursive else p.glob("*.pdf")
            out.extend(sorted(x for x in glob if x.is_file()))
        else:
            print(f"Пропуск (нет пути): {p}", file=sys.stderr)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Импорт PDF Art-11 stadiu → stadiu_list_documents / stadiu_list_lines"
    )
    ap.add_argument("paths", nargs="+", type=Path, help="Файлы .pdf и/или каталоги")
    ap.add_argument("-r", "--recursive", action="store_true")
    ap.add_argument(
        "--force",
        action="store_true",
        help="Перезаписать запись local:<sha256>, даже если уже есть",
    )
    args = ap.parse_args()

    # Пустой DATABASE_URL → SQLite (см. SQLITE_PATH в .env), иначе Postgres.

    db.init_db()
    known = db.known_stadiu_urls()
    pdfs = iter_pdf_paths(list(args.paths), args.recursive)
    if not pdfs:
        print("Нет PDF.", file=sys.stderr)
        return 1

    ok_n = err_n = skip_n = 0
    for i, path in enumerate(pdfs, start=1):
        digest = sha256_file(path)
        synthetic = f"local:{digest}"

        if not args.force and synthetic in known:
            print(f"[{i}/{len(pdfs)}] skip: {path.name}")
            skip_n += 1
            continue

        print(f"[{i}/{len(pdfs)}] {path.name} …", flush=True)
        try:
            if STADIU_PARSE_PDF_SUBPROCESS:
                meta, lines = parse_art11_submission_pdf_isolated(path)
            else:
                meta, lines = parse_art11_submission_pdf(path)
        except Exception as e:  # noqa: BLE001
            err_n += 1
            db.insert_stadiu_document_meta(
                synthetic,
                source_filename=path.name,
                list_year=None,
                snapshot_update_date=None,
                content_sha256=digest,
                parsed_ok=False,
                row_count=None,
                parse_error=repr(e),
            )
            print(f"    ERROR: {e}", file=sys.stderr)
            continue

        db.insert_stadiu_document_meta(
            synthetic,
            source_filename=meta.get("source_filename") or path.name,
            list_year=meta.get("list_year"),
            snapshot_update_date=meta.get("snapshot_update_date"),
            content_sha256=digest,
            parsed_ok=True,
            row_count=meta.get("row_count"),
            parse_error=None,
        )
        db.finalize_stadiu_lines_for_document(synthetic, lines)
        known.add(synthetic)
        ok_n += 1
        print(f"    OK строк={len(lines)}")

    print(
        f"Готово: ok={ok_n}, ошибок={err_n}, пропусков={skip_n}, всего файлов={len(pdfs)}",
        file=sys.stderr,
    )
    return 0 if err_n == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
