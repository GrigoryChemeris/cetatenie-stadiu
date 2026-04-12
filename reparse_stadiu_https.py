#!/usr/bin/env python3
"""
Принудительно перескачать (или взять локальный PDF) и снова распарсить в тот же doc_url.

run_stadiu_once при совпадении sha256 с уже сохранённым файлом не запускает парсер.
После обновления parser_art11.py это нужно для Redobandire и любых URL с тем же содержимым.

Примеры:

  PYTHONPATH=src python reparse_stadiu_https.py \\
    --filename Art._11_2010_Redobandire.pdf \\
    --filename Art._11_2012_Redobandire.pdf \\
    --filename Art._11_2013_Redobandire.pdf \\
    --local-dir ~/Downloads

  PYTHONPATH=src python reparse_stadiu_https.py 'https://cetatenie.just.ro/wp-content/uploads/.../file.pdf'
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import unquote, urlparse

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from stadiu_ingest import db  # noqa: E402
from stadiu_ingest.config import (  # noqa: E402
    PAGE_LOAD_TIMEOUT,
    STADIU_BETWEEN_PDF_SEC,
    STADIU_HTTP_DOWNLOAD_ATTEMPTS,
    STADIU_HTTP_RETRY_BASE_SEC,
    STADIU_PAGE_URL,
    STADIU_PARSE_PDF_SUBPROCESS,
    STADIU_PREFER_HTTP_PDF,
    STADIU_SELENIUM_DOWNLOAD_ATTEMPTS,
)
from stadiu_ingest.http_pdf import download_pdf_via_http_retry, sha256_file  # noqa: E402
from stadiu_ingest.parser_art11 import (  # noqa: E402
    meta_from_art11_pdf_url,
    parse_art11_submission_pdf,
    parse_art11_submission_pdf_isolated,
)
from stadiu_ingest.user_agents import random_user_agent  # noqa: E402


def _local_candidate(local_dir: Path, url: str, storage_url: str) -> Path | None:
    names: list[str] = []
    sf = db.get_stadiu_source_filename_for_url(storage_url)
    if sf:
        names.append(sf)
    tail = Path(unquote(urlparse(url).path)).name
    if tail and tail not in names:
        names.append(tail)
    for name in names:
        p = (local_dir / name).resolve()
        if p.is_file():
            return p
    return None


def _collect_urls(urls: list[str], filenames: list[str]) -> list[str]:
    out: list[str] = []
    for fn in filenames:
        found = db.find_stadiu_urls_by_source_filename(fn)
        if not found:
            print(
                f"Ошибка: в БД нет stadiu_list_documents с source_filename={fn!r}",
                file=sys.stderr,
            )
            raise SystemExit(2)
        out.extend(found)
    out.extend(urls)
    seen: set[str] = set()
    deduped: list[str] = []
    for u in out:
        u = u.strip()
        if not u or u in seen:
            continue
        seen.add(u)
        deduped.append(u)
    return deduped


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Форс: скачать PDF по https (или из --local-dir) и merge_stadiu_lines с новым парсером"
    )
    ap.add_argument(
        "urls",
        nargs="*",
        help="Полные https URL PDF (как в stadiu_list_documents.url)",
    )
    ap.add_argument(
        "--filename",
        action="append",
        default=[],
        help="Имя файла как в БД (source_filename), можно несколько раз",
    )
    ap.add_argument(
        "--local-dir",
        type=Path,
        help="Каталог: если здесь есть PDF с тем же именем, что в БД или в URL — не качаем",
    )
    args = ap.parse_args()

    db.init_db()
    targets = _collect_urls(list(args.urls), list(args.filename))
    if not targets:
        ap.print_help()
        print("\nНужен хотя бы один URL или --filename …", file=sys.stderr)
        return 2

    local_dir = args.local_dir.expanduser().resolve() if args.local_dir else None

    tmp_root = Path(tempfile.mkdtemp(prefix="stadiu_reparse_"))
    driver = None
    exit_code = 0
    try:
        for i, url in enumerate(targets, start=1):
            storage_url = db.resolve_stadiu_document_pk(url) or url
            short = url if len(url) <= 88 else url[:85] + "…"
            label = f"[{i}/{len(targets)}] {short}"

            saved: Path | None = None
            if local_dir is not None:
                cand = _local_candidate(local_dir, url, storage_url)
                if cand is not None:
                    saved = tmp_root / f"local_{i}.pdf"
                    shutil.copy2(cand, saved)
                    print(f"{label} — локальный файл {cand.name}", flush=True)

            pdf_ua = random_user_agent()
            if saved is None and STADIU_PREFER_HTTP_PDF:
                try:
                    saved = download_pdf_via_http_retry(
                        url,
                        tmp_root,
                        user_agent=pdf_ua,
                        timeout=float(PAGE_LOAD_TIMEOUT),
                        referer=STADIU_PAGE_URL,
                        attempts=STADIU_HTTP_DOWNLOAD_ATTEMPTS,
                        base_delay_sec=STADIU_HTTP_RETRY_BASE_SEC,
                    )
                    print(f"{label} — HTTP OK", flush=True)
                except Exception as e:  # noqa: BLE001
                    print(f"{label} — HTTP: {e}", file=sys.stderr)
                    saved = None

            if saved is None:
                from stadiu_ingest.selenium_client import (  # noqa: PLC0415
                    build_chrome,
                    download_pdf_to_dir,
                    set_random_user_agent,
                )

                for sel_try in range(max(1, STADIU_SELENIUM_DOWNLOAD_ATTEMPTS)):
                    if driver is None:
                        print("Запуск Chromium для скачивания PDF…", flush=True)
                        driver = build_chrome(tmp_root, random_user_agent())
                    set_random_user_agent(driver, pdf_ua)
                    try:
                        saved = download_pdf_to_dir(
                            driver,
                            tmp_root,
                            url,
                            timeout=float(PAGE_LOAD_TIMEOUT),
                        )
                        print(f"{label} — Selenium OK", flush=True)
                        break
                    except Exception as e:  # noqa: BLE001
                        print(
                            f"{label} — Selenium попытка {sel_try + 1}: {e}",
                            file=sys.stderr,
                        )
                        try:
                            driver.quit()
                        except Exception:
                            pass
                        driver = None

            if saved is None:
                print(f"{label} — скачивание не удалось", file=sys.stderr)
                return 1

            digest = sha256_file(saved)
            logical_name = (
                db.get_stadiu_source_filename_for_url(storage_url)
                or Path(unquote(urlparse(url).path)).name
                or saved.name
            )
            url_meta = meta_from_art11_pdf_url(url)
            list_year = url_meta.get("list_year")
            snap_date = url_meta.get("snapshot_update_date")
            parse_error: str | None = None
            parsed_ok = False
            row_count: int | None = None
            lines: list = []

            try:
                if STADIU_PARSE_PDF_SUBPROCESS:
                    file_meta, lines = parse_art11_submission_pdf_isolated(saved)
                else:
                    file_meta, lines = parse_art11_submission_pdf(saved)
                parsed_ok = True
                row_count = file_meta.get("row_count")
                if file_meta.get("list_year"):
                    list_year = file_meta.get("list_year")
                if file_meta.get("snapshot_update_date"):
                    snap_date = file_meta.get("snapshot_update_date")
            except Exception as ex:  # noqa: BLE001
                parse_error = repr(ex)
                exit_code = 1
                print(f"Ошибка парсинга: {ex}", file=sys.stderr)

            db.insert_stadiu_document_meta(
                storage_url,
                source_filename=logical_name,
                list_year=list_year,
                snapshot_update_date=snap_date,
                content_sha256=digest,
                parsed_ok=parsed_ok,
                row_count=row_count,
                parse_error=parse_error,
            )
            if parsed_ok:
                db.finalize_stadiu_lines_for_document(storage_url, lines)
                su = (
                    storage_url
                    if len(storage_url) <= 72
                    else storage_url[:69] + "…"
                )
                print(
                    f"    → doc_url={su} строк={len(lines)} sha256={digest[:12]}…",
                    flush=True,
                )

            try:
                saved.unlink(missing_ok=True)
            except OSError:
                pass
            time.sleep(STADIU_BETWEEN_PDF_SEC)

        return exit_code
    finally:
        try:
            shutil.rmtree(tmp_root, ignore_errors=True)
        except OSError:
            pass
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
