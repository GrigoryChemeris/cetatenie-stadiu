"""
Один прогон: Selenium → stadiu-dosar → только Art. 11 PDF → скачивание → парсинг → БД.
Логика как cetatenie_mvp.run_once; User-Agent из stadiu_ingest.user_agents.
"""

from __future__ import annotations

import hashlib
import logging
import sys
import tempfile
import time
from pathlib import Path

from stadiu_ingest import db
from stadiu_ingest.config import (
    COLD_START_MAX_STADIU_PDFS,
    MAX_NEW_STADIU_DOWNLOADS,
    PAGE_LOAD_TIMEOUT,
    STADIU_PAGE_URL,
)
from stadiu_ingest.parser_art11 import (
    meta_from_art11_pdf_url,
    parse_art11_submission_pdf,
)
from stadiu_ingest.selenium_client import (
    build_chrome,
    download_pdf_to_dir,
    fetch_html,
    set_random_user_agent,
)
from stadiu_ingest.stadiu_dosar_html import extract_art11_pdf_links_from_html
from stadiu_ingest.user_agents import random_user_agent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("stadiu_ingest")


def main() -> int:
    db.init_db()
    known = db.known_stadiu_urls()
    cold_start = len(known) == 0

    list_ua = random_user_agent()
    download_root = Path(tempfile.mkdtemp(prefix="stadiu_dl_"))
    log.info("Selenium stadiu-dosar | UA: %s...", list_ua[:70])

    driver = None
    try:
        driver = build_chrome(download_root, list_ua)
        html = fetch_html(
            driver,
            STADIU_PAGE_URL,
            settle_seconds=3.0,
            wait_for_content=True,
            stadiu_dosar_page=True,
        )

        items = extract_art11_pdf_links_from_html(html, base_url=STADIU_PAGE_URL)
        log.info("Art. 11: найдено ссылок на PDF: %s", len(items))

        unseen = [it for it in items if it["url"] not in known]
        if cold_start:
            if COLD_START_MAX_STADIU_PDFS > 0:
                batch = unseen[:COLD_START_MAX_STADIU_PDFS]
            else:
                batch = list(unseen)
            log.info(
                "Холодный старт: в очереди %s из %s новых PDF",
                len(batch),
                len(unseen),
            )
        else:
            batch = unseen[:MAX_NEW_STADIU_DOWNLOADS]

        if not batch:
            log.info("Новых PDF Art. 11 нет.")
            return 0

        log.info(
            "К скачиванию: %s",
            ", ".join((it.get("year") or "?") for it in batch[:15])
            + (" …" if len(batch) > 15 else ""),
        )

        for it in batch:
            url = it["url"]
            label = it.get("year") or url
            pdf_ua = random_user_agent()
            set_random_user_agent(driver, pdf_ua)
            log.info("PDF %s — UA: %s...", label, pdf_ua[:50])

            try:
                saved = download_pdf_to_dir(
                    driver,
                    download_root,
                    url,
                    timeout=float(PAGE_LOAD_TIMEOUT),
                )
            except Exception as e:  # noqa: BLE001
                log.error("Скачивание не удалось: %s", e)
                continue

            digest = hashlib.sha256(saved.read_bytes()).hexdigest()

            canonical = db.find_stadiu_canonical_url_by_sha256(digest)
            if canonical is not None and canonical != url:
                log.info(
                    "Тот же PDF уже в БД (canonical=%s), алиас для URL списка",
                    canonical,
                )
                db.register_stadiu_pdf_url_alias(url, digest, canonical)
                try:
                    saved.unlink(missing_ok=True)
                except OSError:
                    pass
                time.sleep(1.5)
                continue

            url_meta = meta_from_art11_pdf_url(url)
            parse_error = None
            parsed_ok = False
            row_count: int | None = None
            lines: list = []
            list_year = url_meta.get("list_year")
            snap_date = url_meta.get("snapshot_update_date")

            try:
                file_meta, lines = parse_art11_submission_pdf(saved)
                parsed_ok = True
                row_count = file_meta.get("row_count")
                if file_meta.get("list_year"):
                    list_year = file_meta.get("list_year")
                if file_meta.get("snapshot_update_date"):
                    snap_date = file_meta.get("snapshot_update_date")
                log.info(
                    "Парсинг OK: list_year=%s строк=%s",
                    list_year,
                    len(lines),
                )
            except Exception as ex:  # noqa: BLE001
                parse_error = repr(ex)
                log.exception("Ошибка парсинга %s", url)

            db.insert_stadiu_document_meta(
                url,
                source_filename=saved.name,
                list_year=list_year,
                snapshot_update_date=snap_date,
                content_sha256=digest,
                parsed_ok=parsed_ok,
                row_count=row_count,
                parse_error=parse_error,
            )
            if parsed_ok:
                db.replace_stadiu_lines(url, lines)

            try:
                saved.unlink(missing_ok=True)
            except OSError:
                pass

            known.add(url)
            time.sleep(1.5)

        log.info("Готово.")
        return 0

    except Exception as ex:  # noqa: BLE001
        log.exception("Сбой run_stadiu_once: %s", ex)
        return 1
    finally:
        if driver is not None:
            driver.quit()


if __name__ == "__main__":
    sys.exit(main())
