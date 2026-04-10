"""
Один прогон: список stadiu-dosar и PDF Art. 11 по умолчанию через Selenium с ротацией User-Agent
(см. user_agents.py) — меньше риск блокировки, чем у «голого» HTTP с датацентра.
Опционально STADIU_PREFER_HTTP_LIST / STADIU_PREFER_HTTP_PDF=1 — только если осознанно нужно.

Порции за прогон ограничены (MAX_NEW_STADIU_DOWNLOADS, COLD_START_MAX_STADIU_PDFS, refresh).

На каждом прогоне заново загружается страница и парсятся href на PDF. Если министерство
выложило файл с другим именем (часто с суффиксом вроде update в имени), обычно меняется
и URL — такая ссылка не входит в known и обрабатывается как новая (см. приоритет по году ниже).

Дополнительно: один и тот же URL теоретически может отдавать другой PDF — периодически
перекачиваем и сравниваем sha256 (STADIU_REFRESH_AFTER_DAYS / MAX_STADIU_REFRESH_PER_RUN).
"""

from __future__ import annotations

import gc
import logging
import sys
import tempfile
import time
from pathlib import Path

from stadiu_ingest import db
from stadiu_ingest.config import (
    COLD_START_MAX_STADIU_PDFS,
    MAX_NEW_STADIU_DOWNLOADS,
    MAX_STADIU_REFRESH_PER_RUN,
    PAGE_LOAD_TIMEOUT,
    STADIU_BETWEEN_PDF_SEC,
    STADIU_HTTP_DOWNLOAD_ATTEMPTS,
    STADIU_HTTP_RETRY_BASE_SEC,
    STADIU_LIST_HTTP_ATTEMPTS,
    STADIU_LIST_SETTLE_SEC,
    STADIU_PAGE_URL,
    STADIU_PARSE_PDF_SUBPROCESS,
    STADIU_PREFER_HTTP_LIST,
    STADIU_PREFER_HTTP_PDF,
    STADIU_REFRESH_AFTER_DAYS,
    STADIU_SELENIUM_DOWNLOAD_ATTEMPTS,
)
from stadiu_ingest.http_pdf import (
    download_pdf_via_http_retry,
    fetch_stadiu_list_html_via_http,
    sha256_file,
)
from stadiu_ingest.parser_art11 import (
    meta_from_art11_pdf_url,
    parse_art11_submission_pdf,
    parse_art11_submission_pdf_isolated,
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

    driver = None
    try:
        html: str | None = None
        if STADIU_PREFER_HTTP_LIST:
            html = fetch_stadiu_list_html_via_http(
                STADIU_PAGE_URL,
                list_ua,
                float(PAGE_LOAD_TIMEOUT),
                attempts=STADIU_LIST_HTTP_ATTEMPTS,
                base_delay_sec=STADIU_HTTP_RETRY_BASE_SEC,
            )
        if html is None:
            log.info("stadiu-dosar через Selenium | UA: %s...", list_ua[:70])
            driver = build_chrome(download_root, list_ua)
            html = fetch_html(
                driver,
                STADIU_PAGE_URL,
                settle_seconds=STADIU_LIST_SETTLE_SEC,
                wait_for_content=True,
                stadiu_dosar_page=True,
            )
        else:
            log.info(
                "stadiu-dosar по HTTP (список без Chromium) | UA: %s...",
                list_ua[:70],
            )

        items = extract_art11_pdf_links_from_html(html, base_url=STADIU_PAGE_URL)
        log.info("Art. 11: найдено ссылок на PDF: %s", len(items))

        unseen = [it for it in items if it["url"] not in known]
        by_year = db.list_stadiu_document_urls_by_list_year()

        def _unseen_priority(it: dict[str, str]) -> tuple[int, str]:
            y = (it.get("year") or "").strip()
            u = it["url"]
            if y and y in by_year and u not in by_year[y]:
                return (0, u)
            return (1, u)

        unseen.sort(key=_unseen_priority)

        refresh_items: list[dict[str, str]] = []
        if not cold_start and STADIU_REFRESH_AFTER_DAYS > 0:
            for it in items:
                u = it["url"]
                if u not in known:
                    continue
                if db.stadiu_https_url_needs_refresh(u, STADIU_REFRESH_AFTER_DAYS):
                    refresh_items.append(it)

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
            batch_new = unseen[:MAX_NEW_STADIU_DOWNLOADS]
            nu = {x["url"] for x in batch_new}
            batch_refresh = [
                x for x in refresh_items if x["url"] not in nu
            ][:MAX_STADIU_REFRESH_PER_RUN]
            batch = batch_new + batch_refresh
            if batch_refresh:
                log.info(
                    "Перепроверка по времени (>%s дн.): %s ссылок",
                    STADIU_REFRESH_AFTER_DAYS,
                    len(batch_refresh),
                )

        if not batch:
            log.info("Нет новых PDF Art. 11 и нет ссылок на перепроверку.")
            return 0

        log.info(
            "В работе: %s",
            ", ".join((it.get("year") or "?") for it in batch[:18])
            + (" …" if len(batch) > 18 else ""),
        )

        if STADIU_PREFER_HTTP_PDF and driver is not None:
            try:
                driver.quit()
            except Exception:
                pass
            driver = None
            log.info(
                "Chromium закрыт после списка; PDF по умолчанию — HTTP (STADIU_PREFER_HTTP_PDF=1)"
            )

        for it in batch:
            url = it["url"]
            label = it.get("year") or url
            row_pk = db.resolve_stadiu_document_pk(url)
            storage_url = row_pk or url

            pdf_ua = random_user_agent()
            saved: Path | None = None
            if STADIU_PREFER_HTTP_PDF:
                try:
                    saved = download_pdf_via_http_retry(
                        url,
                        download_root,
                        user_agent=pdf_ua,
                        timeout=float(PAGE_LOAD_TIMEOUT),
                        referer=STADIU_PAGE_URL,
                        attempts=STADIU_HTTP_DOWNLOAD_ATTEMPTS,
                        base_delay_sec=STADIU_HTTP_RETRY_BASE_SEC,
                    )
                    log.info("PDF %s — по HTTP, UA: %s...", label, pdf_ua[:50])
                except Exception as e:  # noqa: BLE001
                    log.info("PDF %s — HTTP не удалось после повторов (%s), пробуем Selenium", label, e)

            if saved is None:
                for sel_try in range(max(1, STADIU_SELENIUM_DOWNLOAD_ATTEMPTS)):
                    if driver is None:
                        log.info("Запуск Chromium для скачивания PDF…")
                        driver = build_chrome(download_root, random_user_agent())
                    set_random_user_agent(driver, pdf_ua)
                    log.info(
                        "PDF %s — Selenium (%s/%s), UA: %s...",
                        label,
                        sel_try + 1,
                        STADIU_SELENIUM_DOWNLOAD_ATTEMPTS,
                        pdf_ua[:50],
                    )
                    try:
                        saved = download_pdf_to_dir(
                            driver,
                            download_root,
                            url,
                            timeout=float(PAGE_LOAD_TIMEOUT),
                        )
                        break
                    except Exception as e:  # noqa: BLE001
                        log.warning(
                            "PDF %s — Selenium попытка %s: %s",
                            label,
                            sel_try + 1,
                            e,
                        )
                        try:
                            driver.quit()
                        except Exception:
                            pass
                        driver = None
                        if sel_try + 1 < STADIU_SELENIUM_DOWNLOAD_ATTEMPTS:
                            time.sleep(
                                min(
                                    STADIU_HTTP_RETRY_BASE_SEC * (2**sel_try),
                                    45.0,
                                )
                            )
                if saved is None:
                    log.error("Скачивание не удалось (HTTP+Selenium): %s", label)
                    continue

            digest = sha256_file(saved)

            if row_pk:
                old_sha, _ = db.get_stadiu_sha_and_downloaded_at(row_pk)
                if old_sha == digest:
                    db.touch_stadiu_downloaded_at(row_pk)
                    log.info(
                        "Перепроверка: содержимое то же (sha256), обновлена дата проверки — %s",
                        label,
                    )
                    try:
                        saved.unlink(missing_ok=True)
                    except OSError:
                        pass
                    time.sleep(STADIU_BETWEEN_PDF_SEC)
                    continue

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
                time.sleep(STADIU_BETWEEN_PDF_SEC)
                continue

            url_meta = meta_from_art11_pdf_url(url)
            parse_error = None
            parsed_ok = False
            row_count: int | None = None
            lines: list = []
            list_year = url_meta.get("list_year")
            snap_date = url_meta.get("snapshot_update_date")

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
                log.info(
                    "Парсинг OK: list_year=%s строк=%s",
                    list_year,
                    len(lines),
                )
            except Exception as ex:  # noqa: BLE001
                parse_error = repr(ex)
                log.exception("Ошибка парсинга %s", url)

            db.insert_stadiu_document_meta(
                storage_url,
                source_filename=saved.name,
                list_year=list_year,
                snapshot_update_date=snap_date,
                content_sha256=digest,
                parsed_ok=parsed_ok,
                row_count=row_count,
                parse_error=parse_error,
            )
            if parsed_ok:
                db.merge_stadiu_lines(storage_url, lines)

            try:
                saved.unlink(missing_ok=True)
            except OSError:
                pass

            known.add(url)
            gc.collect()
            time.sleep(STADIU_BETWEEN_PDF_SEC)

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
