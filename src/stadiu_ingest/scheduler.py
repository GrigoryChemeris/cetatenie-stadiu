"""Цикл: run_stadiu_once + случайная пауза между прогонами."""

from __future__ import annotations

import logging
import random
import time

from stadiu_ingest.config import (
    COLD_START_MAX_STADIU_PDFS,
    MAX_NEW_STADIU_DOWNLOADS,
    MAX_STADIU_REFRESH_PER_RUN,
    POLL_INTERVAL_MAX_MINUTES,
    POLL_INTERVAL_MIN_MINUTES,
    STADIU_LIST_POLL_MINUTES,
    STADIU_REFRESH_AFTER_DAYS,
)
from stadiu_ingest.run_stadiu_once import main as run_stadiu_once_main

log = logging.getLogger("stadiu_ingest.scheduler")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    lo_m = min(POLL_INTERVAL_MIN_MINUTES, POLL_INTERVAL_MAX_MINUTES)
    hi_m = max(POLL_INTERVAL_MIN_MINUTES, POLL_INTERVAL_MAX_MINUTES)
    if STADIU_LIST_POLL_MINUTES > 0:
        log.info(
            "stadiu scheduler: пауза %s мин между прогонами (STADIU_LIST_POLL_MINUTES; опрос списка PDF)",
            STADIU_LIST_POLL_MINUTES,
        )
    else:
        log.info("stadiu scheduler: пауза %s–%s мин между прогонами", lo_m, hi_m)
    log.info(
        "лимиты прогона: MAX_NEW_STADIU_DOWNLOADS=%s COLD_START_MAX_STADIU_PDFS=%s "
        "STADIU_REFRESH_AFTER_DAYS=%s MAX_STADIU_REFRESH_PER_RUN=%s",
        MAX_NEW_STADIU_DOWNLOADS,
        COLD_START_MAX_STADIU_PDFS,
        STADIU_REFRESH_AFTER_DAYS,
        MAX_STADIU_REFRESH_PER_RUN,
    )

    while True:
        code = run_stadiu_once_main()
        if code != 0:
            log.warning("run_stadiu_once завершился с кодом %s", code)
        if STADIU_LIST_POLL_MINUTES > 0:
            wait = STADIU_LIST_POLL_MINUTES * 60
        else:
            lo = lo_m * 60
            hi = hi_m * 60
            wait = random.randint(lo, hi)
        log.info("Пауза %s с (~%.1f мин)", wait, wait / 60)
        time.sleep(wait)
