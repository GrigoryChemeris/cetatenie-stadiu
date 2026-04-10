"""Цикл: run_stadiu_once + случайная пауза между прогонами."""

from __future__ import annotations

import logging
import random
import time

from stadiu_ingest.config import POLL_INTERVAL_MAX_MINUTES, POLL_INTERVAL_MIN_MINUTES
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
    log.info("stadiu scheduler: пауза %s–%s мин между прогонами", lo_m, hi_m)

    while True:
        code = run_stadiu_once_main()
        if code != 0:
            log.warning("run_stadiu_once завершился с кодом %s", code)
        lo = lo_m * 60
        hi = hi_m * 60
        wait = random.randint(lo, hi)
        log.info("Пауза %s с (~%.1f мин)", wait, wait / 60)
        time.sleep(wait)
