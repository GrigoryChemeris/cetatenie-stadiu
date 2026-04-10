#!/usr/bin/env python3
"""
Долгоживущий процесс для отдельного сервиса Railway (отдельный блок).

Сейчас: только heartbeat в лог. Импорт PDF — локально: ingest_stadiu_pdfs.py.

Дальше сюда можно добавить Selenium-скачивание с
https://cetatenie.just.ro/stadiu-dosar/ (раздел Art. 11) по расписанию.
"""

from __future__ import annotations

import logging
import os
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("stadiu")

# Интервал «пульса» в лог (сек); реальные задачи добавятся позже
HEARTBEAT_SECONDS = int(os.getenv("STADIU_HEARTBEAT_SECONDS", str(6 * 3600)))


def main() -> None:
    log.info(
        "cetatenie-stadiu: сервис запущен. Импорт: railway run / локально "
        "ingest_stadiu_pdfs.py. Heartbeat каждые %s ч.",
        HEARTBEAT_SECONDS // 3600,
    )
    while True:
        time.sleep(HEARTBEAT_SECONDS)
        log.info("cetatenie-stadiu: heartbeat (ожидание задач скачивания stadiu-dosar)")


if __name__ == "__main__":
    main()
