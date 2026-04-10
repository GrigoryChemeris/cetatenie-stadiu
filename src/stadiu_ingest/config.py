import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
PG_CONNECT_TIMEOUT = int(os.getenv("PG_CONNECT_TIMEOUT", "120"))
SQLITE_PATH = Path(os.getenv("SQLITE_PATH", "./data/stadiu.db")).resolve()

STADIU_PAGE_URL = os.getenv(
    "STADIU_PAGE_URL", "https://cetatenie.just.ro/stadiu-dosar/"
)
MAX_NEW_STADIU_DOWNLOADS = int(os.getenv("MAX_NEW_STADIU_DOWNLOADS", "2"))
# Первый прогон при пустой БД (нет ни одной записи в stadiu_list_documents): 0 = все PDF Art.11 за раз
COLD_START_MAX_STADIU_PDFS = int(os.getenv("COLD_START_MAX_STADIU_PDFS", "0"))

PAGE_LOAD_TIMEOUT = int(os.getenv("PAGE_LOAD_TIMEOUT", "120"))
LIST_PAGE_WAIT_TIMEOUT = int(os.getenv("LIST_PAGE_WAIT_TIMEOUT", "90"))
HEADLESS = os.getenv("HEADLESS", "1").strip().lower() in ("1", "true", "yes")

CHROME_BIN = os.getenv("CHROME_BIN", "").strip()
CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH", "").strip()

POLL_INTERVAL_MIN_MINUTES = int(os.getenv("POLL_INTERVAL_MIN_MINUTES", "30"))
POLL_INTERVAL_MAX_MINUTES = int(os.getenv("POLL_INTERVAL_MAX_MINUTES", "45"))
