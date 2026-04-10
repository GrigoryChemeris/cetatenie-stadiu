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
MAX_NEW_STADIU_DOWNLOADS = int(os.getenv("MAX_NEW_STADIU_DOWNLOADS", "1"))
# Первый прогон при пустой БД: 0 = все PDF за один прогон (не рекомендуется). По умолчанию — маленькие порции.
COLD_START_MAX_STADIU_PDFS = int(os.getenv("COLD_START_MAX_STADIU_PDFS", "3"))

# Тот же URL на сайте, но файл на сервере мог обновиться — перекачка и сравнение sha256.
# 0 = не планировать принудительную проверку по времени (только новые URL).
STADIU_REFRESH_AFTER_DAYS = int(os.getenv("STADIU_REFRESH_AFTER_DAYS", "7"))
MAX_STADIU_REFRESH_PER_RUN = int(os.getenv("MAX_STADIU_REFRESH_PER_RUN", "1"))

PAGE_LOAD_TIMEOUT = int(os.getenv("PAGE_LOAD_TIMEOUT", "120"))
LIST_PAGE_WAIT_TIMEOUT = int(os.getenv("LIST_PAGE_WAIT_TIMEOUT", "90"))
# После появления контента на stadiu-dosar: пауза на догрузку вкладок / лоадер (сек).
STADIU_LIST_SETTLE_SEC = float(os.getenv("STADIU_LIST_SETTLE_SEC", "12"))
HEADLESS = os.getenv("HEADLESS", "1").strip().lower() in ("1", "true", "yes")

CHROME_BIN = os.getenv("CHROME_BIN", "").strip()
CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH", "").strip()

# По умолчанию выключено: сайт лучше не дёргать «голым» urllib — Selenium + смена User-Agent.
# 1 = пробовать HTTP (меньше RAM; риск 503/блока с датацентра).
STADIU_PREFER_HTTP_PDF = os.getenv("STADIU_PREFER_HTTP_PDF", "0").strip().lower() in (
    "1",
    "true",
    "yes",
)

# Доп. флаги Chromium для контейнеров с малым объёмом памяти.
STADIU_CHROME_LOW_MEMORY = os.getenv("STADIU_CHROME_LOW_MEMORY", "1").strip().lower() in (
    "1",
    "true",
    "yes",
)

# По умолчанию выключено: список только через Selenium + UA (как PDF).
# 1 = сначала обычный GET (экономия RAM; возможны отличия от «браузерной» выдачи).
STADIU_PREFER_HTTP_LIST = os.getenv("STADIU_PREFER_HTTP_LIST", "0").strip().lower() in (
    "1",
    "true",
    "yes",
)

# Парсинг pdfplumber в дочернем процессе — после exit освобождается память движка PDF.
STADIU_PARSE_PDF_SUBPROCESS = os.getenv(
    "STADIU_PARSE_PDF_SUBPROCESS", "1"
).strip().lower() in ("1", "true", "yes")

POLL_INTERVAL_MIN_MINUTES = int(os.getenv("POLL_INTERVAL_MIN_MINUTES", "30"))
POLL_INTERVAL_MAX_MINUTES = int(os.getenv("POLL_INTERVAL_MAX_MINUTES", "45"))
# Если > 0 — фиксированная пауза между прогонами (мин), вместо случайного POLL_INTERVAL_*.
# Каждый прогон заново читает страницу stadiu-dosar и парсит href PDF; при смене имени файла на сайте
# обычно меняется URL — он попадёт в «новые». Пример: 240 ≈ 6 раз в сутки.
STADIU_LIST_POLL_MINUTES = int(os.getenv("STADIU_LIST_POLL_MINUTES", "0"))

# Устойчивость сети (Railway / блокировки / rate limit)
STADIU_HTTP_DOWNLOAD_ATTEMPTS = int(os.getenv("STADIU_HTTP_DOWNLOAD_ATTEMPTS", "5"))
STADIU_LIST_HTTP_ATTEMPTS = int(os.getenv("STADIU_LIST_HTTP_ATTEMPTS", "4"))
STADIU_HTTP_RETRY_BASE_SEC = float(os.getenv("STADIU_HTTP_RETRY_BASE_SEC", "3"))
STADIU_SELENIUM_DOWNLOAD_ATTEMPTS = int(os.getenv("STADIU_SELENIUM_DOWNLOAD_ATTEMPTS", "3"))
STADIU_BETWEEN_PDF_SEC = float(os.getenv("STADIU_BETWEEN_PDF_SEC", "8"))
