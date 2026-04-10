FROM python:3.12-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

ENV CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER_PATH=/usr/bin/chromedriver \
    PYTHONUNBUFFERED=1 \
    MALLOC_ARENA_MAX=2 \
    STADIU_PREFER_HTTP_PDF=0 \
    STADIU_PREFER_HTTP_LIST=0 \
    STADIU_PARSE_PDF_SUBPROCESS=1 \
    STADIU_CHROME_LOW_MEMORY=1 \
    MAX_NEW_STADIU_DOWNLOADS=1 \
    MAX_STADIU_REFRESH_PER_RUN=1 \
    COLD_START_MAX_STADIU_PDFS=3

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY run_stadiu_scheduler.py run_stadiu_once.py run_stadiu_service.py ./
COPY ingest_stadiu_pdfs.py list_art11_pdfs_from_html.py ./
COPY src ./src

CMD ["python", "run_stadiu_scheduler.py"]
