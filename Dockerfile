FROM python:3.12-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

ENV CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER_PATH=/usr/bin/chromedriver \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY run_stadiu_scheduler.py run_stadiu_once.py run_stadiu_service.py ./
COPY ingest_stadiu_pdfs.py list_art11_pdfs_from_html.py ./
COPY src ./src

CMD ["python", "run_stadiu_scheduler.py"]
