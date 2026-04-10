FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY run_stadiu_service.py ./
COPY ingest_stadiu_pdfs.py ./
COPY list_art11_pdfs_from_html.py ./
COPY src ./src

CMD ["python", "run_stadiu_service.py"]
