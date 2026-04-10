# cetatenie-stadiu

Отдельный сервис для PDF **списков подачи** (stadiu dosar, **Art. 11**): парсинг и хранение строк в Postgres/SQLite. Не зависит от `cetatenie-mvp` (приказы).

## Локально

```bash
cd cetatenie-stadiu
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL='postgresql://…'
PYTHONPATH=src python ingest_stadiu_pdfs.py /path/to/Art-11-2024-Update-08.04.2026.pdf
```

## Railway (отдельный блок)

1. **New** → **GitHub** → репозиторий только с этой папкой (или монорепо с **Root Directory** = `cetatenie-stadiu`).
2. В **Variables** задайте тот же `DATABASE_URL`, что у Postgres (или ссылку через **Reference**), чтобы таблицы `stadiu_*` жили в общей БД.
3. Деплой поднимает `run_stadiu_service.py` (пока только heartbeat). Разовый импорт: **Railway → Shell / `railway run`**:

   `PYTHONPATH=src python ingest_stadiu_pdfs.py /tmp/file.pdf`

## Таблицы

- `stadiu_list_documents` — метаданные снимка (год списка, дата update из имени файла, sha256).
- `stadiu_list_lines` — `dossier_ref`, `registered_date`, `termen_solutie`.

Имена не пересекаются с `pdf_documents` / `pdf_dossier_lines` в `cetatenie-mvp`.
