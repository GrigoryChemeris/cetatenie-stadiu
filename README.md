# cetatenie-stadiu

Отдельный сервис для PDF **списков подачи** (stadiu dosar, **Art. 11**): Selenium → скачивание PDF → парсинг → Postgres/SQLite. Не зависит от `cetatenie-mvp` (приказы).

**User-Agent:** свой пул в `src/stadiu_ingest/user_agents.py` (не те же строки, что в первом сервисе). По умолчанию **и список, и PDF** идут через **Selenium** с сменой UA на каждый PDF; «голый» HTTP отключён (`STADIU_PREFER_HTTP_*`), чтобы реже ловить 503/блок с датацентра.

**Логика как у первого сервиса:** Chromium, ожидание anti-bot / появления ссылок `.pdf`, скачивание в temp, SHA-256, дедуп по URL и по содержимому (`stadiu_url_aliases`). За один прогон — ограниченное число новых PDF (см. `.env.example`).

## Страница stadiu-dosar и артикул 11

На https://cetatenie.just.ro/stadiu-dosar/ несколько вкладок. В DOM все панели уже есть; **Art. 11** — только блок **`id="articolul-11-tab"`** (`extract_art11_pdf_links_from_html`).

## Локально: один прогон (Selenium)

Нужны Chrome/Chromedriver или пути в env (как в первом проекте).

```bash
cd cetatenie-stadiu
source .venv/bin/activate
export DATABASE_URL='postgresql://…'   # или без него → SQLite
PYTHONPATH=src python run_stadiu_once.py
```

## Локально: только PDF с диска

```bash
PYTHONPATH=src python ingest_stadiu_pdfs.py /path/to/Art-11-2024-Update-08.04.2026.pdf
```

## Перепарсить уже известный https-документ (тот же PDF на сервере)

`run_stadiu_once` не вызывает парсер, если sha256 файла не изменился. После правок `parser_art11.py` для Redobandire и т.п.:

```bash
PYTHONPATH=src python reparse_stadiu_https.py \
  --filename Art._11_2010_Redobandire.pdf \
  --filename Art._11_2012_Redobandire.pdf \
  --filename Art._11_2013_Redobandire.pdf \
  --local-dir ~/Downloads
```

Без `--local-dir` скрипт качает по URL из БД: при `STADIU_PREFER_HTTP_PDF=1` — только HTTP; иначе нужен Selenium (как у `run_stadiu_once`). Если все три PDF уже в каталоге и имена совпадают с `source_filename`, достаточно `--local-dir` без Selenium.

## Список URL из сохранённого HTML

```bash
PYTHONPATH=src python list_art11_pdfs_from_html.py ~/Downloads/stadiu-dosar.html
```

## Railway (отдельный блок)

1. Репозиторий / **Root Directory** = `cetatenie-stadiu`.
2. `DATABASE_URL` → тот же Postgres, что у `cetatenie-mvp` (таблицы `stadiu_*`, `stadiu_url_aliases`).
3. **Dockerfile** поднимает `run_stadiu_scheduler.py` (цикл прогонов + пауза **30–45** мин по умолчанию).

Переменные: см. `.env.example`.

## Таблицы

- `stadiu_list_documents` — ключ `url`: для с сайта это **https URL файла**, для локального импорта — `local:<sha256>`.
- `stadiu_list_lines` — строки списка: `dossier_ref` (канонически `N/RD/год`), **`dossier_num`**, **`dossier_year`** (для сортировки и фильтров), `registered_date`, `termen_date`, `solutie_order`. Повторный парсинг того же `doc_url`: по паре (`doc_url`, `dossier_ref`) — `INSERT` новых, `UPDATE` при изменении дат/решения/нормализации; строки, которых больше нет в PDF, **не удаляются**. Индексы: `(doc_url, dossier_year, dossier_num)` и у документов `(list_year, url)`.
- Сортировка в запросах (SQL не хранит порядок «таблицы»): строки — `ORDER BY dossier_year NULLS LAST, dossier_num NULLS LAST` (или сначала `doc_url`, если нужен разрез по PDF); документы — `ORDER BY list_year NULLS LAST, url`.
- `stadiu_url_aliases` — тот же PDF по байтам уже сохранён под другим `url`; ссылка со страницы ведёт на каноническую запись (как `pdf_url_aliases` в первом сервисе).

Итог по отказам / `solutie_order` — см. прежний текст в `parser_art11.py` и комментарии в README ранее (сопоставление с приказами — отдельными запросами).
