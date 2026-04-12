"""
PDF «Art-11-YYYY-Update-DD.MM.YYYY»: списки stadiu dosar (Art. 11).

Колонки в документе ANC:
  NR. DOSAR — N/RD/год, реже «1 /RD/2012» с пробелами; в тексте бывает N\\RD\\год.
  DATA ÎNREGISTRĂRII — дата подачи документов.
  TERMEN — ориентировочная дата рассмотрения (часто не соблюдается); может быть пусто.
  SOLUTIE / SOLUȚIE — …/P/год, …/P/дд.мм.гггг или редко «211/P 12.11.2010» (пробел после P).
    Отказы часто не публикуются отдельным приказом: если в списке приказов (cetatenie-mvp)
    нет соответствующего номера — по смыслу это отказ (эвристика и счётчики: в репозитории
    cetatenie-mvp скрипт ``stadiu_ordine_outcome.py``).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

# Досье: 1/RD/2013, 1\RD\2022, «1 /RD/2012» (пробелы вокруг слэшей).
_ROW_RE = re.compile(
    r"^\s*(\d+)\s*[/\\]\s*RD\s*[/\\]\s*(\d{4})\s+(\d{2}\.\d{2}\.\d{4})\s*(.*)$",
    re.IGNORECASE,
)
_DATE = re.compile(r"^\d{2}\.\d{2}\.\d{4}$")
# Приказ в хвосте: …/P/2024, …/P/06.08.2014, …/P 16.12.2010 (Redobandire 2010)
_SOLUTIE_END = re.compile(
    r"(\d+/P(?:/\d{4}|/\d{2}\.\d{2}\.\d{4}|\s+\d{2}\.\d{2}\.\d{4}))\s*$",
)

_FILENAME_RE = re.compile(
    r"^Art-11-(\d{4})-Update-(\d{2}\.\d{2}\.\d{4})$",
    re.IGNORECASE,
)
# Art._11_2013_Redobandire.pdf и похожие имена на сайте
_FILENAME_LOOSE_RE = re.compile(
    r"^Art[._-]+11[._-]+(\d{4})",
    re.IGNORECASE,
)


def parse_filename_meta(stem: str) -> dict[str, str | None]:
    m = _FILENAME_RE.match(stem)
    if m:
        return {
            "list_year": m.group(1),
            "snapshot_update_date": m.group(2),
        }
    m2 = _FILENAME_LOOSE_RE.match(stem)
    if m2:
        return {"list_year": m2.group(1), "snapshot_update_date": None}
    return {"list_year": None, "snapshot_update_date": None}


def meta_from_art11_pdf_url(url: str) -> dict[str, str | None]:
    """Год/update из имени файла в URL, если локальное имя после скачивания другое."""
    path = unquote(urlparse(url).path)
    return parse_filename_meta(Path(path).stem)


def split_termen_solutie(tail: str) -> tuple[str | None, str | None]:
    """
    Хвост строки после DATA ÎNREGISTRĂRII: опционально дата TERMEN, опционально N/P/YYYY.
    """
    t = tail.strip()
    if not t:
        return None, None

    solutie: str | None = None
    m = _SOLUTIE_END.search(t)
    if m:
        solutie = m.group(1)
        t = t[: m.start()].strip()

    termen: str | None = t if t else None
    if termen is not None and not _DATE.match(termen):
        # Редкий мусор в PDF — сохраняем как есть
        pass
    elif termen == "":
        termen = None

    return termen, solutie


def parse_art11_submission_pdf(path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    import pdfplumber

    stem = path.stem
    meta = parse_filename_meta(stem)
    meta["source_filename"] = path.name

    rows: list[dict[str, Any]] = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for line in text.splitlines():
                line = line.strip()
                if not line or "NR. DOSAR" in line:
                    continue
                if "DATA" in line and "ÎNREGISTRĂRII" in line:
                    continue
                if "TERMEN" in line and (
                    "SOLUTIE" in line or "SOLUȚIE" in line or "SOLUŢIE" in line
                ):
                    continue
                m = _ROW_RE.match(line)
                if not m:
                    continue
                termen, solutie = split_termen_solutie(m.group(4))
                num_s, year_s = m.group(1), m.group(2)
                dr = f"{num_s}/RD/{year_s}"
                rows.append(
                    {
                        "dossier_ref": dr,
                        "dossier_num": int(num_s),
                        "dossier_year": int(year_s),
                        "registered_date": m.group(3),
                        "termen_date": termen,
                        "solutie_order": solutie,
                    }
                )
            del page

    meta["row_count"] = len(rows)
    return meta, rows


def _parse_art11_submission_pdf_worker(
    path_str: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Топ-уровень для pickle/multiprocessing (нельзя nested-функцию в Pool)."""
    return parse_art11_submission_pdf(Path(path_str))


def parse_art11_submission_pdf_isolated(
    path: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """
    Парсинг в отдельном процессе (maxtasksperchild=1): после завершения воркера
    освобождается RSS pdfplumber/pdfium — полезно на Railway с малым лимитом RAM.
    """
    import multiprocessing as mp
    import sys

    ctx = mp.get_context("spawn" if sys.platform == "win32" else "fork")
    with ctx.Pool(1, maxtasksperchild=1) as pool:
        return pool.apply(
            _parse_art11_submission_pdf_worker,
            (str(path.resolve()),),
        )
