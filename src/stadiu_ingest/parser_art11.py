"""
PDF «Art-11-YYYY-Update-DD.MM.YYYY»: списки stadiu dosar (Art. 11).

Колонки в документе ANC:
  NR. DOSAR — N/RD/год_подачи (в PDF часто N\\RD\\год; порядковый N в году).
  DATA ÎNREGISTRĂRII — дата подачи документов.
  TERMEN — ориентировочная дата рассмотрения (часто не соблюдается); может быть пусто.
  SOLUTIE — номер приказа (формат …/P/год). Пусто = решение ещё не присвоено номер в этом списке.
    Отказы часто не публикуются отдельным приказом: если в списке приказов (cetatenie-mvp)
    нет соответствующего номера — по смыслу это отказ (логику сопоставления делать в SQL/отчётах).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

# Строка данных: досье, дата регистрации, остаток — TERMEN (дата?) и/или SOLUTIE (…/P/…)
_ROW_RE = re.compile(
    r"^(\d+\\RD\\\d{4})\s+(\d{2}\.\d{2}\.\d{4})\s*(.*)$",
)
_DATE = re.compile(r"^\d{2}\.\d{2}\.\d{4}$")
# Номер приказа в SOLUTIE: 1061/P/2026
_SOLUTIE_END = re.compile(r"(\d+/P/\d{4})\s*$")

_FILENAME_RE = re.compile(
    r"^Art-11-(\d{4})-Update-(\d{2}\.\d{2}\.\d{4})$",
    re.IGNORECASE,
)


def parse_filename_meta(stem: str) -> dict[str, str | None]:
    m = _FILENAME_RE.match(stem)
    if not m:
        return {"list_year": None, "snapshot_update_date": None}
    return {
        "list_year": m.group(1),
        "snapshot_update_date": m.group(2),
    }


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


def parse_art11_submission_pdf(path: Path) -> tuple[dict[str, Any], list[dict[str, str | None]]]:
    import pdfplumber

    stem = path.stem
    meta = parse_filename_meta(stem)
    meta["source_filename"] = path.name

    rows: list[dict[str, str | None]] = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for line in text.splitlines():
                line = line.strip()
                if not line or "NR. DOSAR" in line:
                    continue
                if "DATA" in line and "ÎNREGISTRĂRII" in line:
                    continue
                if "TERMEN" in line and "SOLUTIE" in line:
                    continue
                m = _ROW_RE.match(line)
                if not m:
                    continue
                termen, solutie = split_termen_solutie(m.group(3))
                rows.append(
                    {
                        "dossier_ref": m.group(1).replace("\\", "/"),
                        "registered_date": m.group(2),
                        "termen_date": termen,
                        "solutie_order": solutie,
                    }
                )

    meta["row_count"] = len(rows)
    return meta, rows
