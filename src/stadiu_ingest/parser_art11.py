"""
PDF «Art-11-YYYY-Update-DD.MM.YYYY»: таблица NR. DOSAR / DATA ÎNREGISTRĂRII / TERMEN SOLUTIE.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

_ROW_RE = re.compile(
    r"^(\d+\\RD\\\d{4})\s+(\d{2}\.\d{2}\.\d{4})\s+(.+)$",
)
# Имя файла с сайта: Art-11-2024-Update-08.04.2026.pdf
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


def parse_art11_submission_pdf(path: Path) -> tuple[dict[str, Any], list[dict[str, str]]]:
    import pdfplumber

    stem = path.stem
    meta = parse_filename_meta(stem)
    meta["source_filename"] = path.name

    rows: list[dict[str, str]] = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for line in text.splitlines():
                line = line.strip()
                if not line or "NR. DOSAR" in line:
                    continue
                if "DATA" in line and "ÎNREGISTRĂRII" in line:
                    continue
                m = _ROW_RE.match(line)
                if not m:
                    continue
                rows.append(
                    {
                        "dossier_ref": m.group(1).replace("\\", "/"),
                        "registered_date": m.group(2),
                        "termen_solutie": m.group(3).strip(),
                    }
                )

    meta["row_count"] = len(rows)
    return meta, rows
