#!/usr/bin/env python3
"""Фоновый цикл: stadiu-dosar → Art. 11 PDF → БД."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from stadiu_ingest.scheduler import main  # noqa: E402

if __name__ == "__main__":
    main()
