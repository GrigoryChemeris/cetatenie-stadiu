#!/usr/bin/env python3
"""Один прогон без планировщика (отладка / cron)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from stadiu_ingest.run_stadiu_once import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
