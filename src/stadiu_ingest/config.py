import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
PG_CONNECT_TIMEOUT = int(os.getenv("PG_CONNECT_TIMEOUT", "120"))
SQLITE_PATH = Path(os.getenv("SQLITE_PATH", "./data/stadiu.db")).resolve()
