from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Iterable

from stadiu_ingest.config import DATABASE_URL, PG_CONNECT_TIMEOUT, SQLITE_PATH

_USE_PG = bool(DATABASE_URL)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _postgres_dsn() -> str:
    from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
    import socket

    u = (DATABASE_URL or "").strip()
    if not u:
        return u
    parsed = urlparse(u)
    host = (parsed.hostname or "").lower()
    if "railway.internal" in host:
        return u
    if "rlwy.net" not in host and "proxy.rlwy" not in host:
        return u

    q = dict(parse_qsl(parsed.query, keep_blank_values=True))
    keys_lower = {k.lower() for k in q}
    if "sslmode" not in keys_lower:
        q["sslmode"] = "require"
    if "gssencmode" not in keys_lower:
        q["gssencmode"] = "disable"
    if "hostaddr" not in keys_lower and parsed.hostname:
        try:
            port = parsed.port or 5432
            for fam, _, _, _, sockaddr in socket.getaddrinfo(
                parsed.hostname, port, type=socket.SOCK_STREAM
            ):
                if fam == socket.AF_INET:
                    q["hostaddr"] = sockaddr[0]
                    break
        except OSError:
            pass
    return urlunparse(parsed._replace(query=urlencode(q)))


def _pg_connect():
    import psycopg

    return psycopg.connect(
        _postgres_dsn(),
        connect_timeout=PG_CONNECT_TIMEOUT,
    )


_SQLITE_DDL = """
CREATE TABLE IF NOT EXISTS stadiu_list_documents (
    url TEXT PRIMARY KEY,
    source_filename TEXT,
    list_year TEXT,
    snapshot_update_date TEXT,
    content_sha256 TEXT,
    downloaded_at TEXT,
    parsed_ok INTEGER NOT NULL DEFAULT 0,
    row_count INTEGER,
    parse_error TEXT
);

CREATE TABLE IF NOT EXISTS stadiu_list_lines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_url TEXT NOT NULL,
    dossier_ref TEXT NOT NULL,
    registered_date TEXT,
    termen_solutie TEXT,
    FOREIGN KEY (doc_url) REFERENCES stadiu_list_documents(url)
);

CREATE INDEX IF NOT EXISTS idx_stadiu_lines_doc ON stadiu_list_lines(doc_url);
"""

_POSTGRES_DDL = """
CREATE TABLE IF NOT EXISTS stadiu_list_documents (
    url TEXT PRIMARY KEY,
    source_filename TEXT,
    list_year TEXT,
    snapshot_update_date TEXT,
    content_sha256 TEXT,
    downloaded_at TIMESTAMPTZ,
    parsed_ok BOOLEAN NOT NULL DEFAULT FALSE,
    row_count INTEGER,
    parse_error TEXT
);

CREATE TABLE IF NOT EXISTS stadiu_list_lines (
    id SERIAL PRIMARY KEY,
    doc_url TEXT NOT NULL REFERENCES stadiu_list_documents(url) ON DELETE CASCADE,
    dossier_ref TEXT NOT NULL,
    registered_date TEXT,
    termen_solutie TEXT
);

CREATE INDEX IF NOT EXISTS idx_stadiu_lines_doc ON stadiu_list_lines(doc_url);
"""


@contextmanager
def get_conn() -> Generator[Any, None, None]:
    if _USE_PG:
        conn = _pg_connect()
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()
    else:
        SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(SQLITE_PATH)
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()


def init_db() -> None:
    if _USE_PG:
        with _pg_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_POSTGRES_DDL)
            conn.commit()
    else:
        SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(SQLITE_PATH) as conn:
            conn.executescript(_SQLITE_DDL)


def known_stadiu_urls() -> set[str]:
    with get_conn() as conn:
        if _USE_PG:
            with conn.cursor() as cur:
                cur.execute("SELECT url FROM stadiu_list_documents")
                rows = cur.fetchall()
        else:
            rows = conn.execute("SELECT url FROM stadiu_list_documents").fetchall()
    return {r[0] for r in rows}


def insert_stadiu_document_meta(
    url: str,
    *,
    source_filename: str,
    list_year: str | None,
    snapshot_update_date: str | None,
    content_sha256: str,
    parsed_ok: bool,
    row_count: int | None,
    parse_error: str | None,
) -> None:
    now = _utc_now_iso()
    ok = parsed_ok
    if _USE_PG:
        sql = """
        INSERT INTO stadiu_list_documents (
            url, source_filename, list_year, snapshot_update_date, content_sha256,
            downloaded_at, parsed_ok, row_count, parse_error
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (url) DO UPDATE SET
            source_filename = EXCLUDED.source_filename,
            list_year = EXCLUDED.list_year,
            snapshot_update_date = EXCLUDED.snapshot_update_date,
            content_sha256 = EXCLUDED.content_sha256,
            downloaded_at = EXCLUDED.downloaded_at,
            parsed_ok = EXCLUDED.parsed_ok,
            row_count = EXCLUDED.row_count,
            parse_error = EXCLUDED.parse_error
        """
        params = (
            url,
            source_filename,
            list_year,
            snapshot_update_date,
            content_sha256,
            now,
            ok,
            row_count,
            parse_error,
        )
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
    else:
        with get_conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO stadiu_list_documents (
                    url, source_filename, list_year, snapshot_update_date, content_sha256,
                    downloaded_at, parsed_ok, row_count, parse_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    url,
                    source_filename,
                    list_year,
                    snapshot_update_date,
                    content_sha256,
                    now,
                    1 if ok else 0,
                    row_count,
                    parse_error,
                ),
            )


def replace_stadiu_lines(doc_url: str, lines: Iterable[dict[str, str]]) -> None:
    if _USE_PG:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM stadiu_list_lines WHERE doc_url = %s", (doc_url,)
                )
                for row in lines:
                    cur.execute(
                        """
                        INSERT INTO stadiu_list_lines (doc_url, dossier_ref, registered_date, termen_solutie)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            doc_url,
                            row["dossier_ref"],
                            row["registered_date"],
                            row["termen_solutie"],
                        ),
                    )
    else:
        with get_conn() as conn:
            conn.execute(
                "DELETE FROM stadiu_list_lines WHERE doc_url = ?", (doc_url,)
            )
            for row in lines:
                conn.execute(
                    """
                    INSERT INTO stadiu_list_lines (doc_url, dossier_ref, registered_date, termen_solutie)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        doc_url,
                        row["dossier_ref"],
                        row["registered_date"],
                        row["termen_solutie"],
                    ),
                )
