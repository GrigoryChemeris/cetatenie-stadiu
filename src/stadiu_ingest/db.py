from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Generator, Iterable, Mapping

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
    termen_date TEXT,
    solutie_order TEXT,
    FOREIGN KEY (doc_url) REFERENCES stadiu_list_documents(url)
);

CREATE INDEX IF NOT EXISTS idx_stadiu_lines_doc ON stadiu_list_lines(doc_url);

CREATE TABLE IF NOT EXISTS stadiu_url_aliases (
    list_url TEXT PRIMARY KEY,
    content_sha256 TEXT NOT NULL,
    canonical_url TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_stadiu_aliases_canonical ON stadiu_url_aliases(canonical_url);
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
    termen_date TEXT,
    solutie_order TEXT
);

CREATE INDEX IF NOT EXISTS idx_stadiu_lines_doc ON stadiu_list_lines(doc_url);

CREATE TABLE IF NOT EXISTS stadiu_url_aliases (
    list_url TEXT PRIMARY KEY,
    content_sha256 TEXT NOT NULL,
    canonical_url TEXT NOT NULL REFERENCES stadiu_list_documents(url) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_stadiu_aliases_canonical ON stadiu_url_aliases(canonical_url);
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


def _migrate_stadiu_lines_old_column(conn: Any, *, is_pg: bool) -> None:
    """Было termen_solutie одной строкой → termen_date + solutie_order."""
    from stadiu_ingest.parser_art11 import split_termen_solutie

    if is_pg:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'stadiu_list_lines'
                """
            )
            cols = {r[0] for r in cur.fetchall()}
            if not cols or "termen_date" in cols:
                return
            if "termen_solutie" not in cols:
                return
            cur.execute("ALTER TABLE stadiu_list_lines ADD COLUMN termen_date TEXT")
            cur.execute("ALTER TABLE stadiu_list_lines ADD COLUMN solutie_order TEXT")
            cur.execute(
                "SELECT id, termen_solutie FROM stadiu_list_lines WHERE termen_solutie IS NOT NULL"
            )
            for rid, ts in cur.fetchall():
                td, so = split_termen_solutie(ts)
                cur.execute(
                    "UPDATE stadiu_list_lines SET termen_date = %s, solutie_order = %s WHERE id = %s",
                    (td, so, rid),
                )
            cur.execute("ALTER TABLE stadiu_list_lines DROP COLUMN termen_solutie")
    else:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(stadiu_list_lines)")
        cols = {r[1] for r in cur.fetchall()}
        if not cols or "termen_date" in cols:
            return
        if "termen_solutie" not in cols:
            return
        cur.execute("ALTER TABLE stadiu_list_lines ADD COLUMN termen_date TEXT")
        cur.execute("ALTER TABLE stadiu_list_lines ADD COLUMN solutie_order TEXT")
        cur.execute("SELECT id, termen_solutie FROM stadiu_list_lines")
        for rid, ts in cur.fetchall():
            if ts:
                td, so = split_termen_solutie(ts)
                cur.execute(
                    "UPDATE stadiu_list_lines SET termen_date = ?, solutie_order = ? WHERE id = ?",
                    (td, so, rid),
                )
        try:
            cur.execute("ALTER TABLE stadiu_list_lines DROP COLUMN termen_solutie")
        except sqlite3.OperationalError:
            pass


def _exec_pg_ddl(conn: Any, ddl: str) -> None:
    with conn.cursor() as cur:
        for part in ddl.split(";"):
            stmt = part.strip()
            if stmt:
                cur.execute(stmt + ";")


def init_db() -> None:
    if _USE_PG:
        with _pg_connect() as conn:
            _exec_pg_ddl(conn, _POSTGRES_DDL)
            _migrate_stadiu_lines_old_column(conn, is_pg=True)
            conn.commit()
    else:
        SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(SQLITE_PATH) as conn:
            conn.executescript(_SQLITE_DDL)
            _migrate_stadiu_lines_old_column(conn, is_pg=False)


def known_stadiu_urls() -> set[str]:
    with get_conn() as conn:
        if _USE_PG:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT url FROM stadiu_list_documents
                    UNION
                    SELECT list_url FROM stadiu_url_aliases
                    """
                )
                rows = cur.fetchall()
        else:
            rows = conn.execute(
                """
                SELECT url FROM stadiu_list_documents
                UNION
                SELECT list_url FROM stadiu_url_aliases
                """
            ).fetchall()
    return {r[0] for r in rows}


def find_stadiu_urls_by_source_filename(filename: str) -> list[str]:
    """Точное совпадение source_filename (как после скачивания с сайта)."""
    fn = (filename or "").strip()
    if not fn:
        return []
    with get_conn() as conn:
        if _USE_PG:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT url FROM stadiu_list_documents
                    WHERE TRIM(source_filename) = %s
                    ORDER BY url
                    """,
                    (fn,),
                )
                rows = cur.fetchall()
        else:
            rows = conn.execute(
                """
                SELECT url FROM stadiu_list_documents
                WHERE TRIM(source_filename) = ?
                ORDER BY url
                """,
                (fn,),
            ).fetchall()
    return [str(r[0]).strip() for r in rows if r and r[0]]


def get_stadiu_source_filename_for_url(canonical_url: str) -> str | None:
    """Имя файла из stadiu_list_documents для канонического url."""
    u = (canonical_url or "").strip()
    if not u:
        return None
    with get_conn() as conn:
        if _USE_PG:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT source_filename FROM stadiu_list_documents WHERE url = %s LIMIT 1",
                    (u,),
                )
                row = cur.fetchone()
        else:
            row = conn.execute(
                "SELECT source_filename FROM stadiu_list_documents WHERE url = ? LIMIT 1",
                (u,),
            ).fetchone()
    if not row or row[0] is None:
        return None
    s = str(row[0]).strip()
    return s if s else None


def list_stadiu_document_urls_by_list_year() -> dict[str, set[str]]:
    """
    list_year → множество URL документов. Если на сайте сменили имя PDF (новый href) для того же года,
    новый URL не входит в множество — его можно приоритезировать в очереди скачивания.
    """
    with get_conn() as conn:
        if _USE_PG:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT list_year, url FROM stadiu_list_documents
                    WHERE list_year IS NOT NULL AND TRIM(list_year) <> ''
                    """
                )
                rows = cur.fetchall()
        else:
            rows = conn.execute(
                """
                SELECT list_year, url FROM stadiu_list_documents
                WHERE list_year IS NOT NULL AND TRIM(list_year) <> ''
                """
            ).fetchall()
    out: dict[str, set[str]] = {}
    for ly, url in rows:
        if not ly or not url:
            continue
        y = str(ly).strip()
        if not y:
            continue
        out.setdefault(y, set()).add(str(url).strip())
    return out


def resolve_stadiu_document_pk(href: str) -> str | None:
    """
    Первичный ключ строки в stadiu_list_documents для ссылки со страницы:
    сам URL или canonical_url, если href записан только как алиас.
    """
    with get_conn() as conn:
        if _USE_PG:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM stadiu_list_documents WHERE url = %s LIMIT 1",
                    (href,),
                )
                if cur.fetchone():
                    return href
                cur.execute(
                    "SELECT canonical_url FROM stadiu_url_aliases WHERE list_url = %s LIMIT 1",
                    (href,),
                )
                row = cur.fetchone()
        else:
            row = conn.execute(
                "SELECT 1 FROM stadiu_list_documents WHERE url = ? LIMIT 1", (href,)
            ).fetchone()
            if row:
                return href
            row = conn.execute(
                "SELECT canonical_url FROM stadiu_url_aliases WHERE list_url = ? LIMIT 1",
                (href,),
            ).fetchone()
    return row[0] if row else None


def get_stadiu_sha_and_downloaded_at(url_pk: str) -> tuple[str | None, str | None]:
    with get_conn() as conn:
        if _USE_PG:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT content_sha256, downloaded_at::text FROM stadiu_list_documents WHERE url = %s",
                    (url_pk,),
                )
                row = cur.fetchone()
        else:
            row = conn.execute(
                "SELECT content_sha256, downloaded_at FROM stadiu_list_documents WHERE url = ?",
                (url_pk,),
            ).fetchone()
    if not row:
        return None, None
    return row[0], row[1]


def touch_stadiu_downloaded_at(url_pk: str) -> None:
    now = _utc_now_iso()
    if _USE_PG:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE stadiu_list_documents SET downloaded_at = %s WHERE url = %s",
                    (now, url_pk),
                )
    else:
        with get_conn() as conn:
            conn.execute(
                "UPDATE stadiu_list_documents SET downloaded_at = ? WHERE url = ?",
                (now, url_pk),
            )


def stadiu_https_url_needs_refresh(href: str, min_interval_days: int) -> bool:
    """Пора ли перекачать тот же https-URL (файл мог обновиться на сервере)."""
    if min_interval_days <= 0:
        return False
    pk = resolve_stadiu_document_pk(href)
    if not pk or not pk.startswith(("http://", "https://")):
        return False
    _sha, downloaded_at = get_stadiu_sha_and_downloaded_at(pk)
    if not downloaded_at:
        return True
    raw = downloaded_at.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return True
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    return now - dt >= timedelta(days=min_interval_days)


def find_stadiu_canonical_url_by_sha256(content_sha256: str) -> str | None:
    with get_conn() as conn:
        if _USE_PG:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT url FROM stadiu_list_documents WHERE content_sha256 = %s LIMIT 1",
                    (content_sha256,),
                )
                row = cur.fetchone()
        else:
            row = conn.execute(
                "SELECT url FROM stadiu_list_documents WHERE content_sha256 = ? LIMIT 1",
                (content_sha256,),
            ).fetchone()
    return row[0] if row else None


def register_stadiu_pdf_url_alias(
    list_url: str, content_sha256: str, canonical_url: str
) -> None:
    if list_url == canonical_url:
        return
    if _USE_PG:
        sql = """
        INSERT INTO stadiu_url_aliases (list_url, content_sha256, canonical_url)
        VALUES (%s, %s, %s)
        ON CONFLICT (list_url) DO UPDATE SET
            content_sha256 = EXCLUDED.content_sha256,
            canonical_url = EXCLUDED.canonical_url
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (list_url, content_sha256, canonical_url))
    else:
        with get_conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO stadiu_url_aliases (list_url, content_sha256, canonical_url)
                VALUES (?, ?, ?)
                """,
                (list_url, content_sha256, canonical_url),
            )


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


def _normalize_stadiu_cell(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s if s else None
    return str(v)


def _stadiu_line_tuple(row: Mapping[str, Any]) -> tuple[str | None, str | None, str | None]:
    return (
        _normalize_stadiu_cell(row["registered_date"]),
        _normalize_stadiu_cell(row.get("termen_date")),
        _normalize_stadiu_cell(row.get("solutie_order")),
    )


def merge_stadiu_lines(doc_url: str, lines: Iterable[Mapping[str, Any]]) -> None:
    """
    Слияние снимка PDF с БД по ключу (doc_url, dossier_ref).

    - Строка есть в новом парсе и не было в БД → INSERT.
    - Была и поля те же → ничего не делаем.
    - Была, но изменились registered_date / termen_date / solutie_order → UPDATE только этих колонок.
    - Была в БД, но в новом PDF досье нет → строку не удаляем.
    """
    by_ref: dict[str, Mapping[str, Any]] = {}
    for row in lines:
        dr = _normalize_stadiu_cell(row.get("dossier_ref"))
        if not dr:
            continue
        by_ref[dr] = row

    with get_conn() as conn:
        if _USE_PG:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT dossier_ref, registered_date, termen_date, solutie_order
                    FROM stadiu_list_lines WHERE doc_url = %s
                    """,
                    (doc_url,),
                )
                existing_rows = cur.fetchall()
                first_vals: dict[str, tuple[str | None, str | None, str | None]] = {}
                for r in existing_rows:
                    dr = _normalize_stadiu_cell(r[0])
                    if not dr or dr in first_vals:
                        continue
                    first_vals[dr] = (
                        _normalize_stadiu_cell(r[1]),
                        _normalize_stadiu_cell(r[2]),
                        _normalize_stadiu_cell(r[3]),
                    )
                for dr, row in by_ref.items():
                    new_t = _stadiu_line_tuple(row)
                    if dr not in first_vals:
                        cur.execute(
                            """
                            INSERT INTO stadiu_list_lines (
                                doc_url, dossier_ref, registered_date, termen_date, solutie_order
                            )
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            (doc_url, dr, new_t[0], new_t[1], new_t[2]),
                        )
                    elif first_vals[dr] != new_t:
                        cur.execute(
                            """
                            UPDATE stadiu_list_lines SET
                                registered_date = %s,
                                termen_date = %s,
                                solutie_order = %s
                            WHERE doc_url = %s AND dossier_ref = %s
                            """,
                            (new_t[0], new_t[1], new_t[2], doc_url, dr),
                        )
        else:
            existing_rows = conn.execute(
                """
                SELECT dossier_ref, registered_date, termen_date, solutie_order
                FROM stadiu_list_lines WHERE doc_url = ?
                """,
                (doc_url,),
            ).fetchall()
            first_vals = {}
            for r in existing_rows:
                dr = _normalize_stadiu_cell(r[0])
                if not dr or dr in first_vals:
                    continue
                first_vals[dr] = (
                    _normalize_stadiu_cell(r[1]),
                    _normalize_stadiu_cell(r[2]),
                    _normalize_stadiu_cell(r[3]),
                )
            for dr, row in by_ref.items():
                new_t = _stadiu_line_tuple(row)
                if dr not in first_vals:
                    conn.execute(
                        """
                        INSERT INTO stadiu_list_lines (
                            doc_url, dossier_ref, registered_date, termen_date, solutie_order
                        )
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (doc_url, dr, new_t[0], new_t[1], new_t[2]),
                    )
                elif first_vals[dr] != new_t:
                    conn.execute(
                        """
                        UPDATE stadiu_list_lines SET
                            registered_date = ?,
                            termen_date = ?,
                            solutie_order = ?
                        WHERE doc_url = ? AND dossier_ref = ?
                        """,
                        (new_t[0], new_t[1], new_t[2], doc_url, dr),
                    )
