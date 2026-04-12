from __future__ import annotations

import logging
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Generator, Iterable, Mapping

from stadiu_ingest.config import DATABASE_URL, PG_CONNECT_TIMEOUT, SQLITE_PATH

_USE_PG = bool(DATABASE_URL)
_log = logging.getLogger(__name__)

# N/RD/год (после нормализации в парсере)
_DOSSIER_REF_PARTS = re.compile(r"^(\d+)/RD/(\d{4})\s*$", re.IGNORECASE)


def parse_dossier_ref_parts(dossier_ref: str | None) -> tuple[int | None, int | None]:
    """Номер досье и год из канонического dossier_ref."""
    dr = (dossier_ref or "").strip()
    if not dr:
        return None, None
    m = _DOSSIER_REF_PARTS.match(dr)
    if not m:
        return None, None
    try:
        return int(m.group(1)), int(m.group(2))
    except ValueError:
        return None, None


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
    dossier_num INTEGER,
    dossier_year INTEGER,
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
    dossier_num INTEGER,
    dossier_year INTEGER,
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


def _migrate_stadiu_lines_dossier_columns(conn: Any, *, is_pg: bool) -> None:
    """dossier_num / dossier_year + индексы для сортировки по году и номеру."""

    def _backfill_lines(cursor: Any, sql_select: str, is_pg_inner: bool) -> None:
        placeholder = "%s" if is_pg_inner else "?"
        cursor.execute(sql_select)
        batch: list[tuple[int | None, int | None, int]] = []
        for rid, dr in cursor.fetchall():
            n, y = parse_dossier_ref_parts(dr)
            batch.append((n, y, rid))
            if len(batch) >= 2000:
                cursor.executemany(
                    f"""
                    UPDATE stadiu_list_lines SET dossier_num = {placeholder},
                        dossier_year = {placeholder}
                    WHERE id = {placeholder}
                    """,
                    batch,
                )
                batch.clear()
        if batch:
            cursor.executemany(
                f"""
                UPDATE stadiu_list_lines SET dossier_num = {placeholder},
                    dossier_year = {placeholder}
                WHERE id = {placeholder}
                """,
                batch,
            )

    if is_pg:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'stadiu_list_lines'
                """
            )
            line_cols = {r[0] for r in cur.fetchall()}
            if not line_cols:
                return
            added = False
            if "dossier_year" not in line_cols:
                cur.execute(
                    "ALTER TABLE stadiu_list_lines ADD COLUMN dossier_num INTEGER"
                )
                cur.execute(
                    "ALTER TABLE stadiu_list_lines ADD COLUMN dossier_year INTEGER"
                )
                added = True
            if added:
                _backfill_lines(cur, "SELECT id, dossier_ref FROM stadiu_list_lines", True)
            else:
                _backfill_lines(
                    cur,
                    """
                    SELECT id, dossier_ref FROM stadiu_list_lines
                    WHERE dossier_num IS NULL OR dossier_year IS NULL
                    """,
                    True,
                )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_stadiu_lines_doc_year_num
                ON stadiu_list_lines (doc_url, dossier_year, dossier_num)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_stadiu_documents_list_year_url
                ON stadiu_list_documents (list_year, url)
                """
            )
    else:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(stadiu_list_lines)")
        line_cols = {r[1] for r in cur.fetchall()}
        if not line_cols:
            return
        added = False
        if "dossier_year" not in line_cols:
            cur.execute("ALTER TABLE stadiu_list_lines ADD COLUMN dossier_num INTEGER")
            cur.execute("ALTER TABLE stadiu_list_lines ADD COLUMN dossier_year INTEGER")
            added = True
        if added:
            _backfill_lines(cur, "SELECT id, dossier_ref FROM stadiu_list_lines", False)
        else:
            _backfill_lines(
                cur,
                """
                SELECT id, dossier_ref FROM stadiu_list_lines
                WHERE dossier_num IS NULL OR dossier_year IS NULL
                """,
                False,
            )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_stadiu_lines_doc_year_num
            ON stadiu_list_lines (doc_url, dossier_year, dossier_num)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_stadiu_documents_list_year_url
            ON stadiu_list_documents (list_year, url)
            """
        )


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
            _migrate_stadiu_lines_dossier_columns(conn, is_pg=True)
            conn.commit()
    else:
        SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(SQLITE_PATH) as conn:
            conn.executescript(_SQLITE_DDL)
            _migrate_stadiu_lines_old_column(conn, is_pg=False)
            _migrate_stadiu_lines_dossier_columns(conn, is_pg=False)


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


def _dossier_num_year_from_row(row: Mapping[str, Any], dossier_ref: str) -> tuple[int | None, int | None]:
    n_raw = row.get("dossier_num")
    y_raw = row.get("dossier_year")
    if n_raw is not None and y_raw is not None:
        try:
            return int(n_raw), int(y_raw)
        except (TypeError, ValueError):
            pass
    return parse_dossier_ref_parts(dossier_ref)


def _stadiu_line_snapshot(
    row: Mapping[str, Any], dossier_ref: str
) -> tuple[str | None, str | None, str | None, int | None, int | None]:
    reg, term, sol = _stadiu_line_tuple(row)
    n, y = _dossier_num_year_from_row(row, dossier_ref)
    return (reg, term, sol, n, y)


def count_stadiu_lines_for_document(doc_url: str) -> int:
    """Число строк в stadiu_list_lines для данного doc_url."""
    with get_conn() as conn:
        if _USE_PG:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*)::int FROM stadiu_list_lines WHERE doc_url = %s",
                    (doc_url,),
                )
                row = cur.fetchone()
                return int(row[0]) if row and row[0] is not None else 0
        row = conn.execute(
            "SELECT COUNT(*) FROM stadiu_list_lines WHERE doc_url = ?",
            (doc_url,),
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else 0


def mark_stadiu_document_merge_mismatch(
    url: str,
    *,
    parse_error: str,
    row_count: int,
) -> None:
    """Пометить документ после merge с 0 строк при ненулевом парсе (рассинхрон метаданных)."""
    with get_conn() as conn:
        if _USE_PG:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE stadiu_list_documents SET
                        parsed_ok = FALSE,
                        row_count = %s,
                        parse_error = %s
                    WHERE url = %s
                    """,
                    (row_count, parse_error[:2000], url),
                )
        else:
            conn.execute(
                """
                UPDATE stadiu_list_documents SET
                    parsed_ok = 0,
                    row_count = ?,
                    parse_error = ?
                WHERE url = ?
                """,
                (row_count, parse_error[:2000], url),
            )


def merge_stadiu_lines(doc_url: str, lines: Iterable[Mapping[str, Any]]) -> None:
    """
    Слияние снимка PDF с БД по ключу (doc_url, dossier_ref).

    - Строка есть в новом парсе и не было в БД → INSERT.
    - Была и поля те же → ничего не делаем.
    - Была, но изменились даты / решение / dossier_num|year → UPDATE.
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
                    SELECT dossier_ref, registered_date, termen_date, solutie_order,
                           dossier_num, dossier_year
                    FROM stadiu_list_lines WHERE doc_url = %s
                    """,
                    (doc_url,),
                )
                existing_rows = cur.fetchall()
                first_vals: dict[
                    str,
                    tuple[str | None, str | None, str | None, int | None, int | None],
                ] = {}
                for r in existing_rows:
                    dr = _normalize_stadiu_cell(r[0])
                    if not dr or dr in first_vals:
                        continue
                    n_e, y_e = r[4], r[5]
                    if n_e is None or y_e is None:
                        n_e, y_e = parse_dossier_ref_parts(dr)
                    else:
                        try:
                            n_e = int(n_e)
                            y_e = int(y_e)
                        except (TypeError, ValueError):
                            n_e, y_e = parse_dossier_ref_parts(dr)
                    first_vals[dr] = (
                        _normalize_stadiu_cell(r[1]),
                        _normalize_stadiu_cell(r[2]),
                        _normalize_stadiu_cell(r[3]),
                        n_e,
                        y_e,
                    )
                for dr, row in by_ref.items():
                    new_t = _stadiu_line_snapshot(row, dr)
                    if dr not in first_vals:
                        cur.execute(
                            """
                            INSERT INTO stadiu_list_lines (
                                doc_url, dossier_ref, dossier_num, dossier_year,
                                registered_date, termen_date, solutie_order
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                doc_url,
                                dr,
                                new_t[3],
                                new_t[4],
                                new_t[0],
                                new_t[1],
                                new_t[2],
                            ),
                        )
                    elif first_vals[dr] != new_t:
                        cur.execute(
                            """
                            UPDATE stadiu_list_lines SET
                                registered_date = %s,
                                termen_date = %s,
                                solutie_order = %s,
                                dossier_num = %s,
                                dossier_year = %s
                            WHERE doc_url = %s AND dossier_ref = %s
                            """,
                            (
                                new_t[0],
                                new_t[1],
                                new_t[2],
                                new_t[3],
                                new_t[4],
                                doc_url,
                                dr,
                            ),
                        )
        else:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT dossier_ref, registered_date, termen_date, solutie_order,
                       dossier_num, dossier_year
                FROM stadiu_list_lines WHERE doc_url = ?
                """,
                (doc_url,),
            )
            existing_rows = cur.fetchall()
            first_vals = {}
            for r in existing_rows:
                dr = _normalize_stadiu_cell(r[0])
                if not dr or dr in first_vals:
                    continue
                n_e, y_e = r[4], r[5]
                if n_e is None or y_e is None:
                    n_e, y_e = parse_dossier_ref_parts(dr)
                else:
                    try:
                        n_e = int(n_e)
                        y_e = int(y_e)
                    except (TypeError, ValueError):
                        n_e, y_e = parse_dossier_ref_parts(dr)
                first_vals[dr] = (
                    _normalize_stadiu_cell(r[1]),
                    _normalize_stadiu_cell(r[2]),
                    _normalize_stadiu_cell(r[3]),
                    n_e,
                    y_e,
                )
            for dr, row in by_ref.items():
                new_t = _stadiu_line_snapshot(row, dr)
                if dr not in first_vals:
                    cur.execute(
                        """
                        INSERT INTO stadiu_list_lines (
                            doc_url, dossier_ref, dossier_num, dossier_year,
                            registered_date, termen_date, solutie_order
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            doc_url,
                            dr,
                            new_t[3],
                            new_t[4],
                            new_t[0],
                            new_t[1],
                            new_t[2],
                        ),
                    )
                elif first_vals[dr] != new_t:
                    cur.execute(
                        """
                        UPDATE stadiu_list_lines SET
                            registered_date = ?,
                            termen_date = ?,
                            solutie_order = ?,
                            dossier_num = ?,
                            dossier_year = ?
                        WHERE doc_url = ? AND dossier_ref = ?
                        """,
                        (
                            new_t[0],
                            new_t[1],
                            new_t[2],
                            new_t[3],
                            new_t[4],
                            doc_url,
                            dr,
                        ),
                    )


def finalize_stadiu_lines_for_document(
    doc_url: str, lines: list[Mapping[str, Any]]
) -> None:
    """
    merge_stadiu_lines + проверка: при 0 строк в БД при ненулевом парсе — mark_stadiu_document_merge_mismatch.
    Использовать вместо прямого merge из run_stadiu_once / reparse / ingest.
    """
    try:
        merge_stadiu_lines(doc_url, lines)
    except Exception as e:  # noqa: BLE001
        _log.exception("merge_stadiu_lines %s", doc_url[:120])
        mark_stadiu_document_merge_mismatch(
            doc_url,
            parse_error=f"merge_stadiu_lines: {e!r}",
            row_count=0,
        )
        return

    n_db = count_stadiu_lines_for_document(doc_url)
    n_par = len(lines)
    if n_db == 0 and n_par > 0:
        msg = (
            f"после merge в БД 0 строк при {n_par} строках парсера "
            f"(doc_url={doc_url[:200]})"
        )
        _log.error("stadiu: %s", msg)
        mark_stadiu_document_merge_mismatch(
            doc_url,
            parse_error=msg,
            row_count=0,
        )
    elif n_db != n_par:
        _log.warning(
            "stadiu: строк в БД=%s, в парсере=%s (документ %s)",
            n_db,
            n_par,
            doc_url[:100],
        )


def list_stadiu_documents_suspicious_zero_lines(
    *,
    limit: int = 500,
    include_all_zero_line_docs: bool = False,
) -> list[tuple[Any, ...]]:
    """
    Документы с 0 строк в stadiu_list_lines.

    По умолчанию — «подозрительные»: parsed_ok или row_count>0, а строк нет.
    С include_all_zero_line_docs — любой документ без строк.
    """
    with get_conn() as conn:
        if _USE_PG:
            zero = "(SELECT COUNT(*)::bigint FROM stadiu_list_lines l WHERE l.doc_url = d.url) = 0"
            if include_all_zero_line_docs:
                cond = zero
                params: tuple[Any, ...] = (limit,)
            else:
                cond = f"{zero} AND (d.parsed_ok OR COALESCE(d.row_count, 0) > 0)"
                params = (limit,)
            sql = f"""
            SELECT d.url, d.parsed_ok, d.row_count, d.parse_error,
                   (SELECT COUNT(*)::bigint FROM stadiu_list_lines l WHERE l.doc_url = d.url)
            FROM stadiu_list_documents d
            WHERE {cond}
            ORDER BY d.downloaded_at DESC NULLS LAST
            LIMIT %s
            """
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return list(cur.fetchall())
        zero = "(SELECT COUNT(*) FROM stadiu_list_lines l WHERE l.doc_url = d.url) = 0"
        if include_all_zero_line_docs:
            cond = zero
            params = (limit,)
        else:
            cond = f"{zero} AND (d.parsed_ok != 0 OR COALESCE(d.row_count, 0) > 0)"
            params = (limit,)
        sql = f"""
        SELECT d.url, d.parsed_ok, d.row_count, d.parse_error,
               (SELECT COUNT(*) FROM stadiu_list_lines l WHERE l.doc_url = d.url)
        FROM stadiu_list_documents d
        WHERE {cond}
        ORDER BY d.downloaded_at DESC
        LIMIT ?
        """
        cur = conn.execute(sql, params)
        return list(cur.fetchall())
