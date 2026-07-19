from __future__ import annotations

import contextlib
import json
import sqlite3
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

from .models import PdfCandidate, StoredPdf
from .urls import canonicalize_url

_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS pages (
    url TEXT PRIMARY KEY,
    parent_url TEXT,
    depth INTEGER NOT NULL DEFAULT 0,
    discovery_method TEXT NOT NULL DEFAULT 'seed',
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS ix_pages_status ON pages(status, depth);
CREATE TABLE IF NOT EXISTS pdfs (
    url TEXT PRIMARY KEY,
    source_page TEXT,
    anchor_text TEXT,
    discovery_method TEXT NOT NULL,
    depth INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    http_status INTEGER,
    final_url TEXT,
    content_type TEXT,
    content_disposition TEXT,
    etag TEXT,
    last_modified TEXT,
    sha256 TEXT,
    size_bytes INTEGER NOT NULL DEFAULT 0,
    storage_key TEXT,
    object_path TEXT,
    filename TEXT,
    acquisition_strategy TEXT,
    redirect_chain_json TEXT,
    first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    downloaded_at TEXT
);
CREATE INDEX IF NOT EXISTS ix_pdfs_status ON pdfs(status);
CREATE INDEX IF NOT EXISTS ix_pdfs_sha ON pdfs(sha256);
CREATE TABLE IF NOT EXISTS endpoints (
    url TEXT PRIMARY KEY,
    endpoint_type TEXT NOT NULL,
    status TEXT NOT NULL,
    detail TEXT,
    checked_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


class CrawlState:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(_SCHEMA)
        self.reset_in_progress()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> CrawlState:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def reset_in_progress(self) -> None:
        with self.conn:
            self.conn.execute("UPDATE pages SET status='pending' WHERE status='in_progress'")
            self.conn.execute("UPDATE pdfs SET status='pending' WHERE status='downloading'")

    def enqueue_page(
        self,
        url: str,
        *,
        parent_url: str | None = None,
        depth: int = 0,
        method: str = "html_link",
    ) -> bool:
        normalized = canonicalize_url(url)
        if not normalized:
            return False
        with self.conn:
            cursor = self.conn.execute(
                """
                INSERT OR IGNORE INTO pages(url,parent_url,depth,discovery_method)
                VALUES(?,?,?,?)
                """,
                (normalized, parent_url, depth, method),
            )
            inserted = cursor.rowcount == 1
            if not inserted:
                self.conn.execute(
                    """
                    UPDATE pages
                    SET last_seen_at=CURRENT_TIMESTAMP,
                        parent_url=COALESCE(parent_url, ?),
                        depth=MIN(depth, ?)
                    WHERE url=?
                    """,
                    (parent_url, depth, normalized),
                )
        return inserted

    def enqueue_pages(self, rows: Iterable[tuple[str, str | None, int, str]]) -> int:
        inserted = 0
        for url, parent_url, depth, method in rows:
            inserted += int(
                self.enqueue_page(url, parent_url=parent_url, depth=depth, method=method)
            )
        return inserted

    def enqueue_pdf(self, candidate: PdfCandidate) -> bool:
        normalized = canonicalize_url(candidate.url)
        if not normalized:
            return False
        with self.conn:
            cursor = self.conn.execute(
                """
                INSERT OR IGNORE INTO pdfs(
                    url,source_page,anchor_text,discovery_method,depth
                ) VALUES(?,?,?,?,?)
                """,
                (
                    normalized,
                    candidate.source_page,
                    candidate.anchor_text,
                    candidate.discovery_method,
                    candidate.depth,
                ),
            )
            inserted = cursor.rowcount == 1
            if not inserted:
                self.conn.execute(
                    """
                    UPDATE pdfs
                    SET last_seen_at=CURRENT_TIMESTAMP,
                        source_page=COALESCE(source_page, ?),
                        anchor_text=CASE
                            WHEN length(COALESCE(anchor_text,'')) < length(COALESCE(?,''))
                            THEN ? ELSE anchor_text END,
                        depth=MIN(depth, ?)
                    WHERE url=?
                    """,
                    (
                        candidate.source_page,
                        candidate.anchor_text,
                        candidate.anchor_text,
                        candidate.depth,
                        normalized,
                    ),
                )
        return inserted

    def enqueue_pdfs(self, candidates: Iterable[PdfCandidate]) -> int:
        return sum(int(self.enqueue_pdf(candidate)) for candidate in candidates)

    def claim_pages(self, limit: int) -> list[sqlite3.Row]:
        rows = self.conn.execute(
            "SELECT * FROM pages WHERE status='pending' ORDER BY depth, first_seen_at LIMIT ?",
            (limit,),
        ).fetchall()
        if rows:
            with self.conn:
                self.conn.executemany(
                    "UPDATE pages SET status='in_progress', attempts=attempts+1 WHERE url=?",
                    [(row["url"],) for row in rows],
                )
        return rows

    def claim_pdfs(self, limit: int) -> list[sqlite3.Row]:
        rows = self.conn.execute(
            "SELECT * FROM pdfs WHERE status='pending' ORDER BY first_seen_at LIMIT ?",
            (limit,),
        ).fetchall()
        if rows:
            with self.conn:
                self.conn.executemany(
                    "UPDATE pdfs SET status='downloading', attempts=attempts+1 WHERE url=?",
                    [(row["url"],) for row in rows],
                )
        return rows

    def mark_page(
        self,
        url: str,
        *,
        success: bool,
        error: str | None = None,
        retry: bool = False,
    ) -> None:
        status = "done" if success else ("pending" if retry else "failed")
        with self.conn:
            self.conn.execute(
                """
                UPDATE pages
                SET status=?, last_error=?, last_seen_at=CURRENT_TIMESTAMP
                WHERE url=?
                """,
                (status, error[:4000] if error else None, canonicalize_url(url)),
            )

    def mark_pdf_failed(
        self,
        url: str,
        *,
        error: str,
        http_status: int | None = None,
        retry: bool = False,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE pdfs
                SET status=?, last_error=?, http_status=?, last_seen_at=CURRENT_TIMESTAMP
                WHERE url=?
                """,
                (
                    "pending" if retry else "failed",
                    error[:4000],
                    http_status,
                    canonicalize_url(url),
                ),
            )

    def mark_pdf_not_modified(self, url: str) -> bool:
        normalized = canonicalize_url(url)
        row = self.conn.execute("SELECT sha256 FROM pdfs WHERE url=?", (normalized,)).fetchone()
        if row is None or not row["sha256"]:
            return False
        with self.conn:
            self.conn.execute(
                """
                UPDATE pdfs
                SET status='downloaded', last_error=NULL, http_status=304,
                    downloaded_at=CURRENT_TIMESTAMP, last_seen_at=CURRENT_TIMESTAMP
                WHERE url=?
                """,
                (normalized,),
            )
        return True

    def mark_pdf_stored(self, stored: StoredPdf) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE pdfs SET status='downloaded', last_error=NULL, http_status=200,
                    final_url=?, content_type=?, content_disposition=?, etag=?, last_modified=?,
                    sha256=?, size_bytes=?, storage_key=?, object_path=?, filename=?,
                    acquisition_strategy=?, redirect_chain_json=?, downloaded_at=CURRENT_TIMESTAMP,
                    last_seen_at=CURRENT_TIMESTAMP
                WHERE url=?
                """,
                (
                    stored.final_url,
                    stored.content_type,
                    stored.content_disposition,
                    stored.etag,
                    stored.last_modified,
                    stored.sha256,
                    stored.size_bytes,
                    stored.storage_key,
                    stored.object_path,
                    stored.filename,
                    stored.acquisition_strategy,
                    json.dumps(stored.redirect_chain),
                    canonicalize_url(stored.url),
                ),
            )

    def record_endpoint(
        self, url: str, endpoint_type: str, status: str, detail: str | None = None
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO endpoints(url,endpoint_type,status,detail) VALUES(?,?,?,?)
                ON CONFLICT(url) DO UPDATE SET endpoint_type=excluded.endpoint_type,
                    status=excluded.status, detail=excluded.detail,
                    checked_at=CURRENT_TIMESTAMP
                """,
                (url, endpoint_type, status, detail),
            )

    def count(self, table: str, status: str | None = None) -> int:
        if table not in {"pages", "pdfs", "endpoints"}:
            raise ValueError(table)
        if status is None:
            row = self.conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
        else:
            row = self.conn.execute(
                f"SELECT COUNT(*) AS n FROM {table} WHERE status=?", (status,)
            ).fetchone()
        return int(row["n"])

    def pending_work(self) -> bool:
        return bool(self.count("pages", "pending") or self.count("pdfs", "pending"))

    def all_downloaded(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM pdfs WHERE status='downloaded' ORDER BY downloaded_at, url"
        ).fetchall()
        return [dict(row) for row in rows]

    def all_failures(self) -> list[dict[str, Any]]:
        query = """
            SELECT 'page' AS kind,url,status,attempts,last_error
            FROM pages WHERE status='failed'
            UNION ALL
            SELECT 'pdf' AS kind,url,status,attempts,last_error
            FROM pdfs WHERE status='failed'
            ORDER BY kind,url
        """
        return [dict(row) for row in self.conn.execute(query).fetchall()]

    def existing_conditional_headers(self, url: str) -> dict[str, str]:
        row = self.conn.execute(
            "SELECT etag,last_modified FROM pdfs WHERE url=?", (canonicalize_url(url),)
        ).fetchone()
        if row is None:
            return {}
        headers: dict[str, str] = {}
        if row["etag"]:
            headers["If-None-Match"] = row["etag"]
        if row["last_modified"]:
            headers["If-Modified-Since"] = row["last_modified"]
        return headers

    @contextlib.contextmanager
    def transaction(self) -> Iterator[None]:
        with self.conn:
            yield
