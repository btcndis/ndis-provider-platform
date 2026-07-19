from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class PdfCandidate:
    url: str
    source_page: str | None = None
    anchor_text: str | None = None
    discovery_method: str = "html_link"
    depth: int = 0


@dataclass(slots=True)
class DownloadResult:
    success: bool
    url: str
    final_url: str | None = None
    http_status: int | None = None
    content_type: str | None = None
    content_disposition: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    sha256: str | None = None
    size_bytes: int = 0
    filename: str | None = None
    temp_path: Path | None = None
    redirect_chain: list[str] = field(default_factory=list)
    acquisition_strategy: str = "httpx_stream"
    error: str | None = None

    def serializable(self) -> dict[str, Any]:
        data = asdict(self)
        if self.temp_path is not None:
            data["temp_path"] = str(self.temp_path)
        return data


@dataclass(slots=True)
class StoredPdf:
    url: str
    final_url: str
    sha256: str
    size_bytes: int
    storage_key: str
    object_path: str
    filename: str
    source_page: str | None
    anchor_text: str | None
    discovery_method: str
    acquisition_strategy: str
    content_type: str | None
    content_disposition: str | None
    etag: str | None
    last_modified: str | None
    redirect_chain: list[str]
    already_existed: bool
    acquired_at: str


@dataclass(slots=True)
class RunSummary:
    started_at: str
    finished_at: str | None = None
    pages_seen: int = 0
    pages_crawled: int = 0
    pages_failed: int = 0
    pdf_urls_seen: int = 0
    pdf_downloaded: int = 0
    pdf_unchanged_or_duplicate: int = 0
    pdf_failed: int = 0
    bytes_downloaded: int = 0
    manifest_path: str | None = None
