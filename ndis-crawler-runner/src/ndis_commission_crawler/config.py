from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_BASE_URL = "https://www.ndiscommission.gov.au/"
DEFAULT_USER_AGENT = (
    "Veritaxon-NDIS-Commission-Acquirer/1.0 "
    "(+public-document-preservation; contact=admin@example.invalid)"
)


@dataclass(slots=True)
class CrawlSettings:
    base_url: str = DEFAULT_BASE_URL
    library_root: Path = Path("library")
    state_db: Path | None = None
    downloads_dir: Path | None = None
    checkpoint_file: Path | None = None
    user_agent: str = DEFAULT_USER_AGENT
    max_pages: int = 0
    max_depth: int = 20
    page_batch_size: int = 24
    download_batch_size: int = 8
    concurrency: int = 4
    request_delay_min: float = 0.75
    request_delay_max: float = 1.75
    page_timeout_ms: int = 120_000
    max_scroll_steps: int = 40
    max_download_bytes: int = 100 * 1024 * 1024
    max_retries: int = 5
    retry_backoff_base: float = 2.0
    headless: bool = True
    respect_robots: bool = True
    capture_debug: bool = True
    browser_download_fallback: bool = True
    verify_with_pdf_strategy: bool = False
    proxy_url: str | None = None
    extra_allowed_file_hosts: list[str] = field(default_factory=list)

    def finalize(self) -> CrawlSettings:
        self.library_root = self.library_root.expanduser().resolve()
        self.state_db = (
            (self.state_db or self.library_root / "metadata" / "ndis_commission_crawl.db")
            .expanduser()
            .resolve()
        )
        self.downloads_dir = (
            (self.downloads_dir or self.library_root / "tmp" / "ndis_commission_browser_downloads")
            .expanduser()
            .resolve()
        )
        self.checkpoint_file = (
            (
                self.checkpoint_file
                or self.library_root / "metadata" / "ndis_commission_deep_crawl_checkpoint.json"
            )
            .expanduser()
            .resolve()
        )
        self.library_root.mkdir(parents=True, exist_ok=True)
        self.state_db.parent.mkdir(parents=True, exist_ok=True)
        self.downloads_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        return self
