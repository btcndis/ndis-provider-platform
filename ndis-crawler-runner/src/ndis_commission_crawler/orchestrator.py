from __future__ import annotations

import asyncio
import contextlib
from collections import Counter
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from .config import CrawlSettings
from .crawl4ai_engine import Crawl4AIEngine
from .downloader import Crawl4AIBrowserDownloadFallback, PdfDownloader
from .library import NdisLibrary
from .models import RunSummary
from .sitemaps import SitemapSeeder
from .state import CrawlState
from .urls import is_allowed_file_url


class NdisCommissionAcquirer:
    def __init__(self, settings: CrawlSettings) -> None:
        self.settings = settings.finalize()
        self.summary = RunSummary(started_at=datetime.now(UTC).isoformat())

    async def run(self) -> RunSummary:
        library = NdisLibrary(self.settings.library_root)
        state_db = self.settings.state_db
        assert state_db is not None
        with CrawlState(state_db) as state:
            seeder = SitemapSeeder(
                base_url=self.settings.base_url,
                user_agent=self.settings.user_agent,
                max_retries=self.settings.max_retries,
                state=state,
            )
            seed_outcome = await seeder.seed()
            for page in seed_outcome.page_urls:
                state.enqueue_page(page, parent_url=None, depth=0, method="sitemap_or_seed")
            for pdf in seed_outcome.pdf_urls:
                if is_allowed_file_url(
                    pdf.url,
                    self.settings.base_url,
                    self.settings.extra_allowed_file_hosts,
                ):
                    state.enqueue_pdf(pdf)

            engine = Crawl4AIEngine(self.settings, state, library)
            with contextlib.suppress(RuntimeError):
                await engine.deep_prefetch()

            downloader = PdfDownloader(self.settings)
            browser_fallback = Crawl4AIBrowserDownloadFallback(self.settings)
            try:
                while True:
                    page_rows = state.claim_pages(self.settings.page_batch_size)
                    if not page_rows:
                        break
                    try:
                        await engine.crawl_rows(page_rows)
                    except RuntimeError as exc:
                        for row in page_rows:
                            state.mark_page(str(row["url"]), success=False, error=str(exc))
                        break
                    await self._drain_pdf_batch(state, library, downloader, browser_fallback)

                while state.count("pdfs", "pending"):
                    await self._drain_pdf_batch(state, library, downloader, browser_fallback)
            finally:
                await downloader.aclose()

            self._populate_summary(state)
            downloaded = state.all_downloaded()
            manifest = library.write_manifests(
                downloaded,
                state.all_failures(),
                asdict(self.summary),
            )
            self.summary.manifest_path = str(manifest)
            return self.summary

    def _populate_summary(self, state: CrawlState) -> None:
        self.summary.finished_at = datetime.now(UTC).isoformat()
        self.summary.pages_seen = state.count("pages")
        self.summary.pages_crawled = state.count("pages", "done")
        self.summary.pages_failed = state.count("pages", "failed")
        self.summary.pdf_urls_seen = state.count("pdfs")
        self.summary.pdf_downloaded = state.count("pdfs", "downloaded")
        self.summary.pdf_failed = state.count("pdfs", "failed")
        downloaded = state.all_downloaded()
        occurrences = Counter(str(row["sha256"]) for row in downloaded if row.get("sha256"))
        self.summary.pdf_unchanged_or_duplicate = sum(
            max(0, count - 1) for count in occurrences.values()
        )
        self.summary.bytes_downloaded = sum(int(row.get("size_bytes") or 0) for row in downloaded)

    async def _drain_pdf_batch(
        self,
        state: CrawlState,
        library: NdisLibrary,
        downloader: PdfDownloader,
        browser_fallback: Crawl4AIBrowserDownloadFallback,
    ) -> None:
        rows = state.claim_pdfs(self.settings.download_batch_size)
        if not rows:
            return
        semaphore = asyncio.Semaphore(self.settings.concurrency)

        async def acquire(row: Any) -> None:
            url = str(row["url"])
            if not is_allowed_file_url(
                url,
                self.settings.base_url,
                self.settings.extra_allowed_file_hosts,
            ):
                state.mark_pdf_failed(
                    url,
                    error="PDF host is not in the allowed source host set",
                )
                return
            async with semaphore:
                result = await downloader.download(
                    url,
                    state.existing_conditional_headers(url),
                )
                if result.http_status == 304:
                    if not state.mark_pdf_not_modified(url):
                        state.mark_pdf_failed(
                            url,
                            error="server returned 304 without a stored object",
                            http_status=304,
                        )
                    return
                if not result.success and self.settings.browser_download_fallback:
                    result = await browser_fallback.download(url)
                if not result.success:
                    state.mark_pdf_failed(
                        url,
                        error=result.error or "download failed",
                        http_status=result.http_status,
                    )
                    return
                try:
                    stored = library.finalize(
                        result,
                        source_page=row["source_page"],
                        anchor_text=row["anchor_text"],
                        discovery_method=str(row["discovery_method"]),
                    )
                except (OSError, ValueError) as exc:
                    state.mark_pdf_failed(
                        url,
                        error=f"library finalization failed: {exc}",
                    )
                    return
                state.mark_pdf_stored(stored)

        await asyncio.gather(*(acquire(row) for row in rows))
