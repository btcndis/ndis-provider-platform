from __future__ import annotations

import asyncio
import hashlib
import os
import re
import tempfile
from pathlib import Path
from urllib.parse import unquote

import httpx

from .config import CrawlSettings
from .models import DownloadResult
from .urls import safe_filename

_RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}
_FILENAME_RE = re.compile(r"filename\*?=(?:UTF-8''|\")?([^\";]+)", re.I)


class PdfDownloader:
    def __init__(self, settings: CrawlSettings) -> None:
        self.settings = settings
        timeout = httpx.Timeout(settings.page_timeout_ms / 1000, connect=30.0)
        self.client = httpx.AsyncClient(
            headers={
                "User-Agent": settings.user_agent,
                "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.5",
                "Accept-Language": "en-AU,en;q=0.9",
            },
            timeout=timeout,
            follow_redirects=True,
            http2=True,
            proxy=settings.proxy_url,
        )

    async def aclose(self) -> None:
        await self.client.aclose()

    async def download(
        self, url: str, conditional_headers: dict[str, str] | None = None
    ) -> DownloadResult:
        last_error: str | None = None
        for attempt in range(1, self.settings.max_retries + 1):
            result = await self._attempt(url, conditional_headers or {})
            if result.success or result.http_status == 304:
                return result
            last_error = result.error
            if result.http_status not in _RETRYABLE_STATUS and result.http_status is not None:
                return result
            await asyncio.sleep(min(self.settings.retry_backoff_base**attempt, 30))
        return DownloadResult(success=False, url=url, error=last_error or "max retries exceeded")

    async def _attempt(self, url: str, conditional_headers: dict[str, str]) -> DownloadResult:
        fd, tmp_name = tempfile.mkstemp(
            prefix="ndis_pdf_", suffix=".part", dir=self.settings.library_root / "tmp"
        )
        os.close(fd)
        tmp_path = Path(tmp_name)
        try:
            async with self.client.stream("GET", url, headers=conditional_headers) as response:
                status = response.status_code
                chain = [str(item.url) for item in response.history]
                final_url = str(response.url)
                content_type = (
                    response.headers.get("content-type", "").split(";", 1)[0].strip() or None
                )
                disposition = response.headers.get("content-disposition")
                if status == 304:
                    tmp_path.unlink(missing_ok=True)
                    return DownloadResult(
                        success=False,
                        url=url,
                        final_url=final_url,
                        http_status=304,
                        content_type=content_type,
                        content_disposition=disposition,
                        redirect_chain=chain,
                        acquisition_strategy="httpx_conditional",
                        error="not modified",
                    )
                if status >= 400:
                    tmp_path.unlink(missing_ok=True)
                    return DownloadResult(
                        success=False,
                        url=url,
                        final_url=final_url,
                        http_status=status,
                        content_type=content_type,
                        redirect_chain=chain,
                        error=f"HTTP {status}",
                    )

                hasher = hashlib.sha256()
                size = 0
                head = bytearray()
                with tmp_path.open("wb") as handle:
                    async for chunk in response.aiter_bytes(64 * 1024):
                        if not chunk:
                            continue
                        size += len(chunk)
                        if size > self.settings.max_download_bytes:
                            raise ValueError(
                                f"PDF exceeds {self.settings.max_download_bytes} bytes"
                            )
                        if len(head) < 2048:
                            head.extend(chunk[: 2048 - len(head)])
                        hasher.update(chunk)
                        handle.write(chunk)

                if b"%PDF-" not in bytes(head[:1024]):
                    preview = bytes(head[:300]).decode("utf-8", errors="replace").lower()
                    if "<html" in preview or "<!doctype" in preview:
                        reason = "server returned HTML/challenge page instead of a PDF"
                    else:
                        reason = f"invalid PDF magic; declared content-type={content_type!r}"
                    tmp_path.unlink(missing_ok=True)
                    return DownloadResult(
                        success=False,
                        url=url,
                        final_url=final_url,
                        http_status=status,
                        content_type=content_type,
                        content_disposition=disposition,
                        redirect_chain=chain,
                        error=reason,
                    )

                filename = self._filename(disposition, final_url)
                return DownloadResult(
                    success=True,
                    url=url,
                    final_url=final_url,
                    http_status=status,
                    content_type=content_type,
                    content_disposition=disposition,
                    etag=response.headers.get("etag"),
                    last_modified=response.headers.get("last-modified"),
                    sha256=hasher.hexdigest(),
                    size_bytes=size,
                    filename=filename,
                    temp_path=tmp_path,
                    redirect_chain=chain,
                    acquisition_strategy="httpx_stream",
                )
        except (httpx.HTTPError, OSError, ValueError) as exc:
            tmp_path.unlink(missing_ok=True)
            return DownloadResult(success=False, url=url, error=f"{type(exc).__name__}: {exc}")

    @staticmethod
    def _filename(disposition: str | None, final_url: str) -> str:
        if disposition:
            match = _FILENAME_RE.search(disposition)
            if match:
                return safe_filename(unquote(match.group(1).strip()))
        return safe_filename(final_url)


class Crawl4AIBrowserDownloadFallback:
    """Uses Crawl4AI's browser download support only after direct streaming fails."""

    def __init__(self, settings: CrawlSettings) -> None:
        self.settings = settings

    async def download(self, url: str) -> DownloadResult:
        try:
            from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig
        except ImportError as exc:
            return DownloadResult(
                success=False,
                url=url,
                acquisition_strategy="crawl4ai_browser_download",
                error=f"Crawl4AI unavailable: {exc}",
            )

        downloads_dir = self.settings.downloads_dir
        assert downloads_dir is not None
        before = {path.resolve() for path in downloads_dir.glob("*") if path.is_file()}
        browser_config = BrowserConfig(
            headless=self.settings.headless,
            accept_downloads=True,
            downloads_path=str(downloads_dir),
            user_agent=self.settings.user_agent,
            headers={"Accept-Language": "en-AU,en;q=0.9"},
            ignore_https_errors=False,
            light_mode=False,
            text_mode=False,
        )
        run_config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            page_timeout=self.settings.page_timeout_ms,
            wait_until="networkidle",
            delay_before_return_html=3.0,
            capture_network_requests=True,
            capture_console_messages=True,
            check_robots_txt=self.settings.respect_robots,
            js_code="""
                (() => {
                  const selectors = [
                    'a[href$=".pdf" i]', 'a[download]',
                    'a[href*="/download" i]', 'button[data-download]',
                    '[role="button"][data-download]'
                  ];
                  for (const selector of selectors) {
                    const el = document.querySelector(selector);
                    if (el) { el.click(); return true; }
                  }
                  return false;
                })();
            """,
            wait_for=8,
            verbose=True,
        )
        try:
            async with AsyncWebCrawler(config=browser_config) as crawler:
                result = await crawler.arun(url=url, config=run_config)
        except Exception as exc:
            return DownloadResult(
                success=False,
                url=url,
                acquisition_strategy="crawl4ai_browser_download",
                error=f"browser fallback failed: {exc}",
            )

        candidates: list[Path] = []
        for raw in getattr(result, "downloaded_files", None) or []:
            path = Path(raw)
            if path.exists() and path.is_file():
                candidates.append(path)
        after = [
            path
            for path in downloads_dir.glob("*")
            if path.is_file() and path.resolve() not in before
        ]
        candidates.extend(after)
        for path in sorted(set(candidates), key=lambda p: p.stat().st_mtime, reverse=True):
            try:
                head = path.read_bytes()[:1024]
            except OSError:
                continue
            if b"%PDF-" not in head:
                continue
            hasher = hashlib.sha256()
            with path.open("rb") as handle:
                while chunk := handle.read(64 * 1024):
                    hasher.update(chunk)
            return DownloadResult(
                success=True,
                url=url,
                final_url=str(getattr(result, "url", url) or url),
                http_status=getattr(result, "status_code", 200),
                content_type="application/pdf",
                sha256=hasher.hexdigest(),
                size_bytes=path.stat().st_size,
                filename=path.name,
                temp_path=path,
                acquisition_strategy="crawl4ai_browser_download",
            )
        return DownloadResult(
            success=False,
            url=url,
            final_url=str(getattr(result, "url", url) or url),
            http_status=getattr(result, "status_code", None),
            acquisition_strategy="crawl4ai_browser_download",
            error=getattr(result, "error_message", None)
            or "browser produced no valid PDF download",
        )
