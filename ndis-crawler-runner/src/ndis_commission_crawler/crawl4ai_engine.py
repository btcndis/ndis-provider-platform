from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from .config import CrawlSettings
from .library import NdisLibrary
from .models import PdfCandidate
from .state import CrawlState
from .urls import (
    canonicalize_url,
    extract_from_crawl4ai_links,
    extract_from_html,
    extract_from_network_events,
    is_allowed_page,
    safe_filename,
)

_RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}

_EXPAND_DYNAMIC_CONTENT_JS = r"""
(async () => {
  const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
  const visible = (el) => !!(
    el && (el.offsetWidth || el.offsetHeight || el.getClientRects().length)
  );

  for (const detail of document.querySelectorAll('details:not([open])')) {
    detail.open = true;
  }
  for (const el of document.querySelectorAll('[aria-expanded="false"]')) {
    const tag = el.tagName.toLowerCase();
    const type = (el.getAttribute('type') || '').toLowerCase();
    if (visible(el) && type !== 'submit' && ['button', 'a'].includes(tag)) {
      try { el.click(); } catch (_) {}
    }
  }

  const labels = /^(load|show|view|see)\s+(more|all)|more\s+(results|resources|items)|next$/i;
  const selector = [
    'button',
    'a[role="button"]',
    'a.pager__link--next',
    '[rel="next"]'
  ].join(', ');
  const nextSelector = [
    'a.pager__link--next',
    '[rel="next"]',
    'button.load-more',
    '.load-more button'
  ].join(', ');

  for (let round = 0; round < 12; round++) {
    let clicked = 0;
    const candidates = [...document.querySelectorAll(selector)];
    for (const el of candidates) {
      const text = (
        el.innerText || el.textContent || el.getAttribute('aria-label') || ''
      ).trim();
      const type = (el.getAttribute('type') || '').toLowerCase();
      const href = (el.getAttribute('href') || '').toLowerCase();
      if (!visible(el) || type === 'submit' || href.includes('/user/login')) continue;
      if (labels.test(text) || el.matches(nextSelector)) {
        try {
          el.click();
          clicked++;
          await sleep(700);
        } catch (_) {}
      }
    }
    window.scrollTo(0, document.body.scrollHeight);
    await sleep(900);
    if (!clicked) break;
  }
  window.scrollTo(0, 0);
  return true;
})();
"""


async def _iterate_results(results: Any) -> AsyncIterator[Any]:
    if hasattr(results, "__aiter__"):
        async for result in results:
            yield result
        return
    for result in results:
        yield result


class Crawl4AIEngine:
    def __init__(self, settings: CrawlSettings, state: CrawlState, library: NdisLibrary) -> None:
        self.settings = settings
        self.state = state
        self.library = library

    @staticmethod
    def _imports() -> tuple[Any, Any, Any, Any]:
        try:
            from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig
        except ImportError as exc:
            raise RuntimeError(
                "Crawl4AI is required. Run `uv sync` and `uv run crawl4ai-setup`."
            ) from exc
        return AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig

    def _browser_config(self) -> Any:
        _, BrowserConfig, _, _ = self._imports()
        return BrowserConfig(
            browser_type="chromium",
            headless=self.settings.headless,
            verbose=True,
            accept_downloads=True,
            downloads_path=str(self.settings.downloads_dir),
            user_agent=self.settings.user_agent,
            headers={"Accept-Language": "en-AU,en;q=0.9"},
            ignore_https_errors=False,
            java_script_enabled=True,
            text_mode=False,
            light_mode=False,
            avoid_ads=True,
            use_persistent_context=True,
            user_data_dir=str(self.settings.library_root / "metadata" / "crawl4ai-browser-profile"),
        )

    def _run_config(self, *, stream: bool = True, prefetch: bool = False) -> Any:
        _, _, CacheMode, CrawlerRunConfig = self._imports()
        return CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            stream=stream,
            prefetch=prefetch,
            check_robots_txt=self.settings.respect_robots,
            page_timeout=self.settings.page_timeout_ms,
            wait_until="networkidle",
            delay_before_return_html=1.5,
            word_count_threshold=0,
            excluded_tags=["noscript"],
            scan_full_page=not prefetch,
            scroll_delay=0.35,
            max_scroll_steps=self.settings.max_scroll_steps,
            process_iframes=not prefetch,
            flatten_shadow_dom=not prefetch,
            remove_overlay_elements=True,
            remove_consent_popups=True,
            capture_network_requests=not prefetch,
            capture_console_messages=self.settings.capture_debug and not prefetch,
            exclude_external_links=False,
            exclude_social_media_links=True,
            score_links=True,
            preserve_https_for_internal_links=True,
            js_code=None if prefetch else _EXPAND_DYNAMIC_CONTENT_JS,
            verbose=True,
            mean_delay=self.settings.request_delay_min,
            max_range=max(0.0, self.settings.request_delay_max - self.settings.request_delay_min),
            semaphore_count=self.settings.concurrency,
        )

    def _dispatcher(self) -> Any | None:
        try:
            from crawl4ai import MemoryAdaptiveDispatcher, RateLimiter
        except ImportError:
            try:
                from crawl4ai.async_dispatcher import MemoryAdaptiveDispatcher, RateLimiter
            except ImportError:
                return None
        return MemoryAdaptiveDispatcher(
            memory_threshold_percent=80.0,
            check_interval=0.5,
            max_session_permit=self.settings.concurrency,
            rate_limiter=RateLimiter(
                base_delay=(self.settings.request_delay_min, self.settings.request_delay_max),
                max_delay=30.0,
                max_retries=self.settings.max_retries,
                rate_limit_codes=sorted(_RETRYABLE_STATUS - {408, 425}),
            ),
        )

    async def deep_prefetch(self) -> None:
        try:
            from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
        except ImportError:
            return

        checkpoint_file = self.settings.checkpoint_file
        assert checkpoint_file is not None
        resume_state: dict[str, Any] | None = None
        if checkpoint_file.exists():
            try:
                resume_state = json.loads(checkpoint_file.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                resume_state = None

        async def save_state(state: dict[str, Any]) -> None:
            tmp = checkpoint_file.with_suffix(".part")
            tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
            os.replace(tmp, checkpoint_file)

        max_pages = self.settings.max_pages if self.settings.max_pages > 0 else 10_000
        try:
            strategy = BFSDeepCrawlStrategy(
                max_depth=min(self.settings.max_depth, 8),
                max_pages=max_pages,
                include_external=False,
                resume_state=resume_state,
                on_state_change=save_state,
            )
            config = self._run_config(stream=True, prefetch=True).clone(deep_crawl_strategy=strategy)
            AsyncWebCrawler, _, _, _ = self._imports()
            async with AsyncWebCrawler(config=self._browser_config()) as crawler:
                results = await crawler.arun(self.settings.base_url, config=config)
                async for result in _iterate_results(results):
                    self._process_prefetch_result(result)
        except TypeError:
            return
        except Exception:
            return

    def _process_prefetch_result(self, result: Any) -> None:
        page_url = canonicalize_url(
            str(getattr(result, "url", "") or ""), self.settings.base_url
        )
        if not page_url:
            return
        metadata = getattr(result, "metadata", None)
        depth = int(metadata.get("depth", 0) or 0) if isinstance(metadata, dict) else 0
        pages, pdfs = extract_from_crawl4ai_links(getattr(result, "links", None), page_url, depth)
        self._enqueue_discovered(page_url, depth, pages, pdfs, "crawl4ai_deep_prefetch")

    async def crawl_rows(self, rows: list[Any]) -> None:
        if not rows:
            return
        AsyncWebCrawler, _, _, _ = self._imports()
        urls = [str(row["url"]) for row in rows]
        row_by_url = {canonicalize_url(str(row["url"])): row for row in rows}
        processed: set[str] = set()
        try:
            async with AsyncWebCrawler(config=self._browser_config()) as crawler:
                kwargs: dict[str, Any] = {"urls": urls, "config": self._run_config(stream=True)}
                dispatcher = self._dispatcher()
                if dispatcher is not None:
                    kwargs["dispatcher"] = dispatcher
                results = await crawler.arun_many(**kwargs)
                async for result in _iterate_results(results):
                    requested_url = canonicalize_url(
                        str(getattr(result, "url", "") or ""), self.settings.base_url
                    )
                    row = row_by_url.get(requested_url)
                    if row is None and len(rows) == 1:
                        row = rows[0]
                    if row is None:
                        continue
                    original = str(row["url"])
                    processed.add(original)
                    if not getattr(result, "success", False):
                        self._mark_page_failure(
                            row,
                            error=str(getattr(result, "error_message", "crawl failed")),
                            status=getattr(result, "status_code", None),
                        )
                        self._write_debug(result, original, success=False)
                        continue
                    self._process_page_result(result, row)
                    self.state.mark_page(original, success=True)
                    self._write_debug(result, original, success=True)
        except Exception as exc:
            for row in rows:
                original = str(row["url"])
                if original not in processed:
                    self._mark_page_failure(
                        row, error=f"Crawl4AI batch failure: {exc}", status=None
                    )
            return

        for row in rows:
            original = str(row["url"])
            if original not in processed:
                self._mark_page_failure(row, error="Crawl4AI returned no result", status=None)

    def _mark_page_failure(self, row: Any, *, error: str, status: int | None) -> None:
        attempts_made = int(row["attempts"]) + 1
        retryable = status is None or status in _RETRYABLE_STATUS
        retry = retryable and attempts_made < self.settings.max_retries
        self.state.mark_page(str(row["url"]), success=False, error=error, retry=retry)

    def _process_page_result(self, result: Any, row: Any) -> None:
        original = str(row["url"])
        page_url = (
            canonicalize_url(str(getattr(result, "url", original) or original), original)
            or original
        )
        depth = int(row["depth"])
        html = str(getattr(result, "html", "") or getattr(result, "cleaned_html", "") or "")
        pages_a, pdfs_a = extract_from_html(html, page_url, depth)
        pages_b, pdfs_b = extract_from_crawl4ai_links(
            getattr(result, "links", None), page_url, depth
        )
        network = getattr(result, "network_requests", None) or getattr(
            result, "captured_requests", None
        )
        pdfs_c = extract_from_network_events(network, page_url, depth)
        response_headers = getattr(result, "response_headers", None)
        pdfs_d: list[PdfCandidate] = []
        if isinstance(response_headers, dict):
            content_type = str(
                response_headers.get("content-type") or response_headers.get("Content-Type") or ""
            ).lower()
            if "application/pdf" in content_type:
                pdfs_d.append(
                    PdfCandidate(
                        page_url, original, None, "crawl4ai_response_content_type", depth
                    )
                )

        candidates = self._deduplicate_candidates(pdfs_a + pdfs_b + pdfs_c + pdfs_d)
        self._enqueue_discovered(
            page_url, depth, pages_a + pages_b, candidates, "crawl4ai_render"
        )
        self._import_unambiguous_browser_downloads(
            result, page_url=page_url, depth=depth, candidates=candidates
        )

    @staticmethod
    def _deduplicate_candidates(candidates: list[PdfCandidate]) -> list[PdfCandidate]:
        unique: dict[str, PdfCandidate] = {}
        for candidate in candidates:
            unique.setdefault(candidate.url, candidate)
        return list(unique.values())

    def _import_unambiguous_browser_downloads(
        self,
        result: Any,
        *,
        page_url: str,
        depth: int,
        candidates: list[PdfCandidate],
    ) -> None:
        for raw in getattr(result, "downloaded_files", None) or []:
            path = Path(str(raw))
            if not path.exists() or not path.is_file():
                continue
            matches = [
                candidate
                for candidate in candidates
                if safe_filename(candidate.url).lower() == path.name.lower()
            ]
            if len(matches) == 1:
                candidate = matches[0]
            elif len(candidates) == 1:
                candidate = candidates[0]
            else:
                continue
            try:
                stored = self.library.import_browser_download(
                    path,
                    url=candidate.url,
                    final_url=candidate.url,
                    source_page=page_url,
                    anchor_text=candidate.anchor_text,
                    discovery_method="crawl4ai_automatic_download",
                )
            except (OSError, ValueError):
                continue
            self.state.enqueue_pdf(
                PdfCandidate(
                    candidate.url,
                    page_url,
                    candidate.anchor_text,
                    "crawl4ai_automatic_download",
                    depth,
                )
            )
            self.state.mark_pdf_stored(stored)

    def _enqueue_discovered(
        self,
        page_url: str,
        depth: int,
        pages: list[str],
        pdfs: list[PdfCandidate],
        method: str,
    ) -> None:
        next_depth = depth + 1
        current_pages = self.state.count("pages")
        for url in pages:
            if next_depth > self.settings.max_depth or not is_allowed_page(
                url, self.settings.base_url
            ):
                continue
            if self.settings.max_pages > 0 and current_pages >= self.settings.max_pages:
                break
            inserted = self.state.enqueue_page(
                url, parent_url=page_url, depth=next_depth, method=method
            )
            current_pages += int(inserted)
        self.state.enqueue_pdfs(pdfs)

    def _write_debug(self, result: Any, requested_url: str, *, success: bool) -> None:
        if not self.settings.capture_debug:
            return
        debug_dir = self.settings.library_root / "logs" / "ndis-commission-crawl4ai"
        debug_dir.mkdir(parents=True, exist_ok=True)
        key = canonicalize_url(requested_url).encode("utf-8").hex()[:80]
        payload = {
            "requested_url": requested_url,
            "result_url": str(getattr(result, "url", "") or ""),
            "success": success,
            "status_code": getattr(result, "status_code", None),
            "error_message": getattr(result, "error_message", None),
            "response_headers": getattr(result, "response_headers", None) or {},
            "network_requests": getattr(result, "network_requests", None)
            or getattr(result, "captured_requests", None)
            or [],
            "console_messages": getattr(result, "console_messages", None) or [],
        }
        (debug_dir / f"{key}.json").write_text(
            json.dumps(payload, indent=2, default=str), encoding="utf-8"
        )
