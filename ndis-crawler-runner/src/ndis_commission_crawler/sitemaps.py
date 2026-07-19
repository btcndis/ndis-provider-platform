from __future__ import annotations

import asyncio
import gzip
import re
from dataclasses import dataclass, field
from urllib.parse import urljoin

import httpx
from defusedxml import ElementTree

from .models import PdfCandidate
from .state import CrawlState
from .urls import canonicalize_url, is_allowed_page, looks_like_pdf_url


@dataclass(slots=True)
class SeedOutcome:
    page_urls: list[str] = field(default_factory=list)
    pdf_urls: list[PdfCandidate] = field(default_factory=list)
    sitemap_urls: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


class SitemapSeeder:
    def __init__(
        self, *, base_url: str, user_agent: str, max_retries: int, state: CrawlState
    ) -> None:
        self.base_url = base_url
        self.user_agent = user_agent
        self.max_retries = max_retries
        self.state = state

    async def seed(self) -> SeedOutcome:
        outcome = SeedOutcome(page_urls=[canonicalize_url(self.base_url)])
        timeout = httpx.Timeout(40.0, connect=20.0)
        async with httpx.AsyncClient(
            headers={
                "User-Agent": self.user_agent,
                "Accept": "text/html,application/xml,text/xml,*/*",
            },
            timeout=timeout,
            follow_redirects=True,
            http2=True,
        ) as client:
            robots_url = urljoin(self.base_url, "/robots.txt")
            robots_text = ""
            try:
                response = await self._get(client, robots_url)
                robots_text = response.text if response.status_code == 200 else ""
                self.state.record_endpoint(robots_url, "robots", f"http_{response.status_code}")
            except httpx.HTTPError as exc:
                self.state.record_endpoint(robots_url, "robots", "unreachable", str(exc))
                outcome.errors.append(f"robots: {exc}")

            sitemap_urls = [
                match.group(1).strip()
                for match in re.finditer(r"^\s*Sitemap:\s*(\S+)", robots_text, re.I | re.M)
            ]
            for path in ("/sitemap.xml", "/sitemap_index.xml", "/sitemap-index.xml"):
                candidate = urljoin(self.base_url, path)
                if candidate not in sitemap_urls:
                    sitemap_urls.append(candidate)

            seen_sitemaps: set[str] = set()
            seen_urls: set[str] = set(outcome.page_urls)
            queue = list(sitemap_urls)
            while queue:
                sitemap_url = canonicalize_url(queue.pop(0))
                if not sitemap_url or sitemap_url in seen_sitemaps:
                    continue
                seen_sitemaps.add(sitemap_url)
                try:
                    response = await self._get(client, sitemap_url)
                    if response.status_code != 200:
                        self.state.record_endpoint(
                            sitemap_url, "sitemap", f"http_{response.status_code}"
                        )
                        continue
                    body = response.content
                    if sitemap_url.endswith(".gz") or response.headers.get(
                        "content-type", ""
                    ).lower().startswith("application/gzip"):
                        body = gzip.decompress(body)
                    root = ElementTree.fromstring(body)
                    self.state.record_endpoint(sitemap_url, "sitemap", "ok")
                    outcome.sitemap_urls.append(sitemap_url)
                except (httpx.HTTPError, ElementTree.ParseError, OSError) as exc:
                    self.state.record_endpoint(sitemap_url, "sitemap", "error", str(exc))
                    outcome.errors.append(f"{sitemap_url}: {exc}")
                    continue

                root_name = root.tag.rsplit("}", 1)[-1].lower()
                locations = [
                    (node.text or "").strip()
                    for node in root.iter()
                    if node.tag.rsplit("}", 1)[-1].lower() == "loc" and (node.text or "").strip()
                ]
                if root_name == "sitemapindex":
                    for loc in locations:
                        normalized = canonicalize_url(loc, sitemap_url)
                        if normalized and normalized not in seen_sitemaps:
                            queue.append(normalized)
                else:
                    for loc in locations:
                        normalized = canonicalize_url(loc, sitemap_url)
                        if not normalized or normalized in seen_urls:
                            continue
                        seen_urls.add(normalized)
                        if looks_like_pdf_url(normalized):
                            outcome.pdf_urls.append(
                                PdfCandidate(normalized, sitemap_url, None, "sitemap", 0)
                            )
                        elif is_allowed_page(normalized, self.base_url):
                            outcome.page_urls.append(normalized)
        return outcome

    async def _get(self, client: httpx.AsyncClient, url: str) -> httpx.Response:
        last_exc: httpx.HTTPError | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = await client.get(url)
                if response.status_code not in {429, 500, 502, 503, 504}:
                    return response
            except httpx.HTTPError as exc:
                last_exc = exc
            await asyncio.sleep(min(2**attempt, 20))
        if last_exc:
            raise last_exc
        return response
