from __future__ import annotations

import html as html_lib
import json
import re
from collections.abc import Iterable
from pathlib import PurePosixPath
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup

from .models import PdfCandidate

_PDF_PATH_RE = re.compile(r"(?:\.pdf)(?:$|[?#])", re.I)
_PDF_URL_RE = re.compile(
    r"https?:(?:\\?/\\?/|//)[^\s\"'<>]+?(?:\.pdf(?:\?[^\s\"'<>]*)?|/download(?:\?[^\s\"'<>]*)?)",
    re.I,
)
_DOWNLOADISH_RE = re.compile(r"/(?:download|file|media|document)(?:/|\?|$)", re.I)
_TRACKING_KEYS = {"fbclid", "gclid", "mc_cid", "mc_eid", "ref", "source"}
_SKIP_SCHEMES = ("mailto:", "tel:", "javascript:", "data:", "blob:", "about:")


def canonicalize_url(url: str, base_url: str | None = None) -> str:
    value = html_lib.unescape(url.strip()).replace("\\/", "/")
    if base_url:
        value = urljoin(base_url, value)
    parts = urlsplit(value)
    if parts.scheme.lower() not in {"http", "https"}:
        return ""
    host = (parts.hostname or "").lower().rstrip(".")
    if not host:
        return ""
    port = parts.port
    netloc = host
    if port and not (
        (parts.scheme.lower() == "https" and port == 443)
        or (parts.scheme.lower() == "http" and port == 80)
    ):
        netloc = f"{host}:{port}"
    path = quote(parts.path or "/", safe="/%:@!$&'()*+,;=-._~")
    if path != "/":
        path = re.sub(r"/{2,}", "/", path)
    query_items = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        if key.lower().startswith("utm_") or key.lower() in _TRACKING_KEYS:
            continue
        query_items.append((key, value))
    query = urlencode(query_items, doseq=True)
    return urlunsplit((parts.scheme.lower(), netloc, path, query, ""))


def host_for(url: str) -> str:
    return (urlsplit(url).hostname or "").lower()


def is_same_site(url: str, base_url: str) -> bool:
    host = host_for(url)
    base_host = host_for(base_url)
    return (
        host == base_host
        or host.endswith("." + base_host.removeprefix("www."))
        or base_host.endswith("." + host.removeprefix("www."))
    )


def is_allowed_file_url(url: str, base_url: str, extra_hosts: Iterable[str] = ()) -> bool:
    host = host_for(url)
    if not host:
        return False
    if is_same_site(url, base_url):
        return True
    allowed = {item.lower().strip().rstrip(".") for item in extra_hosts if item.strip()}
    return any(host == item or host.endswith("." + item) for item in allowed)


def is_allowed_page(url: str, base_url: str) -> bool:
    if not url or not is_same_site(url, base_url):
        return False
    path = urlsplit(url).path.lower()
    if looks_like_pdf_url(url):
        return False
    blocked = (
        "/user/login",
        "/admin",
        "/search?",
        "/print/",
        "/cdn-cgi/",
        "/api/",
    )
    return not any(part in url.lower() for part in blocked) and not path.endswith(
        (
            ".jpg",
            ".jpeg",
            ".png",
            ".gif",
            ".svg",
            ".webp",
            ".css",
            ".js",
            ".woff",
            ".woff2",
            ".ico",
            ".mp4",
            ".mp3",
        )
    )


def looks_like_pdf_url(url: str) -> bool:
    if _PDF_PATH_RE.search(url):
        return True
    path = urlsplit(url).path.lower()
    return bool(
        _DOWNLOADISH_RE.search(path) and ("pdf" in url.lower() or path.endswith("/download"))
    )


def safe_filename(value: str, fallback: str = "document.pdf") -> str:
    name = PurePosixPath(urlsplit(value).path).name or fallback
    name = re.sub(r"[^A-Za-z0-9._()\- ]+", "_", name).strip(" ._")
    if not name:
        name = fallback
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    return name[:220]


def _candidate(
    url: str, base_url: str, source_page: str, text: str | None, method: str, depth: int
) -> PdfCandidate | None:
    normalized = canonicalize_url(url, base_url)
    if not normalized:
        return None
    if looks_like_pdf_url(normalized):
        return PdfCandidate(
            normalized, source_page, (text or "").strip()[:1000] or None, method, depth
        )
    return None


def extract_from_html(
    html: str, page_url: str, depth: int = 0
) -> tuple[list[str], list[PdfCandidate]]:
    soup = BeautifulSoup(html or "", "lxml")
    pages: dict[str, None] = {}
    pdfs: dict[str, PdfCandidate] = {}

    attr_pairs = (
        ("a", "href"),
        ("area", "href"),
        ("iframe", "src"),
        ("embed", "src"),
        ("object", "data"),
        ("source", "src"),
        ("link", "href"),
    )
    for tag_name, attr in attr_pairs:
        for tag in soup.find_all(tag_name):
            raw = tag.get(attr)
            if not isinstance(raw, str) or not raw or raw.lower().startswith(_SKIP_SCHEMES):
                continue
            raw_text = tag.get_text(" ", strip=True) or tag.get("title") or tag.get("aria-label")
            text = str(raw_text) if raw_text else None
            pdf = _candidate(raw, page_url, page_url, text, f"dom_{tag_name}_{attr}", depth)
            if pdf:
                pdfs.setdefault(pdf.url, pdf)
                continue
            normalized = canonicalize_url(raw, page_url)
            if normalized:
                pages.setdefault(normalized, None)

    for meta in soup.find_all("meta"):
        if str(meta.get("http-equiv", "")).lower() == "refresh":
            content = str(meta.get("content", ""))
            match = re.search(r"url\s*=\s*(.+)$", content, re.I)
            if match:
                raw = match.group(1).strip(" '\"")
                pdf = _candidate(raw, page_url, page_url, None, "meta_refresh", depth)
                if pdf:
                    pdfs.setdefault(pdf.url, pdf)
                else:
                    normalized = canonicalize_url(raw, page_url)
                    if normalized:
                        pages.setdefault(normalized, None)

    raw_text = html_lib.unescape(html or "").replace("\\/", "/")
    for match in _PDF_URL_RE.finditer(raw_text):
        raw = match.group(0).rstrip("),.;]}")
        pdf = _candidate(raw, page_url, page_url, None, "embedded_script_url", depth)
        if pdf:
            pdfs.setdefault(pdf.url, pdf)

    for script in soup.find_all("script", attrs={"type": re.compile("ld\\+json", re.I)}):
        try:
            payload = json.loads(script.string or script.get_text() or "null")
        except (json.JSONDecodeError, TypeError):
            continue
        for raw in _walk_json_strings(payload):
            pdf = _candidate(raw, page_url, page_url, None, "json_ld", depth)
            if pdf:
                pdfs.setdefault(pdf.url, pdf)

    return list(pages), list(pdfs.values())


def _walk_json_strings(value: object) -> Iterable[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for child in value.values():
            yield from _walk_json_strings(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_json_strings(child)


def extract_from_crawl4ai_links(
    links: object, page_url: str, depth: int = 0
) -> tuple[list[str], list[PdfCandidate]]:
    pages: dict[str, None] = {}
    pdfs: dict[str, PdfCandidate] = {}
    if not isinstance(links, dict):
        return [], []
    for group in ("internal", "external"):
        items = links.get(group) or []
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            raw = item.get("href") or item.get("url")
            if not isinstance(raw, str):
                continue
            text = item.get("text") or item.get("title")
            pdf = _candidate(
                raw,
                page_url,
                page_url,
                str(text) if text else None,
                f"crawl4ai_{group}_link",
                depth,
            )
            if pdf:
                pdfs.setdefault(pdf.url, pdf)
            else:
                normalized = canonicalize_url(raw, page_url)
                if normalized:
                    pages.setdefault(normalized, None)
    return list(pages), list(pdfs.values())


def extract_from_network_events(
    events: object, page_url: str, depth: int = 0
) -> list[PdfCandidate]:
    found: dict[str, PdfCandidate] = {}
    if not isinstance(events, list):
        return []
    for event in events:
        if not isinstance(event, dict):
            continue
        raw = event.get("url") or event.get("request_url")
        if not isinstance(raw, str):
            continue
        headers = event.get("headers") or event.get("response_headers") or {}
        content_type = ""
        if isinstance(headers, dict):
            content_type = str(headers.get("content-type") or headers.get("Content-Type") or "")
        if "application/pdf" in content_type.lower() or looks_like_pdf_url(raw):
            normalized = canonicalize_url(raw, page_url)
            if normalized:
                found.setdefault(
                    normalized, PdfCandidate(normalized, page_url, None, "browser_network", depth)
                )
    return list(found.values())
