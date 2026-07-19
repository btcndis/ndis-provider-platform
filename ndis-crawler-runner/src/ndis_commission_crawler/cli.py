from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict
from pathlib import Path

from .config import CrawlSettings
from .orchestrator import NdisCommissionAcquirer
from .state import CrawlState


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ndis-commission-crawl",
        description="Crawl the public NDIS Commission website with Crawl4AI and acquire PDFs.",
    )
    sub = parser.add_subparsers(dest="command")

    crawl = sub.add_parser("crawl", help="discover pages and download PDFs")
    crawl.add_argument("--base-url", default="https://www.ndiscommission.gov.au/")
    crawl.add_argument("--library-root", type=Path, required=True)
    crawl.add_argument("--state-db", type=Path)
    crawl.add_argument("--max-pages", type=int, default=0)
    crawl.add_argument("--max-depth", type=int, default=20)
    crawl.add_argument("--concurrency", type=int, default=4)
    crawl.add_argument("--page-batch-size", type=int, default=24)
    crawl.add_argument("--download-batch-size", type=int, default=8)
    crawl.add_argument("--max-download-mb", type=int, default=100)
    crawl.add_argument("--proxy")
    crawl.add_argument("--allow-file-host", action="append", default=[])
    crawl.add_argument("--headed", action="store_true")
    crawl.add_argument("--no-browser-download-fallback", action="store_true")
    crawl.add_argument("--ignore-robots", action="store_true")
    crawl.add_argument("--no-debug-capture", action="store_true")

    status = sub.add_parser("status", help="show durable queue and acquisition state")
    status.add_argument("--state-db", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "crawl":
        settings = CrawlSettings(
            base_url=args.base_url,
            library_root=args.library_root,
            state_db=args.state_db,
            max_pages=max(0, args.max_pages),
            max_depth=max(0, args.max_depth),
            concurrency=max(1, args.concurrency),
            page_batch_size=max(1, args.page_batch_size),
            download_batch_size=max(1, args.download_batch_size),
            max_download_bytes=max(1, args.max_download_mb) * 1024 * 1024,
            proxy_url=args.proxy,
            extra_allowed_file_hosts=args.allow_file_host,
            headless=not args.headed,
            browser_download_fallback=not args.no_browser_download_fallback,
            respect_robots=not args.ignore_robots,
            capture_debug=not args.no_debug_capture,
        )
        summary = asyncio.run(NdisCommissionAcquirer(settings).run())
        print(json.dumps(asdict(summary), indent=2))
        return 0 if summary.pdf_failed == 0 else 1

    if args.command == "status":
        with CrawlState(args.state_db) as state:
            payload = {
                "pages": {
                    "total": state.count("pages"),
                    "pending": state.count("pages", "pending"),
                    "done": state.count("pages", "done"),
                    "failed": state.count("pages", "failed"),
                },
                "pdfs": {
                    "total": state.count("pdfs"),
                    "pending": state.count("pdfs", "pending"),
                    "downloaded": state.count("pdfs", "downloaded"),
                    "failed": state.count("pdfs", "failed"),
                },
            }
        print(json.dumps(payload, indent=2))
        return 0

    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
