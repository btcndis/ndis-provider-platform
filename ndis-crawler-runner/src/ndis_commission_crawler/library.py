from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .models import DownloadResult, StoredPdf
from .urls import safe_filename


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


class NdisLibrary:
    """Writes immutable bytes using Veritaxon's existing object-store layout."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.objects_dir = root / "objects"
        self.manifests_dir = root / "manifests"
        self.exports_dir = root / "exports" / "ndis-commission-pdfs"
        self.tmp_dir = root / "tmp"
        for path in (self.objects_dir, self.manifests_dir, self.exports_dir, self.tmp_dir):
            path.mkdir(parents=True, exist_ok=True)

    def path_for_sha256(self, sha256: str) -> Path:
        return self.objects_dir / "sha256" / sha256[:2] / sha256[2:4] / sha256

    def storage_key(self, sha256: str) -> str:
        return f"sha256/{sha256[:2]}/{sha256[2:4]}/{sha256}"

    def finalize(
        self,
        result: DownloadResult,
        *,
        source_page: str | None,
        anchor_text: str | None,
        discovery_method: str,
    ) -> StoredPdf:
        if (
            not result.success
            or result.temp_path is None
            or result.sha256 is None
            or result.final_url is None
        ):
            raise ValueError("cannot finalize an unsuccessful download")
        temp_path = result.temp_path
        if not temp_path.exists():
            raise FileNotFoundError(temp_path)
        actual_sha = _sha256_file(temp_path)
        if actual_sha != result.sha256:
            raise ValueError("sha256 changed before finalization")
        destination = self.path_for_sha256(result.sha256)
        destination.parent.mkdir(parents=True, exist_ok=True)
        already_existed = destination.exists()
        if already_existed:
            temp_path.unlink(missing_ok=True)
        else:
            os.replace(temp_path, destination)
            destination.chmod(0o444)

        filename = safe_filename(result.filename or result.final_url)
        friendly = self._unique_friendly_path(filename, result.sha256)
        if not friendly.exists():
            try:
                os.link(destination, friendly)
            except OSError:
                shutil.copy2(destination, friendly)
                friendly.chmod(0o444)

        return StoredPdf(
            url=result.url,
            final_url=result.final_url,
            sha256=result.sha256,
            size_bytes=result.size_bytes,
            storage_key=self.storage_key(result.sha256),
            object_path=str(destination),
            filename=friendly.name,
            source_page=source_page,
            anchor_text=anchor_text,
            discovery_method=discovery_method,
            acquisition_strategy=result.acquisition_strategy,
            content_type=result.content_type,
            content_disposition=result.content_disposition,
            etag=result.etag,
            last_modified=result.last_modified,
            redirect_chain=result.redirect_chain,
            already_existed=already_existed,
            acquired_at=datetime.now(UTC).isoformat(),
        )

    def import_browser_download(
        self,
        path: Path,
        *,
        url: str,
        final_url: str,
        source_page: str | None,
        anchor_text: str | None,
        discovery_method: str,
    ) -> StoredPdf:
        with path.open("rb") as handle:
            data_head = handle.read(2048)
        if b"%PDF-" not in data_head:
            raise ValueError(f"browser download is not a PDF: {path}")
        sha256 = _sha256_file(path)
        staged = self.tmp_dir / f"browser_{sha256}.part"
        if path.resolve() != staged.resolve():
            shutil.copy2(path, staged)
        result = DownloadResult(
            success=True,
            url=url,
            final_url=final_url,
            content_type="application/pdf",
            sha256=sha256,
            size_bytes=staged.stat().st_size,
            filename=path.name,
            temp_path=staged,
            acquisition_strategy="crawl4ai_browser_download",
        )
        return self.finalize(
            result,
            source_page=source_page,
            anchor_text=anchor_text,
            discovery_method=discovery_method,
        )

    def _unique_friendly_path(self, filename: str, sha256: str) -> Path:
        target = self.exports_dir / filename
        if not target.exists():
            return target
        try:
            existing_sha = _sha256_file(target)
        except OSError:
            existing_sha = ""
        if existing_sha == sha256:
            return target
        stem = target.stem[:180]
        return target.with_name(f"{stem}--{sha256[:12]}.pdf")

    def write_manifests(
        self,
        downloaded: list[dict[str, Any]],
        failures: list[dict[str, Any]],
        summary: dict[str, Any],
    ) -> Path:
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        manifest_path = self.manifests_dir / f"ndis-commission-pdfs-{stamp}.json"
        final_summary = dict(summary)
        final_summary["manifest_path"] = str(manifest_path)
        payload = {
            "schema_version": 1,
            "source_key": "ndiscommission_gov_au",
            "source_url": "https://www.ndiscommission.gov.au/",
            "generated_at": datetime.now(UTC).isoformat(),
            "summary": final_summary,
            "documents": downloaded,
            "failures": failures,
        }
        rendered = json.dumps(payload, indent=2, ensure_ascii=False)
        manifest_path.write_text(rendered, encoding="utf-8")
        latest = self.manifests_dir / "ndis-commission-pdfs-latest.json"
        latest.write_text(rendered, encoding="utf-8")

        jsonl = self.manifests_dir / f"ndis-commission-pdfs-{stamp}.jsonl"
        with jsonl.open("w", encoding="utf-8") as handle:
            for row in downloaded:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")

        csv_path = self.manifests_dir / f"ndis-commission-pdfs-{stamp}.csv"
        fields = [
            "url",
            "final_url",
            "source_page",
            "anchor_text",
            "filename",
            "sha256",
            "size_bytes",
            "storage_key",
            "object_path",
            "discovery_method",
            "acquisition_strategy",
            "content_type",
            "downloaded_at",
        ]
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(downloaded)
        return manifest_path
