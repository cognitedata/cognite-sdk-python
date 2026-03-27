#!/usr/bin/env python3
"""Verification script for diagram_download_converted_file.

Run this script to verify the usefulness of the diagram download converted file endpoint.
Requires: CogniteClient configured with valid credentials and a completed diagram convert job.

Usage:
    # Using file_id (from a completed convert job):
    python scripts/verify_diagram_download.py --job-id 123 --file-id 456

    # Using file_external_id:
    python scripts/verify_diagram_download.py --job-id 123 --file-external-id "my-diagram.pdf"

    # Download as SVG instead of PNG:
    python scripts/verify_diagram_download.py --job-id 123 --file-id 456 --format svg

    # Download a specific page:
    python scripts/verify_diagram_download.py --job-id 123 --file-id 456 --page 2
"""
from __future__ import annotations

import argparse
from pathlib import Path

from cognite.client import CogniteClient


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify diagram_download_converted_file endpoint")
    parser.add_argument("--job-id", type=int, required=True, help="Diagram convert job ID")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--file-id", type=int, help="CDF file ID")
    group.add_argument("--file-external-id", type=str, help="CDF file external ID")
    parser.add_argument("--page", type=int, default=1, help="Page number (default: 1)")
    parser.add_argument(
        "--format",
        choices=["png", "svg"],
        default="png",
        help="Output format (default: png)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Save to file (optional). If not set, prints size and first bytes.",
    )
    args = parser.parse_args()

    client = CogniteClient()

    mime_type = "image/png" if args.format == "png" else "image/svg+xml"
    kwargs: dict = {"job_id": args.job_id, "page": args.page, "mime_type": mime_type}
    if args.file_id is not None:
        kwargs["file_id"] = args.file_id
    else:
        kwargs["file_external_id"] = args.file_external_id

    print(f"Downloading converted diagram: job_id={args.job_id}, page={args.page}, format={args.format}")
    content = client.diagrams.download_converted_file(**kwargs)

    print(f"Downloaded {len(content)} bytes")
    if args.output:
        args.output.write_bytes(content)
        print(f"Saved to {args.output}")
    else:
        preview = content[:50] if len(content) >= 50 else content
        print(f"First bytes (hex): {preview.hex()[:80]}...")


if __name__ == "__main__":
    main()
