#!/usr/bin/env python3
"""E2E check for ILA ``client.streams`` (list, create, retrieve, delete) on SDK v8.

Uses OIDC client credentials from the environment. Run::

    poetry run python scripts/ila_streams_pr_e2e.py
"""

from __future__ import annotations

import os
import re
import sys
import uuid
from pathlib import Path

from dotenv import load_dotenv

from cognite.client import CogniteClient
from cognite.client.config import global_config
from cognite.client.data_classes.streams import StreamDeleteItem, StreamWrite

global_config.disable_pypi_version_check = True

REPO_ROOT = Path(__file__).resolve().parents[1]


def _cdf_cluster() -> str:
    if v := os.environ.get("COGNITE_CDF_CLUSTER"):
        return v
    base = os.environ.get("COGNITE_BASE_URL", "")
    m = re.match(r"https?://([^.]+)\.cognitedata\.com/?", base.strip())
    if m:
        return m.group(1)
    return "api"


def main() -> int:
    load_dotenv(REPO_ROOT / ".env")
    load_dotenv()

    tenant = os.environ.get("COGNITE_TENANT_ID") or os.environ.get("AZURE_TENANT_ID")
    missing = [k for k in ("COGNITE_PROJECT", "COGNITE_CLIENT_ID", "COGNITE_CLIENT_SECRET") if not os.environ.get(k)]
    if not tenant:
        missing.append("COGNITE_TENANT_ID or AZURE_TENANT_ID")
    if missing:
        print("Missing env:", ", ".join(missing), file=sys.stderr)
        return 2

    client = CogniteClient.default_oauth_client_credentials(
        project=os.environ["COGNITE_PROJECT"],
        cdf_cluster=_cdf_cluster(),
        tenant_id=tenant,
        client_id=os.environ["COGNITE_CLIENT_ID"],
        client_secret=os.environ["COGNITE_CLIENT_SECRET"],
        client_name=os.environ.get("COGNITE_CLIENT_NAME", "pr-ila-streams-e2e"),
    )

    ext = f"sdk_e2e_{uuid.uuid4().hex[:16]}"

    print("streams.list()")
    n0 = len(client.streams.list())
    print(f"  [OK] {n0} stream(s)")

    print("streams.create(StreamWrite template ImmutableTestStream)")
    client.streams.create([StreamWrite(ext, {"template": {"name": "ImmutableTestStream"}})])
    print(f"  [OK] created {ext!r}")

    print("streams.retrieve()")
    got = client.streams.retrieve(ext)
    print(f"  [OK] external_id={got.external_id!r} type={got.type!r}")

    print("streams.delete(StreamDeleteItem)")
    client.streams.delete([StreamDeleteItem(ext)])
    print("  [OK] delete request accepted (soft-delete)")

    print("streams.list() after delete")
    n1 = len(client.streams.list())
    print(f"  [OK] {n1} stream(s)")

    print("-- ILA Streams E2E: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
