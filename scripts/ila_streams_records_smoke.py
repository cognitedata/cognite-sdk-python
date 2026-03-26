#!/usr/bin/env python3
"""Verify ILA Streams + Records against a live CDF project (manual / local only).

Configure credentials the same way as integration tests (see ``CONTRIBUTING.md``): ``.env`` with
``LOGIN_FLOW``, ``COGNITE_PROJECT``, ``COGNITE_BASE_URL``, ``COGNITE_CLIENT_NAME``, and the auth
fields for your chosen flow.

**Always lists streams** (read). Optional steps use env vars:

* ``ILA_STREAM_EXTERNAL_ID`` — target stream for record calls
* ``ILA_RECORD_ITEM_JSON`` — single JSON object for one ingest row (``space``, ``externalId``, ``sources``, …)

Examples::

    poetry run python scripts/ila_streams_records_smoke.py
    ILA_STREAM_EXTERNAL_ID=my-stream ILA_RECORD_ITEM_JSON='{"space":"...","externalId":"x","sources":[]}' \\
        poetry run python scripts/ila_streams_records_smoke.py --ingest-one

This script is not run in CI.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
from pathlib import Path

from dotenv import load_dotenv

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCertificate, OAuthClientCredentials, OAuthInteractive

REPO_ROOT = Path(__file__).resolve().parents[1]


def _make_client() -> CogniteClient:
    login_flow = os.environ["LOGIN_FLOW"].lower()
    if login_flow == "client_credentials":
        credentials = OAuthClientCredentials(
            token_url=os.environ["COGNITE_TOKEN_URL"],
            client_id=os.environ["COGNITE_CLIENT_ID"],
            client_secret=os.environ["COGNITE_CLIENT_SECRET"],
            scopes=os.environ["COGNITE_TOKEN_SCOPES"].split(","),
        )
    elif login_flow == "interactive":
        credentials = OAuthInteractive(
            authority_url=os.environ["COGNITE_AUTHORITY_URL"],
            client_id=os.environ["COGNITE_CLIENT_ID"],
            scopes=os.environ.get("COGNITE_TOKEN_SCOPES", "").split(","),
            redirect_port=random.randint(53000, 60000),
        )
    elif login_flow == "client_certificate":
        credentials = OAuthClientCertificate(
            authority_url=os.environ["COGNITE_AUTHORITY_URL"],
            client_id=os.environ["COGNITE_CLIENT_ID"],
            cert_thumbprint=os.environ["COGNITE_CERT_THUMBPRINT"],
            certificate=Path(os.environ["COGNITE_CERTIFICATE"]).read_text(),
            scopes=os.environ.get("COGNITE_TOKEN_SCOPES", "").split(","),
        )
    else:
        raise SystemExit("LOGIN_FLOW must be client_credentials, interactive, or client_certificate")

    return CogniteClient(
        ClientConfig(
            client_name=os.environ["COGNITE_CLIENT_NAME"],
            project=os.environ["COGNITE_PROJECT"],
            base_url=os.environ["COGNITE_BASE_URL"],
            credentials=credentials,
        )
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ingest-one",
        action="store_true",
        help="Call ingest_items with ILA_RECORD_ITEM_JSON on ILA_STREAM_EXTERNAL_ID",
    )
    args = parser.parse_args()

    load_dotenv(REPO_ROOT / ".env")
    load_dotenv()

    client = _make_client()

    streams = client.streams.list()
    print(f"Found {len(streams)} stream(s).")
    for s in streams:
        print(f"  - {s.external_id!r} ({getattr(s, 'type', '')})")

    if args.ingest_one:
        stream_id = os.environ.get("ILA_STREAM_EXTERNAL_ID")
        raw = os.environ.get("ILA_RECORD_ITEM_JSON")
        if not stream_id or not raw:
            print(
                "Set ILA_STREAM_EXTERNAL_ID and ILA_RECORD_ITEM_JSON for --ingest-one.",
                file=sys.stderr,
            )
            return 2
        item = json.loads(raw)
        result = client.streams.records.ingest_items(stream_id, [item])
        print("ingest response:", result.dump(camel_case=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
