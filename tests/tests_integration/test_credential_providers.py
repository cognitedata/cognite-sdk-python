from __future__ import annotations

import pickle

import pytest

from cognite.client import CogniteClient
from cognite.client.credentials import (
    CredentialProvider,
    OAuthClientCredentials,
    OAuthDeviceCode,
    OAuthInteractive,
)

# These tests need to be part of the integration tests as 'msal' sends an HTTP request to verify
# the authority URI; and will -raise- if not given a valid one:
AUTHORITY_URL = "https://login.microsoftonline.com/dff7763f-e2f5-4ffd-9b8a-4ba4bafba5ea"


class TestCredentialProvidersArePicklable:
    @pytest.mark.parametrize(
        "auth_cls, args",
        (
            # client_id is used to pick a cache location, so we vary it:
            (OAuthClientCredentials, ["token_url", "client_id0", "client_secret", "scopes"]),
            (OAuthDeviceCode, [AUTHORITY_URL, "client_id1", "scopes"]),
            (OAuthInteractive, [AUTHORITY_URL, "client_id2", "scopes"]),
        ),
    )
    def test_serialize_and_deserialize(
        self, auth_cls: type[CredentialProvider], args: list[str], cognite_client: CogniteClient
    ) -> None:
        cred_prov = auth_cls(*args)
        roundtrip_cred_prov = pickle.loads(pickle.dumps(cred_prov))
        assert type(roundtrip_cred_prov) is auth_cls
