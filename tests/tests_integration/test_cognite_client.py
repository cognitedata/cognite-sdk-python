from __future__ import annotations

import pickle

import pytest
from _pytest.monkeypatch import MonkeyPatch

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.credentials import OAuthClientCertificate, Token
from cognite.client.exceptions import CogniteAPIError, CogniteProjectAccessError


@pytest.fixture
def cognite_client_with_wrong_base_url(
    cognite_client: CogniteClient, async_client: AsyncCogniteClient, monkeypatch: MonkeyPatch
) -> CogniteClient:
    monkeypatch.setattr(async_client.config, "base_url", "https://cognitedata.com")
    return cognite_client


class TestCogniteClient:
    def test_wrong_project(
        self, monkeypatch: MonkeyPatch, async_client: AsyncCogniteClient, cognite_client: CogniteClient
    ) -> None:
        monkeypatch.setattr(async_client.config, "project", "that-looks-wrong")
        to_match = (
            "^You don't have access to the requested CDF project='that-looks-wrong'. "
            "Did you intend to use one of: ['python-sdk-test']? | code: 401 |"
        )
        with pytest.raises(CogniteProjectAccessError, match=to_match):
            cognite_client.assets.list()

    def test_wrong_project_and_wrong_cluster(
        self, monkeypatch: MonkeyPatch, cognite_client: CogniteClient, async_client: AsyncCogniteClient
    ) -> None:
        monkeypatch.setattr(async_client.config, "project", "that-looks-wrong")
        monkeypatch.setattr(async_client.config, "base_url", "https://aws-dub-dev.cognitedata.com")
        to_match = "^You don't have access to the requested CDF project='that-looks-wrong' | code: 401 |"

        with pytest.raises(CogniteProjectAccessError, match=to_match):
            cognite_client.assets.list()

    def test_wrong_base_url_resulting_in_301(self, cognite_client_with_wrong_base_url: CogniteClient) -> None:
        with pytest.raises(CogniteAPIError) as e:
            cognite_client_with_wrong_base_url.assets.list(limit=1)
        assert e.value.code == 301

    def test_post(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(CogniteAPIError) as e:
            cognite_client.post("/login", json={})
        assert e.value.code == 404

    def test_put(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(CogniteAPIError) as e:
            cognite_client.put("/login")
        assert e.value.code == 404


@pytest.mark.skip(reason="TODO(haakonvt): Fix after httpx upgrade adventure")
def test_cognite_client_is_picklable(cognite_client: CogniteClient) -> None:
    if isinstance(cognite_client.config.credentials, (Token, OAuthClientCertificate)):
        pytest.skip()
    roundtrip_client = pickle.loads(pickle.dumps(cognite_client))
    assert roundtrip_client.iam.token.inspect().projects
