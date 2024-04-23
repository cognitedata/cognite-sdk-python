import pickle

import pytest

from cognite.client.credentials import OAuthClientCertificate, Token
from cognite.client.exceptions import CogniteAPIError


@pytest.fixture
def cognite_client_with_wrong_base_url(cognite_client, monkeypatch):
    monkeypatch.setattr(cognite_client.config, "base_url", "https://cognitedata.com")
    yield cognite_client


class TestCogniteClient:
    def test_wrong_base_url_resulting_in_301(self, cognite_client_with_wrong_base_url):
        with pytest.raises(CogniteAPIError):
            cognite_client_with_wrong_base_url.assets.list(limit=1)

    def test_post(self, cognite_client):
        with pytest.raises(CogniteAPIError) as e:
            cognite_client.post("/login", json={})
        assert e.value.code == 404

    def test_put(self, cognite_client):
        with pytest.raises(CogniteAPIError) as e:
            cognite_client.put("/login")
        assert e.value.code == 404

    def test_delete(self, cognite_client):
        with pytest.raises(CogniteAPIError) as e:
            cognite_client.delete("/login")
        assert e.value.code == 404


def find_mappingproxy(obj):
    # Of course this test only fails in CI, so after investigation of where the test pollution
    # comes from, we may delete this entirely:
    for k, v in vars(obj).items():
        try:
            pickle.dumps(v)
        except TypeError as e:
            print(f"failed dumping attribute {k}: {e}")  # noqa T201
            # We have to go deeper:
            find_mappingproxy(v)


def test_cognite_client_is_picklable(cognite_client):
    if isinstance(cognite_client.config.credentials, (Token, OAuthClientCertificate)):
        pytest.skip()
    # TODO: Test pollution makes this flaky
    try:
        roundtrip_client = pickle.loads(pickle.dumps(cognite_client))
    except TypeError:
        find_mappingproxy(cognite_client)
        raise
    assert roundtrip_client.iam.token.inspect().projects
