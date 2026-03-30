from __future__ import annotations

import pytest

from cognite.client import AsyncCogniteClient
from cognite.client.utils._url import interpolate_and_url_encode, resolve_url


def test_url_encode() -> None:
    assert "/bla/yes%2Fno/bla" == interpolate_and_url_encode("/bla/{}/bla", "yes/no")
    assert "/bla/123/bla/456" == interpolate_and_url_encode("/bla/{}/bla/{}", "123", "456")


class TestResolveUrl:
    @pytest.mark.parametrize(
        "use_api_version, method, url_path, expected_retryable, expected_url",
        [
            # Path with no api_version (base_url only)
            (False, "GET", "/assets/list", True, "https://api.cognitedata.com/assets/list"),
            # Path with api_version (includes /api/v1/projects/{project})
            (True, "GET", "/list", True, "https://api.cognitedata.com/api/v1/projects/dummy/list"),
            # Full https URL (used as-is)
            (
                False,
                "GET",
                "https://auth.cognite.com/api/v1/principals/me",
                True,
                "https://auth.cognite.com/api/v1/principals/me",
            ),
            # Full http URL (used as-is)
            (
                False,
                "GET",
                "http://localhost:8000/api/v1/projects/test/assets",
                True,
                "http://localhost:8000/api/v1/projects/test/assets",
            ),
            # Non-retryable: POST to create endpoint
            (True, "POST", "/assets", False, "https://api.cognitedata.com/api/v1/projects/dummy/assets"),
        ],
    )
    def test_resolve_url(
        self,
        async_client: AsyncCogniteClient,
        use_api_version: bool,
        method: str,
        url_path: str,
        expected_url: str,
        expected_retryable: bool,
    ) -> None:
        api_client = async_client.assets if use_api_version else async_client._api_client
        is_retryable, full_url = resolve_url(api_client, method, url_path)
        assert full_url == expected_url
        assert is_retryable is expected_retryable

    def test_resolve_url_invalid_path_raises(self, async_client: AsyncCogniteClient) -> None:
        with pytest.raises(ValueError, match="URL path must start with '/' or be a full URL"):
            resolve_url(async_client._api_client, "GET", "assets/list")
