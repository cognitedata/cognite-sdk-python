from __future__ import annotations

from abc import ABC
from functools import cached_property
from urllib.parse import urljoin

from cognite.client._api_client import APIClient
from cognite.client.exceptions import CogniteAPIError


class OrgAPIClient(APIClient, ABC):
    _auth_url = "https://auth.cognite.com"

    def _get_base_url_with_base_path(self) -> str:
        """Get base URL with base path including organization and api version if applicable"""
        base_path = ""
        if self._api_version:
            base_path = f"/api/{self._api_version}/orgs/{self._organization}"
        # The OrganizationAPi uses the auth_url as the base for these endpoints instead of the
        # base_url like the rest of the SDK.
        return urljoin(self._auth_url, base_path)

    @cached_property
    def _organization(self) -> str:
        headers = self._configure_headers(
            "application/json",
            additional_headers=self._config.headers.copy(),
            api_subversion=self._api_subversion,
        )
        # This is an internal endpoint, not part of the public API
        full_url = urljoin(self._config.base_url, f"/api/v1/projects/{self._config.project}")
        response = self._http_client_with_retry.request(method="GET", url=full_url, headers=headers)
        if response.status_code != 200:
            raise CogniteAPIError(
                "Could not look-up organization", response.status_code, response.headers.get("x-request-id")
            )
        return response.json()["organization"]
