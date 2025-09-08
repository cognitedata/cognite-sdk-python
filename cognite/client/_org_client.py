from __future__ import annotations

from abc import ABC
from functools import cached_property
from urllib.parse import urljoin

from cognite.client._api_client import APIClient
from cognite.client.exceptions import CogniteAPIError


class OrgAPIClient(APIClient, ABC):
    _AUTH_URL = "https://auth.cognite.com"

    @cached_property
    def _base_url_with_base_path(self) -> str:
        # The OrganizationAPI uses the auth_url as the base for these endpoints instead of the
        # base_url like the rest of the SDK.
        assert self._api_version, "API version must be set for organization endpoints"
        return urljoin(self._AUTH_URL, f"/api/{self._api_version}/orgs/{self._organization}")

    @cached_property
    def _organization(self) -> str:
        # This is an internal endpoint, not part of the public API - and we call it manually to avoid
        # the overridden _base_url_with_base_path property:
        from cognite.client.utils._concurrency import ConcurrencySettings

        headers = self._configure_headers(additional_headers=None, api_subversion=self._api_subversion)
        full_url = urljoin(self._config.base_url, f"/api/{self._api_version}/projects/{self._config.project}")
        executor = ConcurrencySettings._get_event_loop_executor()
        response = executor.run_coro(self._http_client_with_retry("GET", url=full_url, headers=headers))
        try:
            return response.raise_for_status().json()["organization"]
        except Exception:
            raise CogniteAPIError(
                "Could not look-up organization", response.status_code, response.headers.get("x-request-id")
            )
