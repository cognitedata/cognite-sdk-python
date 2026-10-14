from __future__ import annotations

from abc import ABC
from functools import cached_property

from cognite.client._api_client import APIClient
from cognite.client.exceptions import CogniteOrganizationError
from cognite.client.utils._async_helpers import run_sync
from cognite.client.utils._auxiliary import append_url_path


class OrgAPIClient(APIClient, ABC):
    # CogIdP base URL depends on the organization prefix; anything not listed uses the default:
    _DEFAULT_AUTH_URL = "https://auth.cognite.com"
    _AUTH_URLS_BY_ORG_PREFIX = (
        ("cog-dev-", "https://auth-dev.cognitedata-development.cognite.ai"),
        ("cog-staging-", "https://auth-dev.gc-bru-001.cogheim.net"),
    )

    @cached_property
    def _auth_url(self) -> str:
        organization = self._organization
        for prefix, url in self._AUTH_URLS_BY_ORG_PREFIX:
            if organization.startswith(prefix):
                return url
        return self._DEFAULT_AUTH_URL

    # TODO: Code smell: _base_url_with_base_path calling _organization calling run_sync in an async-first SDK...
    @property
    def _base_url_with_base_path(self) -> str:
        # The OrganizationAPI uses the auth_url as the base for these endpoints instead of the
        # base_url like the rest of the SDK.
        assert self._api_version, "API version must be set for organization endpoints"
        base_path = f"/api/{self._api_version}/orgs/{self._organization}"
        return append_url_path(self._auth_url, base_path)

    @cached_property
    def _organization(self) -> str:
        # This is an internal endpoint, not part of the public API - and we call it manually to avoid
        # the overridden _base_url_with_base_path property:
        base_path = f"/api/{self._api_version}/projects/{self._config.project}"
        full_url = append_url_path(self._config.base_url, base_path)
        try:
            response = run_sync(self._request("GET", full_url=full_url, include_cdf_headers=True, semaphore=None))
            return response.json()["organization"]
        except Exception as err:
            raise CogniteOrganizationError from err
