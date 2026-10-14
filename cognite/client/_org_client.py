from __future__ import annotations

from abc import ABC

from cognite.client._api_client import APIClient
from cognite.client.exceptions import CogniteOrganizationError
from cognite.client.utils._auxiliary import append_url_path


class OrgAPIClient(APIClient, ABC):
    # CogIdP base URL depends on the organization prefix; anything not listed uses the default:
    _DEFAULT_AUTH_URL = "https://auth.cognite.com"
    _AUTH_URLS_BY_ORG_PREFIX = (
        ("cog-dev-", "https://auth-dev.cognitedata-development.cognite.ai"),
        ("cog-staging-", "https://auth-dev.gc-bru-001.cogheim.net"),
    )
    _resolved_organization: str | None = None

    async def _get_organization(self) -> str:
        # DEV NOTE: This must be awaited (by every public method) before any URL is built. We used to look this
        # up lazily with run_sync from a sync property, but that deadlocks when called from the SDK's own event
        # loop thread, i.e. from any sync client call.
        if self._resolved_organization is None:
            # This is an internal endpoint, not part of the public API - and we call it manually to avoid
            # the overridden _base_url_with_base_path property:
            base_path = f"/api/{self._api_version}/projects/{self._config.project}"
            full_url = append_url_path(self._config.base_url, base_path)
            try:
                response = await self._request("GET", full_url=full_url, include_cdf_headers=True, semaphore=None)
                self._resolved_organization = response.json()["organization"]
            except Exception as err:
                raise CogniteOrganizationError from err

        return self._resolved_organization

    @property
    def _organization(self) -> str:
        if self._resolved_organization is None:
            raise RuntimeError(
                "SDK developer error: organization accessed before being resolved. Every public method on an "
                "OrgAPIClient subclass must 'await self._get_organization()' before building URLs (it can't be "
                "fetched lazily here, as this is sync code and run_sync would deadlock on the SDK's event loop)"
            )
        return self._resolved_organization

    @property
    def _auth_url(self) -> str:
        for prefix, url in self._AUTH_URLS_BY_ORG_PREFIX:
            if self._organization.startswith(prefix):
                return url
        return self._DEFAULT_AUTH_URL

    @property
    def _base_url_with_base_path(self) -> str:
        # The OrganizationAPI uses the auth_url as the base for these endpoints instead of the
        # base_url like the rest of the SDK.
        assert self._api_version, "API version must be set for organization endpoints"
        base_path = f"/api/{self._api_version}/orgs/{self._organization}"
        return append_url_path(self._auth_url, base_path)
