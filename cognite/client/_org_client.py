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
        ("cog-staging-", "https://auth-staging.gc-bru-001.cogheim.net"),
    )
    _resolved_organization: str | None = None

    async def _get_organization(self) -> str:
        if self._resolved_organization is not None:
            return self._resolved_organization

        try:
            project = await self._cognite_client.projects._current()
        except Exception as err:
            raise CogniteOrganizationError from err
        if not project.organization:
            # Projects outside an organization (internal Cognite projects only) are loaded with an empty string:
            raise CogniteOrganizationError

        self._resolved_organization = project.organization
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
