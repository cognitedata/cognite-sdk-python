from __future__ import annotations

from abc import ABC
from functools import cached_property
from urllib.parse import urljoin

from cognite.client._api_client import APIClient


class OrgAPI(APIClient, ABC):
    def _get_base_url_with_base_path(self) -> str:
        base_path = ""
        if self._api_version:
            base_path = f"/api/{self._api_version}/orgs/{self._organization}"
        return urljoin(self._config.auth_url, base_path)

    @cached_property
    def _organization(self) -> str:
        headers = self._configure_headers(
            "application/json",
            additional_headers=self._config.headers.copy(),
            api_subversion=self._api_subversion,
        )
        full_url = urljoin(self._config.base_url, f"/api/v1/projects/{self._config.project}")
        response = self._http_client.request(method="GET", url=full_url, headers=headers)
        response.raise_for_status()
        try:
            return response.json()["organization"]
        except KeyError as e:
            raise RuntimeError(f"Could not find 'organization' in response: {response.text}") from e
