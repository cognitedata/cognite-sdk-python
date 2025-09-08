from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, Literal, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ


class AsyncDataModelingAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/models"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Data modeling has many sub-APIs
        self.containers = AsyncContainersAPI(self._config, self._api_version, self._cognite_client)
        self.data_models = AsyncDataModelsAPI(self._config, self._api_version, self._cognite_client)
        self.spaces = AsyncSpacesAPI(self._config, self._api_version, self._cognite_client)
        self.views = AsyncViewsAPI(self._config, self._api_version, self._cognite_client)
        self.instances = AsyncInstancesAPI(self._config, self._api_version, self._cognite_client)
        self.graphql = AsyncDataModelingGraphQLAPI(self._config, self._api_version, self._cognite_client)


class AsyncContainersAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/models/containers"
    
    async def list(
        self,
        space: str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ
    ) -> dict[str, Any]:
        """List containers."""
        filter = {}
        if space:
            filter["space"] = space
        res = await self._post(url_path=f"{self._RESOURCE_PATH}/list", json={"filter": filter, "limit": limit})
        return res.json()

    async def retrieve(self, space: str, external_id: str) -> dict[str, Any] | None:
        """Retrieve container."""
        try:
            res = await self._post(
                url_path=f"{self._RESOURCE_PATH}/byids",
                json={"items": [{"space": space, "externalId": external_id}]}
            )
            items = res.json()["items"]
            return items[0] if items else None
        except Exception:
            return None

    async def create(self, containers: Sequence[dict[str, Any]]) -> dict[str, Any]:
        """Create containers."""
        res = await self._post(url_path=self._RESOURCE_PATH, json={"items": containers})
        return res.json()

    async def delete(self, space: str, external_id: str | Sequence[str]) -> None:
        """Delete containers."""
        external_ids = [external_id] if isinstance(external_id, str) else external_id
        items = [{"space": space, "externalId": ext_id} for ext_id in external_ids]
        await self._post(url_path=f"{self._RESOURCE_PATH}/delete", json={"items": items})


class AsyncDataModelsAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/models/datamodels"
    
    async def list(
        self,
        space: str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ
    ) -> dict[str, Any]:
        """List data models."""
        filter = {}
        if space:
            filter["space"] = space
        res = await self._post(url_path=f"{self._RESOURCE_PATH}/list", json={"filter": filter, "limit": limit})
        return res.json()

    async def retrieve(self, space: str, external_id: str, version: str) -> dict[str, Any] | None:
        """Retrieve data model."""
        try:
            res = await self._post(
                url_path=f"{self._RESOURCE_PATH}/byids",
                json={"items": [{"space": space, "externalId": external_id, "version": version}]}
            )
            items = res.json()["items"]
            return items[0] if items else None
        except Exception:
            return None

    async def create(self, data_models: Sequence[dict[str, Any]]) -> dict[str, Any]:
        """Create data models."""
        res = await self._post(url_path=self._RESOURCE_PATH, json={"items": data_models})
        return res.json()

    async def delete(self, space: str, external_id: str, version: str) -> None:
        """Delete data model."""
        await self._post(
            url_path=f"{self._RESOURCE_PATH}/delete",
            json={"items": [{"space": space, "externalId": external_id, "version": version}]}
        )


class AsyncSpacesAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/models/spaces"
    
    async def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> dict[str, Any]:
        """List spaces."""
        res = await self._post(url_path=f"{self._RESOURCE_PATH}/list", json={"limit": limit})
        return res.json()

    async def retrieve(self, space: str) -> dict[str, Any] | None:
        """Retrieve space."""
        try:
            res = await self._post(
                url_path=f"{self._RESOURCE_PATH}/byids",
                json={"items": [{"space": space}]}
            )
            items = res.json()["items"]
            return items[0] if items else None
        except Exception:
            return None

    async def create(self, spaces: Sequence[dict[str, Any]]) -> dict[str, Any]:
        """Create spaces."""
        res = await self._post(url_path=self._RESOURCE_PATH, json={"items": spaces})
        return res.json()

    async def delete(self, space: str | Sequence[str]) -> None:
        """Delete spaces."""
        spaces = [space] if isinstance(space, str) else space
        items = [{"space": s} for s in spaces]
        await self._post(url_path=f"{self._RESOURCE_PATH}/delete", json={"items": items})


class AsyncViewsAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/models/views"
    
    async def list(
        self,
        space: str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ
    ) -> dict[str, Any]:
        """List views."""
        filter = {}
        if space:
            filter["space"] = space
        res = await self._post(url_path=f"{self._RESOURCE_PATH}/list", json={"filter": filter, "limit": limit})
        return res.json()

    async def retrieve(self, space: str, external_id: str, version: str) -> dict[str, Any] | None:
        """Retrieve view."""
        try:
            res = await self._post(
                url_path=f"{self._RESOURCE_PATH}/byids",
                json={"items": [{"space": space, "externalId": external_id, "version": version}]}
            )
            items = res.json()["items"]
            return items[0] if items else None
        except Exception:
            return None

    async def create(self, views: Sequence[dict[str, Any]]) -> dict[str, Any]:
        """Create views."""
        res = await self._post(url_path=self._RESOURCE_PATH, json={"items": views})
        return res.json()

    async def delete(self, space: str, external_id: str, version: str) -> None:
        """Delete view."""
        await self._post(
            url_path=f"{self._RESOURCE_PATH}/delete",
            json={"items": [{"space": space, "externalId": external_id, "version": version}]}
        )


class AsyncInstancesAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/models/instances"
    
    async def list(
        self,
        instance_type: str | None = None,
        space: str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ
    ) -> dict[str, Any]:
        """List instances."""
        filter = {}
        if instance_type:
            filter["instanceType"] = instance_type
        if space:
            filter["space"] = space
        res = await self._post(url_path=f"{self._RESOURCE_PATH}/list", json={"filter": filter, "limit": limit})
        return res.json()

    async def retrieve(self, space: str, external_id: str) -> dict[str, Any] | None:
        """Retrieve instance."""
        try:
            res = await self._post(
                url_path=f"{self._RESOURCE_PATH}/byids",
                json={"items": [{"space": space, "externalId": external_id}]}
            )
            items = res.json()["items"]
            return items[0] if items else None
        except Exception:
            return None

    async def apply(self, instances: Sequence[dict[str, Any]]) -> dict[str, Any]:
        """Apply instances."""
        res = await self._post(url_path=self._RESOURCE_PATH, json={"items": instances})
        return res.json()

    async def delete(self, space: str, external_id: str | Sequence[str]) -> None:
        """Delete instances."""
        external_ids = [external_id] if isinstance(external_id, str) else external_id
        items = [{"space": space, "externalId": ext_id} for ext_id in external_ids]
        await self._post(url_path=f"{self._RESOURCE_PATH}/delete", json={"items": items})

    async def search(self, view: dict[str, Any], query: str | None = None, limit: int | None = DEFAULT_LIMIT_READ) -> dict[str, Any]:
        """Search instances."""
        body = {"view": view, "limit": limit}
        if query:
            body["query"] = query
        res = await self._post(url_path=f"{self._RESOURCE_PATH}/search", json=body)
        return res.json()


class AsyncDataModelingGraphQLAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/models/graphql"
    
    async def query(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        """Execute GraphQL query."""
        body = {"query": query}
        if variables:
            body["variables"] = variables
        res = await self._post(url_path=self._RESOURCE_PATH, json=body)
        return res.json()