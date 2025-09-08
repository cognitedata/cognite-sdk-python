from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, Literal, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    CoordinateReferenceSystem,
    CoordinateReferenceSystemList,
    CoordinateReferenceSystemWrite,
    Feature,
    FeatureList,
    FeatureType,
    FeatureTypeList,
    FeatureTypeWrite,
    FeatureWrite,
)
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr


class AsyncGeospatialAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/geospatial"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.crs = AsyncCoordinateReferenceSystemsAPI(self._config, self._api_version, self._cognite_client)
        self.feature_types = AsyncFeatureTypesAPI(self._config, self._api_version, self._cognite_client)

    async def compute(self, output: dict[str, Any], **kwargs) -> dict[str, Any]:
        """Compute geospatial operations."""
        body = {"output": output, **kwargs}
        res = await self._post(url_path=f"{self._RESOURCE_PATH}/compute", json=body)
        return res.json()


class AsyncCoordinateReferenceSystemsAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/geospatial/crs"

    async def list(self, filter_epsg: int | None = None) -> CoordinateReferenceSystemList:
        """List coordinate reference systems."""
        params = {}
        if filter_epsg:
            params["filterEpsg"] = filter_epsg
        return await self._list(
            list_cls=CoordinateReferenceSystemList,
            resource_cls=CoordinateReferenceSystem,
            method="GET",
            other_params=params,
        )

    async def retrieve_multiple(self, srid: Sequence[int]) -> CoordinateReferenceSystemList:
        """Retrieve CRS by SRID."""
        res = await self._post(
            url_path=f"{self._RESOURCE_PATH}/byids",
            json={"items": [{"srid": s} for s in srid]}
        )
        return CoordinateReferenceSystemList._load(res.json()["items"], cognite_client=self._cognite_client)

    async def create(self, crs: CoordinateReferenceSystemWrite | Sequence[CoordinateReferenceSystemWrite]) -> CoordinateReferenceSystem | CoordinateReferenceSystemList:
        """Create coordinate reference systems."""
        return await self._create_multiple(
            list_cls=CoordinateReferenceSystemList,
            resource_cls=CoordinateReferenceSystem,
            items=crs,
        )


class AsyncFeatureTypesAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/geospatial/featuretypes"

    async def list(self) -> FeatureTypeList:
        """List feature types."""
        return await self._list(
            list_cls=FeatureTypeList,
            resource_cls=FeatureType,
            method="GET",
        )

    async def retrieve(self, external_id: str) -> FeatureType | None:
        """Retrieve feature type by external ID."""
        try:
            res = await self._get(url_path=f"{self._RESOURCE_PATH}/{external_id}")
            return FeatureType._load(res.json(), cognite_client=self._cognite_client)
        except Exception:
            return None

    async def create(self, feature_type: FeatureType | FeatureTypeWrite | Sequence[FeatureType] | Sequence[FeatureTypeWrite]) -> FeatureType | FeatureTypeList:
        """Create feature types."""
        return await self._create_multiple(
            list_cls=FeatureTypeList,
            resource_cls=FeatureType,
            items=feature_type,
        )

    async def delete(self, external_id: str | Sequence[str]) -> None:
        """Delete feature types."""
        external_ids = [external_id] if isinstance(external_id, str) else external_id
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_ids),
            wrap_ids=True,
        )
