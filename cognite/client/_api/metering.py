from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.metering import MeteringData, MeteringDataList
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import MeterId
from cognite.client.utils._time import timestamp_to_ms
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig
    from cognite.client.data_classes.filters import Prefix


class MeteringAPI(APIClient):
    _RESOURCE_PATH = "/metering/meters"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="alpha",
            sdk_maturity="alpha",
            feature_name="Metering API",
        )

    def _time_range_params(
        self,
        start: int | str | datetime.datetime | None,
        end: int | str | datetime.datetime | None,
        number_of_datapoints: int | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if start is not None:
            params["start"] = timestamp_to_ms(start)
        if end is not None:
            params["end"] = timestamp_to_ms(end)
        if number_of_datapoints is not None:
            params["numberOfDatapoints"] = number_of_datapoints
        return params

    @overload
    async def retrieve(
        self,
        id: str,
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        number_of_datapoints: int | None = None,
    ) -> MeteringData | None: ...

    @overload
    async def retrieve(
        self,
        id: SequenceNotStr[str],
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        number_of_datapoints: int | None = None,
    ) -> MeteringDataList: ...

    async def retrieve(
        self,
        id: str | SequenceNotStr[str],
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        number_of_datapoints: int | None = None,
    ) -> MeteringData | MeteringDataList | None:
        """`Retrieve one or more meters by id <https://api-docs.cognite.com/20230101-alpha/tag/Metering/operation/fetchConsumptionDataById/>`_.

        Retrieves consumption data by ``meterId``. Pass a single string to get one meter,
        or a list of strings to get multiple meters at once.

        Args:
            id (str | SequenceNotStr[str]): Single meter ID or list of meter IDs. Metering is identified by an id containing the service name and a service-scoped metering name. For instance ``atlas.monthly_ai_tokens`` is the id of the ``atlas`` service metering ``monthly_ai_tokens``. Service and metering names are always in ``lower_snake_case``.
            start (int | str | datetime.datetime | None): Start of the time range for historical data. Accepts milliseconds since epoch, a ``datetime`` object, or a relative time string like ``"2w-ago"``. **Must be provided together with** ``number_of_datapoints`` to get time-series data. If omitted, only meter metadata is returned without time-series data.
            end (int | str | datetime.datetime | None): End of the time range for historical data. Accepts milliseconds since epoch, a ``datetime`` object, or a relative time string like ``"now"``. Only relevant when ``start`` is provided. Defaults to the current time on the server if omitted.
            number_of_datapoints (int | None): Number of equal-width time buckets to return between ``start`` and ``end``. **Must be provided together with** ``start`` to get time-series data. Valid range: 0-600. API server default is 0 (metadata only).

        Returns:
            MeteringData | MeteringDataList | None: If a single ID is given: the requested meter, or ``None`` if not found. If a list of IDs is given: the requested meters in the same order.

        Examples:

            Retrieve a single meter by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.metering.retrieve(id="atlas.monthly_ai_tokens")

            Retrieve multiple meters by id:

                >>> res = client.metering.retrieve(id=["atlas.monthly_ai_tokens", "files.storage_bytes"])

            Retrieve a single meter with historical data:

                >>> res = client.metering.retrieve(
                ...     id="atlas.monthly_ai_tokens",
                ...     start=1764547200000,
                ...     end=1767225599000,
                ...     number_of_datapoints=30,
                ... )
        """
        self._warning.warn()
        if isinstance(id, str):
            params = self._time_range_params(start, end, number_of_datapoints) or None
            return await self._retrieve(
                identifier=MeterId(id),
                cls=MeteringData,
                headers=self._alpha_version_header(),
                params=params,
            )
        body: dict[str, Any] = {"items": [MeterId(id_).as_dict() for id_ in id]}
        body.update(self._time_range_params(start, end, number_of_datapoints))
        res = await self._post(
            url_path=self._RESOURCE_PATH + "/byids",
            json=body,
            headers=self._alpha_version_header(),
            semaphore=self._get_semaphore("read"),
        )
        return MeteringDataList._load(res.json()["items"])._maybe_set_client_ref(self._cognite_client)

    async def list(
        self,
        filter: Prefix | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        number_of_datapoints: int | None = None,
    ) -> MeteringDataList:
        """`List all meters <https://api-docs.cognite.com/20230101-alpha/tag/Metering/operation/listConsumptionData/>`_.

        Lists all available meters for a specific project. Optionally filter by meter ID prefix using a ``Prefix`` filter.

        Args:
            filter (Prefix | None): Optional ``Prefix`` filter to apply on the ``meterId`` property (only ``Prefix`` filters are supported).
            limit (int | None): Maximum number of meters to return. Defaults to 25. Set to ``None`` or ``-1`` to return all meters.
            start (int | str | datetime.datetime | None): Start of the time range for historical data. Accepts milliseconds since epoch, a ``datetime`` object, or a relative time string like ``"2w-ago"``. **Must be provided together with** ``number_of_datapoints`` to get time-series data. If omitted, only meter metadata is returned without time-series data.
            end (int | str | datetime.datetime | None): End of the time range for historical data. Accepts milliseconds since epoch, a ``datetime`` object, or a relative time string like ``"now"``. Only relevant when ``start`` is provided. Defaults to the current time on the server if omitted.
            number_of_datapoints (int | None): Number of equal-width time buckets to return between ``start`` and ``end``. **Must be provided together with** ``start`` to get time-series data. Valid range: 0-600. API server default is 0 (metadata only).

        Returns:
            MeteringDataList: List of all meters in the project.

        Examples:

            List all meters:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> meters = client.metering.list(limit=None)

            List meters filtered by prefix (e.g., all meters for the 'atlas' service):

                >>> from cognite.client.data_classes.filters import Prefix
                >>> prefix_filter = Prefix("meterId", "atlas.")
                >>> meters = client.metering.list(filter=prefix_filter)

            List meters with historical data:

                >>> meters = client.metering.list(
                ...     start=1764547200000,
                ...     end=1767225599000,
                ...     number_of_datapoints=30,
                ... )
        """
        self._warning.warn()
        other_params = self._time_range_params(start, end, number_of_datapoints) or None

        return await self._list(
            method="GET" if filter is None else "POST",
            list_cls=MeteringDataList,
            resource_cls=MeteringData,
            limit=limit,
            filter=None if filter is None else filter.dump(camel_case_property=True),
            other_params=other_params,
            headers=self._alpha_version_header(),
        )
