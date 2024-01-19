from __future__ import annotations

from abc import ABC
from datetime import datetime
from typing import TYPE_CHECKING, Any, List, Literal, Sequence, Union, cast

from typing_extensions import TypeAlias

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteLabelUpdate,
    CogniteListUpdate,
    CogniteObjectUpdate,
    CognitePrimitiveUpdate,
    CogniteResourceList,
    CogniteSort,
    CogniteUpdate,
    EnumProperty,
    ExternalIDTransformerMixin,
    IdTransformerMixin,
    NoCaseConversionPropertyList,
    PropertySpec,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.utils._identifier import Identifier
from cognite.client.utils._time import MAX_TIMESTAMP_MS, MIN_TIMESTAMP_MS
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.data_classes import Asset, Datapoint


class TimeSeriesCore(WriteableCogniteResource["TimeSeriesWrite"], ABC):
    """No description.

    Args:
        external_id (str | None): The externally supplied ID for the time series.
        name (str | None): The display short name of the time series. Note: Value of this field can differ from name presented by older versions of API 0.3-0.6.
        is_string (bool | None): Whether the time series is string valued or not.
        metadata (dict[str, str] | None): Custom, application-specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        unit (str | None): The physical unit of the time series.
        unit_external_id (str | None): The physical unit of the time series (reference to unit catalog). Only available for numeric time series.
        asset_id (int | None): Asset ID of equipment linked to this time series.
        is_step (bool | None): Whether the time series is a step series or not.
        description (str | None): Description of the time series.
        security_categories (Sequence[int] | None): The required security categories to access this time series.
        data_set_id (int | None): The dataSet ID for the item.
        legacy_name (str | None): Set a value for legacyName to allow applications using API v0.3, v04, v05, and v0.6 to access this time series. The legacy name is the human-readable name for the time series and is mapped to the name field used in API versions 0.3-0.6. The legacyName field value must be unique, and setting this value to an already existing value will return an error. We recommend that you set this field to the same value as externalId.
    """

    def __init__(
        self,
        external_id: str | None = None,
        name: str | None = None,
        is_string: bool | None = None,
        metadata: dict[str, str] | None = None,
        unit: str | None = None,
        unit_external_id: str | None = None,
        asset_id: int | None = None,
        is_step: bool | None = None,
        description: str | None = None,
        security_categories: Sequence[int] | None = None,
        data_set_id: int | None = None,
        legacy_name: str | None = None,
    ) -> None:
        self.external_id = external_id
        self.name = name
        self.is_string = is_string
        self.metadata = metadata
        self.unit = unit
        self.unit_external_id = unit_external_id
        self.asset_id = asset_id
        self.is_step = is_step
        self.description = description
        self.security_categories = security_categories
        self.data_set_id = data_set_id
        self.legacy_name = legacy_name


class TimeSeries(TimeSeriesCore):
    """This represents a sequence of data points. The TimeSeries object is the metadata about
    the datapoints, and the Datapoint object is the actual data points. This is the reading version
    of TimesSeries, which is used when retrieving from CDF.

    Args:
        id (int | None): A server-generated ID for the object.
        external_id (str | None): The externally supplied ID for the time series.
        name (str | None): The display short name of the time series. Note: Value of this field can differ from name presented by older versions of API 0.3-0.6.
        is_string (bool | None): Whether the time series is string valued or not.
        metadata (dict[str, str] | None): Custom, application-specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        unit (str | None): The physical unit of the time series.
        unit_external_id (str | None): The physical unit of the time series (reference to unit catalog). Only available for numeric time series.
        asset_id (int | None): Asset ID of equipment linked to this time series.
        is_step (bool | None): Whether the time series is a step series or not.
        description (str | None): Description of the time series.
        security_categories (Sequence[int] | None): The required security categories to access this time series.
        data_set_id (int | None): The dataSet ID for the item.
        created_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        legacy_name (str | None): Set a value for legacyName to allow applications using API v0.3, v04, v05, and v0.6 to access this time series. The legacy name is the human-readable name for the time series and is mapped to the name field used in API versions 0.3-0.6. The legacyName field value must be unique, and setting this value to an already existing value will return an error. We recommend that you set this field to the same value as externalId.
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        id: int | None = None,
        external_id: str | None = None,
        name: str | None = None,
        is_string: bool | None = None,
        metadata: dict[str, str] | None = None,
        unit: str | None = None,
        unit_external_id: str | None = None,
        asset_id: int | None = None,
        is_step: bool | None = None,
        description: str | None = None,
        security_categories: Sequence[int] | None = None,
        data_set_id: int | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        legacy_name: str | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            name=name,
            is_string=is_string,
            metadata=metadata,
            unit=unit,
            unit_external_id=unit_external_id,
            asset_id=asset_id,
            is_step=is_step,
            description=description,
            security_categories=security_categories,
            data_set_id=data_set_id,
            legacy_name=legacy_name,
        )

        self.id = id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cast("CogniteClient", cognite_client)

    def as_write(self) -> TimeSeriesWrite:
        """Returns a TimeSeriesWrite object with the same properties as this TimeSeries."""
        return TimeSeriesWrite(
            external_id=self.external_id,
            name=self.name,
            is_string=self.is_string,
            metadata=self.metadata,
            unit=self.unit,
            unit_external_id=self.unit_external_id,
            asset_id=self.asset_id,
            is_step=self.is_step,
            description=self.description,
            security_categories=self.security_categories,
            data_set_id=self.data_set_id,
            legacy_name=self.legacy_name,
        )

    def count(self) -> int:
        """Returns the number of datapoints in this time series.

        This result may not be completely accurate, as it is based on aggregates which may be occasionally out of date.

        Returns:
            int: The number of datapoints in this time series.

        Raises:
            RuntimeError: If the time series is string, as count aggregate is only supported for numeric data

        Returns:
            int: The total number of datapoints
        """
        if self.is_string:
            raise RuntimeError("String time series does not support count aggregate.")

        identifier = Identifier.load(self.id, self.external_id).as_dict()
        dps = self._cognite_client.time_series.data.retrieve(
            **identifier, start=MIN_TIMESTAMP_MS, end=MAX_TIMESTAMP_MS + 1, aggregates="count", granularity="100d"
        )
        return sum(dps.count)  # type: ignore [union-attr, arg-type]

    def latest(self, before: int | str | datetime | None = None) -> Datapoint | None:
        """Returns the latest datapoint in this time series. If empty, returns None.

        Args:
            before (int | str | datetime | None): No description.
        Returns:
            Datapoint | None: A datapoint object containing the value and timestamp of the latest datapoint.
        """
        identifier = Identifier.load(self.id, self.external_id).as_dict()
        if dps := self._cognite_client.time_series.data.retrieve_latest(**identifier, before=before):
            return dps[0]  # type: ignore [return-value]
        return None

    def first(self) -> Datapoint | None:
        """Returns the first datapoint in this time series. If empty, returns None.

        Returns:
            Datapoint | None: A datapoint object containing the value and timestamp of the first datapoint.
        """
        identifier = Identifier.load(self.id, self.external_id).as_dict()
        dps = self._cognite_client.time_series.data.retrieve(
            **identifier, start=MIN_TIMESTAMP_MS, end=MAX_TIMESTAMP_MS + 1, limit=1
        )
        if dps:
            return dps[0]  # type: ignore [return-value]
        return None

    def asset(self) -> Asset:
        """Returns the asset this time series belongs to.

        Returns:
            Asset: The asset given by its `asset_id`.
        Raises:
            ValueError: If asset_id is missing.
        """
        if self.asset_id is None:
            raise ValueError("asset_id is None")
        return cast("Asset", self._cognite_client.assets.retrieve(id=self.asset_id))


class TimeSeriesWrite(TimeSeriesCore):
    """This is the write version of TimeSeries, which is used when writing to CDF.

    Args:
        external_id (str | None): The externally supplied ID for the time series.
        name (str | None): The display short name of the time series. Note: Value of this field can differ from name presented by older versions of API 0.3-0.6.
        is_string (bool | None): Whether the time series is string valued or not.
        metadata (dict[str, str] | None): Custom, application-specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        unit (str | None): The physical unit of the time series.
        unit_external_id (str | None): The physical unit of the time series (reference to unit catalog). Only available for numeric time series.
        asset_id (int | None): Asset ID of equipment linked to this time series.
        is_step (bool | None): Whether the time series is a step series or not.
        description (str | None): Description of the time series.
        security_categories (Sequence[int] | None): The required security categories to access this time series.
        data_set_id (int | None): The dataSet ID for the item.
        legacy_name (str | None): Set a value for legacyName to allow applications using API v0.3, v04, v05, and v0.6 to access this time series. The legacy name is the human-readable name for the time series and is mapped to the name field used in API versions 0.3-0.6. The legacyName field value must be unique, and setting this value to an already existing value will return an error. We recommend that you set this field to the same value as externalId.
    """

    def as_write(self) -> TimeSeriesWrite:
        """Returns this TimeSeriesWrite object."""
        return self


class TimeSeriesFilter(CogniteFilter):
    """No description.

    Args:
        name (str | None): Filter on name.
        unit (str | None): Filter on unit.
        unit_external_id (str | None): Filter on unit external ID.
        unit_quantity (str | None): Filter on unit quantity.
        is_string (bool | None): Filter on isString.
        is_step (bool | None): Filter on isStep.
        metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        asset_ids (Sequence[int] | None): Only include time series that reference these specific asset IDs.
        asset_external_ids (SequenceNotStr[str] | None): Asset External IDs of related equipment that this time series relates to.
        asset_subtree_ids (Sequence[dict[str, Any]] | None): Only include time series that are related to an asset in a subtree rooted at any of these assetIds (including the roots given). If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
        data_set_ids (Sequence[dict[str, Any]] | None): No description.
        external_id_prefix (str | None): Filter by this (case-sensitive) prefix for the external ID.
        created_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
        last_updated_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
    """

    def __init__(
        self,
        name: str | None = None,
        unit: str | None = None,
        unit_external_id: str | None = None,
        unit_quantity: str | None = None,
        is_string: bool | None = None,
        is_step: bool | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: Sequence[int] | None = None,
        asset_external_ids: SequenceNotStr[str] | None = None,
        asset_subtree_ids: Sequence[dict[str, Any]] | None = None,
        data_set_ids: Sequence[dict[str, Any]] | None = None,
        external_id_prefix: str | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
    ) -> None:
        self.name = name
        self.unit = unit
        self.unit_external_id = unit_external_id
        self.unit_quantity = unit_quantity
        self.is_string = is_string
        self.is_step = is_step
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.asset_external_ids = asset_external_ids
        self.asset_subtree_ids = asset_subtree_ids
        self.data_set_ids = data_set_ids
        self.external_id_prefix = external_id_prefix
        self.created_time = created_time
        self.last_updated_time = last_updated_time


class TimeSeriesUpdate(CogniteUpdate):
    """Changes will be applied to time series.

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
    """

    class _PrimitiveTimeSeriesUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> TimeSeriesUpdate:
            return self._set(value)

    class _ObjectTimeSeriesUpdate(CogniteObjectUpdate):
        def set(self, value: dict) -> TimeSeriesUpdate:
            return self._set(value)

        def add(self, value: dict) -> TimeSeriesUpdate:
            return self._add(value)

        def remove(self, value: list) -> TimeSeriesUpdate:
            return self._remove(value)

    class _ListTimeSeriesUpdate(CogniteListUpdate):
        def set(self, value: list) -> TimeSeriesUpdate:
            return self._set(value)

        def add(self, value: list) -> TimeSeriesUpdate:
            return self._add(value)

        def remove(self, value: list) -> TimeSeriesUpdate:
            return self._remove(value)

    class _LabelTimeSeriesUpdate(CogniteLabelUpdate):
        def add(self, value: list) -> TimeSeriesUpdate:
            return self._add(value)

        def remove(self, value: list) -> TimeSeriesUpdate:
            return self._remove(value)

    @property
    def external_id(self) -> _PrimitiveTimeSeriesUpdate:
        return TimeSeriesUpdate._PrimitiveTimeSeriesUpdate(self, "externalId")

    @property
    def name(self) -> _PrimitiveTimeSeriesUpdate:
        return TimeSeriesUpdate._PrimitiveTimeSeriesUpdate(self, "name")

    @property
    def metadata(self) -> _ObjectTimeSeriesUpdate:
        return TimeSeriesUpdate._ObjectTimeSeriesUpdate(self, "metadata")

    @property
    def unit(self) -> _PrimitiveTimeSeriesUpdate:
        return TimeSeriesUpdate._PrimitiveTimeSeriesUpdate(self, "unit")

    @property
    def unit_external_id(self) -> _PrimitiveTimeSeriesUpdate:
        return TimeSeriesUpdate._PrimitiveTimeSeriesUpdate(self, "unitExternalId")

    @property
    def asset_id(self) -> _PrimitiveTimeSeriesUpdate:
        return TimeSeriesUpdate._PrimitiveTimeSeriesUpdate(self, "assetId")

    @property
    def description(self) -> _PrimitiveTimeSeriesUpdate:
        return TimeSeriesUpdate._PrimitiveTimeSeriesUpdate(self, "description")

    @property
    def is_step(self) -> _PrimitiveTimeSeriesUpdate:
        return TimeSeriesUpdate._PrimitiveTimeSeriesUpdate(self, "isStep")

    @property
    def security_categories(self) -> _ListTimeSeriesUpdate:
        return TimeSeriesUpdate._ListTimeSeriesUpdate(self, "securityCategories")

    @property
    def data_set_id(self) -> _PrimitiveTimeSeriesUpdate:
        return TimeSeriesUpdate._PrimitiveTimeSeriesUpdate(self, "dataSetId")

    @classmethod
    def _get_update_properties(cls) -> list[PropertySpec]:
        return [
            # External ID is nullable, but is used in the upsert logic and thus cannot be nulled out.
            PropertySpec("external_id", is_nullable=False),
            PropertySpec("name"),
            # TimeSeries does not support setting metadata to an empty array.
            PropertySpec("metadata", is_container=True, is_nullable=False),
            PropertySpec("unit"),
            PropertySpec("unit_external_id"),
            PropertySpec("asset_id"),
            PropertySpec("description"),
            PropertySpec("is_step", is_nullable=False),
            PropertySpec("security_categories", is_container=True),
            PropertySpec("data_set_id"),
        ]


class TimeSeriesWriteList(CogniteResourceList[TimeSeriesWrite], ExternalIDTransformerMixin):
    _RESOURCE = TimeSeriesWrite


class TimeSeriesList(WriteableCogniteResourceList[TimeSeriesWrite, TimeSeries], IdTransformerMixin):
    _RESOURCE = TimeSeries

    def as_write(self) -> TimeSeriesWriteList:
        return TimeSeriesWriteList([ts.as_write() for ts in self.data], cognite_client=self._get_cognite_client())


class TimeSeriesProperty(EnumProperty):
    description = "description"
    external_id = "externalId"
    name = "name"
    unit = "unit"
    unit_external_id = "unitExternalId"
    unit_quantity = "unitQuantity"
    asset_id = "assetId"
    asset_root_id = "assetRootId"
    created_time = "createdTime"
    data_set_id = "dataSetId"
    id = "id"
    last_updated_time = "lastUpdatedTime"
    is_step = "isStep"
    is_string = "isString"
    access_categories = "accessCategories"
    security_categories = "securityCategories"
    metadata = "metadata"

    @staticmethod
    def metadata_key(key: str) -> list[str]:
        return NoCaseConversionPropertyList(["metadata", key])


class SortableTimeSeriesProperty(EnumProperty):
    asset_id = "assetId"
    created_time = "createdTime"
    data_set_id = "dataSetId"
    description = "description"
    external_id = "externalId"
    last_updated_time = "lastUpdatedTime"
    name = "name"

    @staticmethod
    def metadata_key(key: str) -> list[str]:
        return NoCaseConversionPropertyList(["metadata", key])


SortableTimeSeriesPropertyLike: TypeAlias = Union[SortableTimeSeriesProperty, str, List[str]]


class TimeSeriesSort(CogniteSort):
    def __init__(
        self,
        property: SortableTimeSeriesProperty,
        order: Literal["asc", "desc"] = "asc",
        nulls: Literal["auto", "first", "last"] = "auto",
    ):
        super().__init__(property, order, nulls)
