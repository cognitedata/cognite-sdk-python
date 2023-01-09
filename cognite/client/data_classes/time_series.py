from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union, cast

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteLabelUpdate,
    CogniteListUpdate,
    CogniteObjectUpdate,
    CognitePrimitiveUpdate,
    CognitePropertyClassUtil,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
)
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.utils._identifier import Identifier
from cognite.client.utils._time import MAX_TIMESTAMP_MS, MIN_TIMESTAMP_MS

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.data_classes import Asset, Datapoint


class TimeSeries(CogniteResource):
    """No description.

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): The externally supplied ID for the time series.
        name (str): The display short name of the time series. Note: Value of this field can differ from name presented by older versions of API 0.3-0.6.
        is_string (bool): Whether the time series is string valued or not.
        metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        unit (str): The physical unit of the time series.
        asset_id (int): Asset ID of equipment linked to this time series.
        is_step (bool): Whether the time series is a step series or not.
        description (str): Description of the time series.
        security_categories (Sequence[int]): The required security categories to access this time series.
        data_set_id (int): The dataSet Id for the item.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        legacy_name (str): Set a value for legacyName to allow applications using API v0.3, v04, v05, and v0.6 to access this time series. The legacy name is the human-readable name for the time series and is mapped to the name field used in API versions 0.3-0.6. The legacyName field value must be unique, and setting this value to an already existing value will return an error. We recommend that you set this field to the same value as externalId.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        id: int = None,
        external_id: str = None,
        name: str = None,
        is_string: bool = None,
        metadata: Dict[str, str] = None,
        unit: str = None,
        asset_id: int = None,
        is_step: bool = None,
        description: str = None,
        security_categories: Sequence[int] = None,
        data_set_id: int = None,
        created_time: int = None,
        last_updated_time: int = None,
        legacy_name: str = None,
        cognite_client: "CogniteClient" = None,
    ):
        self.id = id
        self.external_id = external_id
        self.name = name
        self.is_string = is_string
        self.metadata = metadata
        self.unit = unit
        self.asset_id = asset_id
        self.is_step = is_step
        self.description = description
        self.security_categories = security_categories
        self.data_set_id = data_set_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.legacy_name = legacy_name
        self._cognite_client = cast("CogniteClient", cognite_client)

    def count(self) -> int:
        """Returns the number of datapoints in this time series.

        This result may not be completely accurate, as it is based on aggregates which may be occasionally out of date.

        Returns:
            int: The number of datapoints in this time series.

        Raises:
            ValueError: If the time series is string as count aggregate is only supported for numeric data

        Returns:
            int: The total number of datapoints
        """
        if self.is_string:
            raise ValueError("String time series does not support count aggregate.")

        identifier = Identifier.load(self.id, self.external_id).as_dict()
        dps = self._cognite_client.time_series.data.retrieve(
            **identifier, start=MIN_TIMESTAMP_MS, end=MAX_TIMESTAMP_MS + 1, aggregates="count", granularity="100d"
        )
        return sum(dps.count)

    def latest(self, before: Union[int, str, datetime] = None) -> Optional["Datapoint"]:
        """Returns the latest datapoint in this time series. If empty, returns None.

        Returns:
            Datapoint: A datapoint object containing the value and timestamp of the latest datapoint.
        """
        identifier = Identifier.load(self.id, self.external_id).as_dict()
        if dps := self._cognite_client.time_series.data.retrieve_latest(**identifier, before=before):
            return dps[0]
        return None

    def first(self) -> Optional["Datapoint"]:
        """Returns the first datapoint in this time series. If empty, returns None.

        Returns:
            Datapoint: A datapoint object containing the value and timestamp of the first datapoint.
        """
        identifier = Identifier.load(self.id, self.external_id).as_dict()
        dps = self._cognite_client.time_series.data.retrieve(
            **identifier, start=MIN_TIMESTAMP_MS, end=MAX_TIMESTAMP_MS + 1, limit=1
        )
        if dps:
            return dps[0]
        return None

    def asset(self) -> "Asset":
        """Returns the asset this time series belongs to.

        Returns:
            Asset: The asset given by its `asset_id`.
        """
        if self.asset_id is None:
            raise ValueError("asset_id is None")
        return self._cognite_client.assets.retrieve(id=self.asset_id)


class TimeSeriesFilter(CogniteFilter):
    """No description.

    Args:
        name (str): Filter on name.
        unit (str): Filter on unit.
        is_string (bool): Filter on isString.
        is_step (bool): Filter on isStep.
        metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        asset_ids (Sequence[int]): Only include time series that reference these specific asset IDs.
        asset_external_ids (Sequence[str]): Asset External IDs of related equipment that this time series relates to.
        asset_subtree_ids (Sequence[Dict[str, Any]]): Only include time series that are related to an asset in a subtree rooted at any of these assetIds (including the roots given). If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
        data_set_ids (Sequence[Dict[str, Any]]): No description.
        external_id_prefix (str): Filter by this (case-sensitive) prefix for the external ID.
        created_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        last_updated_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        name: str = None,
        unit: str = None,
        is_string: bool = None,
        is_step: bool = None,
        metadata: Dict[str, str] = None,
        asset_ids: Sequence[int] = None,
        asset_external_ids: Sequence[str] = None,
        asset_subtree_ids: Sequence[Dict[str, Any]] = None,
        data_set_ids: Sequence[Dict[str, Any]] = None,
        external_id_prefix: str = None,
        created_time: Union[Dict[str, Any], TimestampRange] = None,
        last_updated_time: Union[Dict[str, Any], TimestampRange] = None,
        cognite_client: "CogniteClient" = None,
    ):
        self.name = name
        self.unit = unit
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
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: Union[Dict, str]) -> "TimeSeriesFilter":
        instance = super(TimeSeriesFilter, cls)._load(resource)
        if isinstance(resource, Dict):
            if instance.created_time is not None:
                instance.created_time = TimestampRange(**instance.created_time)
            if instance.last_updated_time is not None:
                instance.last_updated_time = TimestampRange(**instance.last_updated_time)
        return instance


class TimeSeriesUpdate(CogniteUpdate):
    """Changes will be applied to time series.

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
    """

    class _PrimitiveTimeSeriesUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> "TimeSeriesUpdate":
            return self._set(value)

    class _ObjectTimeSeriesUpdate(CogniteObjectUpdate):
        def set(self, value: Dict) -> "TimeSeriesUpdate":
            return self._set(value)

        def add(self, value: Dict) -> "TimeSeriesUpdate":
            return self._add(value)

        def remove(self, value: List) -> "TimeSeriesUpdate":
            return self._remove(value)

    class _ListTimeSeriesUpdate(CogniteListUpdate):
        def set(self, value: List) -> "TimeSeriesUpdate":
            return self._set(value)

        def add(self, value: List) -> "TimeSeriesUpdate":
            return self._add(value)

        def remove(self, value: List) -> "TimeSeriesUpdate":
            return self._remove(value)

    class _LabelTimeSeriesUpdate(CogniteLabelUpdate):
        def add(self, value: List) -> "TimeSeriesUpdate":
            return self._add(value)

        def remove(self, value: List) -> "TimeSeriesUpdate":
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


class TimeSeriesAggregate(dict):
    """No description.

    Args:
        count (int): No description.
    """

    def __init__(self, count: int = None, **kwargs: Any) -> None:
        self.count = count
        self.update(kwargs)

    count = CognitePropertyClassUtil.declare_property("count")


class TimeSeriesList(CogniteResourceList):
    _RESOURCE = TimeSeries
