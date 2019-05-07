from typing import *
from typing import List

from cognite.client._base import *


# GenClass: GetTimeSeriesMetadataDTO
class TimeSeries(CogniteResource):
    """No description.

    Args:
        id (int): Generated id of the time series
        external_id (str): Externally supplied id of the time series
        name (str): Name of time series
        is_string (bool): Whether the time series is string valued or not.
        metadata (Dict[str, Any]): Additional metadata. String key -> String value.
        unit (str): The physical unit of the time series.
        asset_id (int): Asset that this time series belongs to.
        is_step (bool): Whether the time series is a step series or not.
        description (str): Description of the time series.
        security_categories (List[int]): Security categories required in order to access this time series.
        created_time (int): Time when this time-series is created in CDF in milliseconds since Jan 1, 1970.
        last_updated_time (int): The latest time when this time-series is updated in CDF in milliseconds since Jan 1, 1970.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        id: int = None,
        external_id: str = None,
        name: str = None,
        is_string: bool = None,
        metadata: Dict[str, Any] = None,
        unit: str = None,
        asset_id: int = None,
        is_step: bool = None,
        description: str = None,
        security_categories: List[int] = None,
        created_time: int = None,
        last_updated_time: int = None,
        cognite_client=None,
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
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cognite_client

    # GenStop

    def plot(
        self, start="1d-ago", end="now", aggregates=None, granularity=None, id_labels: bool = False, *args, **kwargs
    ):
        plt = utils.local_import("matplotlib.pyplot")
        identifier = utils.assert_at_least_one_of_id_or_external_id(self.id, self.external_id)
        dps = self._cognite_client.datapoints.retrieve(
            start=start, end=end, aggregates=aggregates, granularity=granularity, **identifier
        )
        if id_labels:
            dps.plot(*args, **kwargs)
        else:
            columns = {self.id: self.name}
            for agg in aggregates or []:
                columns["{}|{}".format(self.id, agg)] = "{}|{}".format(self.name, agg)
            df = dps.to_pandas().rename(columns=columns)
            df.plot(*args, **kwargs)
            plt.show()


# GenClass: TimeSeriesSearchDTO.filter
class TimeSeriesFilter(CogniteFilter):
    """Filtering parameters

    Args:
        unit (str): Filter on unit (case-sensitive).
        is_string (bool): Filter on isString.
        is_step (bool): Filter on isStep.
        metadata (Dict[str, Any]): Filter out timeseries that do not match these metadata fields and values (case-sensitive). Format is {"key1":"value1","key2":"value2"}.
        asset_ids (List[int]): Filter out time series that are not linked to any of these assets.
        asset_subtrees (List[int]): Filter out time series that are not linked to assets in the subtree rooted at these assets. Format is list of ids.
        created_time (Dict[str, Any]): Filter out time series with createdTime outside this range.
        last_updated_time (Dict[str, Any]): Filter out time series with lastUpdatedTime outside this range.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        unit: str = None,
        is_string: bool = None,
        is_step: bool = None,
        metadata: Dict[str, Any] = None,
        asset_ids: List[int] = None,
        asset_subtrees: List[int] = None,
        created_time: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        cognite_client=None,
    ):
        self.unit = unit
        self.is_string = is_string
        self.is_step = is_step
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.asset_subtrees = asset_subtrees
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cognite_client

    # GenStop


# GenUpdateClass: TimeSeriesUpdate
class TimeSeriesUpdate(CogniteUpdate):
    """Changes will be applied to timeseries.

    Args:
        id (int): Javascript friendly internal ID given to the object.
        external_id (str): External Id provided by client. Should be unique within the project.
    """

    @property
    def external_id(self):
        return _PrimitiveTimeSeriesUpdate(self, "externalId")

    @property
    def name(self):
        return _PrimitiveTimeSeriesUpdate(self, "name")

    @property
    def metadata(self):
        return _ObjectTimeSeriesUpdate(self, "metadata")

    @property
    def unit(self):
        return _PrimitiveTimeSeriesUpdate(self, "unit")

    @property
    def asset_id(self):
        return _PrimitiveTimeSeriesUpdate(self, "assetId")

    @property
    def description(self):
        return _PrimitiveTimeSeriesUpdate(self, "description")

    @property
    def security_categories(self):
        return _ListTimeSeriesUpdate(self, "securityCategories")


class _PrimitiveTimeSeriesUpdate(CognitePrimitiveUpdate):
    def set(self, value: Any) -> TimeSeriesUpdate:
        return self._set(value)


class _ObjectTimeSeriesUpdate(CogniteObjectUpdate):
    def set(self, value: Dict) -> TimeSeriesUpdate:
        return self._set(value)

    def add(self, value: Dict) -> TimeSeriesUpdate:
        return self._add(value)

    def remove(self, value: List) -> TimeSeriesUpdate:
        return self._remove(value)


class _ListTimeSeriesUpdate(CogniteListUpdate):
    def set(self, value: List) -> TimeSeriesUpdate:
        return self._set(value)

    def add(self, value: List) -> TimeSeriesUpdate:
        return self._add(value)

    def remove(self, value: List) -> TimeSeriesUpdate:
        return self._remove(value)

    # GenStop


class TimeSeriesList(CogniteResourceList):
    _RESOURCE = TimeSeries
    _UPDATE = TimeSeriesUpdate

    def plot(
        self, start="52w-ago", end="now", aggregates=None, granularity="1d", id_labels: bool = False, *args, **kwargs
    ):
        plt = utils.local_import("matplotlib.pyplot")
        aggregates = aggregates or ["average"]
        dps = self._cognite_client.datapoints.retrieve(
            id=[ts.id for ts in self.data], start=start, end=end, aggregates=aggregates, granularity=granularity
        )
        if id_labels:
            dps.plot(*args, **kwargs)
        else:
            columns = {}
            for ts in self.data:
                columns[ts.id] = ts.name
                for agg in aggregates or []:
                    columns["{}|{}".format(ts.id, agg)] = "{}|{}".format(ts.name, agg)
            df = dps.to_pandas().rename(columns=columns)
            df.plot(*args, **kwargs)
            plt.show()
