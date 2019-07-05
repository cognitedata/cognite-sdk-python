from typing import *
from typing import List

from cognite.client.data_classes._base import *


# GenClass: GetTimeSeriesMetadataDTO
class TimeSeries(CogniteResource):
    """No description.

    Args:
        id (int): The generated ID for the time series.
        external_id (str): The externally supplied ID for the time series.
        name (str): The name of the time series.
        is_string (bool): Whether the time series is string valued or not.
        metadata (Dict[str, Any]): Additional metadata. String key -> String value
        unit (str): The physical unit of the time series.
        asset_id (int): The asset that this time series belongs to.
        is_step (bool): Whether the time series is a step series or not.
        description (str): Description of the time series.
        security_categories (List[int]): The required security categories to access this time series.
        created_time (int): Time when this time series was created in CDF in milliseconds since Jan 1, 1970.
        last_updated_time (int): The latest time when this time series was updated in CDF in milliseconds since Jan 1, 1970.
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
        plt = utils._auxiliary.local_import("matplotlib.pyplot")
        identifier = utils._auxiliary.assert_at_least_one_of_id_or_external_id(self.id, self.external_id)
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

    def count(self) -> int:
        """Returns the number of datapoints in this time series.

        This result may not be completely accurate, as it is based on aggregates which may be occasionally out of date.

        Returns:
            int: The number of datapoints in this time series.
        """
        identifier = utils._auxiliary.assert_at_least_one_of_id_or_external_id(self.id, self.external_id)
        dps = self._cognite_client.datapoints.retrieve(
            start=0, end="now", aggregates=["count"], granularity="10d", **identifier
        )
        return sum(dps.count)

    def latest(self) -> Optional["Datapoint"]:
        """Returns the latest datapoint in this time series

        Returns:
            Datapoint: A datapoint object containing the value and timestamp of the latest datapoint.
        """
        identifier = utils._auxiliary.assert_at_least_one_of_id_or_external_id(self.id, self.external_id)
        dps = self._cognite_client.datapoints.retrieve_latest(**identifier)
        if len(dps) > 0:
            return list(dps)[0]
        return None

    def first(self) -> Optional["Datapoint"]:
        """Returns the first datapoint in this time series.

        Returns:
            Datapoint: A datapoint object containing the value and timestamp of the first datapoint.
        """
        identifier = utils._auxiliary.assert_at_least_one_of_id_or_external_id(self.id, self.external_id)
        dps = self._cognite_client.datapoints.retrieve(**identifier, start=0, end="now", limit=1)
        if len(dps) > 0:
            return list(dps)[0]
        return None


# GenClass: TimeSeriesSearchDTO.filter
class TimeSeriesFilter(CogniteFilter):
    """Filtering parameters

    Args:
        name (str): Filter on name.
        unit (str): Filter on unit.
        is_string (bool): Filter on isString.
        is_step (bool): Filter on isStep.
        metadata (Dict[str, Any]): Filter out timeseries that do not match these metadata fields and values (case-sensitive). The format is {"key1":"value1","key2":"value2"}.
        asset_ids (List[int]): Filter out time series that are not linked to any of these assets.
        external_id_prefix (str): Prefix filter on externalId. (case-sensitive)
        created_time (Dict[str, Any]): Filter out time series with createdTime outside this range.
        last_updated_time (Dict[str, Any]): Filter out time series with lastUpdatedTime outside this range.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        name: str = None,
        unit: str = None,
        is_string: bool = None,
        is_step: bool = None,
        metadata: Dict[str, Any] = None,
        asset_ids: List[int] = None,
        external_id_prefix: str = None,
        created_time: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        cognite_client=None,
    ):
        self.name = name
        self.unit = unit
        self.is_string = is_string
        self.is_step = is_step
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.external_id_prefix = external_id_prefix
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cognite_client

    # GenStop


# GenUpdateClass: TimeSeriesUpdate
class TimeSeriesUpdate(CogniteUpdate):
    """Changes will be applied to time series.

    Args:
        id (int): A JavaScript-friendly internal ID for the object.
        external_id (str): The external ID provided by the client. Must be unique within the project.
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
        self, start="1d-ago", end="now", aggregates=None, granularity=None, id_labels: bool = False, *args, **kwargs
    ):
        plt = utils._auxiliary.local_import("matplotlib.pyplot")
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
