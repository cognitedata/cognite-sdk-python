from typing import *
from typing import List

from cognite.client._utils.api_client import APIClient
from cognite.client._utils.base import *


# GenClass: GetTimeSeriesMetadataDTO
class TimeSeries(CogniteResource):
    """No description.

    Args:
        id (int): Generated id of the time series
        external_id (str): Externaly supplied id of the time series
        name (str): Unique name of time series
        is_string (bool): Whether the time series is string valued or not.
        metadata (Dict[str, Any]): Additional metadata. String key -> String value.
        unit (str): The physical unit of the time series.
        asset_id (int): Asset that this time series belongs to.
        is_step (bool): Whether the time series is a step series or not.
        description (str): Description of the time series.
        security_categories (List[int]): Security categories required in order to access this time series.
        created_time (int): Time when this time-series is created in CDP in milliseconds since Jan 1, 1970.
        last_updated_time (int): The latest time when this time-series is updated in CDP in milliseconds since Jan 1, 1970.
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
        **kwargs
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

    # GenStop

    def plot(self, start="1d-ago", end="now", aggregates=None, granularity=None, id_labels: bool = False):
        plt = utils.local_import("matplotlib.pyplot")
        identifier = utils.assert_exactly_one_of_id_or_external_id(self.id, self.external_id)
        dps = self._client.datapoints.get(
            start=start, end=end, aggregates=aggregates, granularity=granularity, **identifier
        )
        if id_labels:
            dps.plot()
        else:
            columns = {self.id: self.name}
            for agg in aggregates or []:
                columns["{}|{}".format(self.id, agg)] = "{}|{}".format(self.name, agg)
            df = dps.to_pandas().rename(columns=columns)
            df.plot()
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
        created_time (Dict[str, Any]): Filter out time series with createdTime before this. Format is milliseconds since epoch.
        last_updated_time (Dict[str, Any]): Filter out time series with lastUpdatedTime before this. Format is milliseconds since epoch.
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
        **kwargs
    ):
        self.unit = unit
        self.is_string = is_string
        self.is_step = is_step
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.asset_subtrees = asset_subtrees
        self.created_time = created_time
        self.last_updated_time = last_updated_time

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
        return PrimitiveUpdate(self, "externalId")

    @property
    def name(self):
        return PrimitiveUpdate(self, "name")

    @property
    def metadata(self):
        return ObjectUpdate(self, "metadata")

    @property
    def unit(self):
        return PrimitiveUpdate(self, "unit")

    @property
    def asset_id(self):
        return PrimitiveUpdate(self, "assetId")

    @property
    def description(self):
        return PrimitiveUpdate(self, "description")

    @property
    def security_categories(self):
        return ListUpdate(self, "securityCategories")


class PrimitiveUpdate(CognitePrimitiveUpdate):
    def set(self, value: Any) -> TimeSeriesUpdate:
        return self._set(value)


class ObjectUpdate(CogniteObjectUpdate):
    def set(self, value: Dict) -> TimeSeriesUpdate:
        return self._set(value)

    def add(self, value: Dict) -> TimeSeriesUpdate:
        return self._add(value)

    def remove(self, value: List) -> TimeSeriesUpdate:
        return self._remove(value)


class ListUpdate(CogniteListUpdate):
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

    def plot(self, start="52w-ago", end="now", aggregates=None, granularity="1d", id_labels: bool = False):
        plt = utils.local_import("matplotlib.pyplot")
        aggregates = aggregates or ["average"]
        dps = self._client.datapoints.get(
            id=[ts.id for ts in self.data], start=start, end=end, aggregates=aggregates, granularity=granularity
        )
        if id_labels:
            dps.plot()
        else:
            columns = {}
            for ts in self.data:
                columns[ts.id] = ts.name
                for agg in aggregates or []:
                    columns["{}|{}".format(ts.id, agg)] = "{}|{}".format(ts.name, agg)
            df = dps.to_pandas().rename(columns=columns)
            df.plot()
            plt.show()


class TimeSeriesAPI(APIClient):
    _RESOURCE_PATH = "/timeseries"

    def __call__(
        self, chunk_size: int = None, include_metadata: bool = False, asset_id: int = None
    ) -> Generator[Union[TimeSeries, TimeSeriesList], None, None]:
        """Iterate over time series

        Fetches time series as they are iterated over, so you keep a limited number of objects in memory.

        Args:
            chunk_size (int, optional): Number of time series to return in each chunk. Defaults to yielding one event a time.
            include_metadata (bool, optional): Whether or not to include metadata
            asset_id (int, optional): List time series related to this asset.

        Yields:
            Union[TimeSeries, TimeSeriesList]: yields TimeSeries one by one if chunk is not specified, else TimeSeriesList objects.
        """
        filter = {"includeMetadata": include_metadata, "assetId": asset_id}
        return self._list_generator(
            TimeSeriesList, resource_path=self._RESOURCE_PATH, method="GET", chunk_size=chunk_size, filter=filter
        )

    def __iter__(self) -> Generator[TimeSeries, None, None]:
        """Iterate over time series

        Fetches time series as they are iterated over, so you keep a limited number of metadata objects in memory.

        Yields:
            TimeSeries: yields TimeSeries one by one.
        """
        return self.__call__()

    def get(
        self, id: Union[int, List[int]] = None, external_id: Union[int, List[int]] = None
    ) -> Union[TimeSeries, TimeSeriesList]:
        """Returns an object containing the requested timeseries.

        Args:
            id (Union[int, List[int]], optional): Id or list of ids
            external_id(Union[str, List[str]], optional): str or list of str

        Returns:
            Union[TimeSeries, TimeSeriesList]: The requested time series

        Examples:

            Retrieving time series by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.get(id=[1,2])
        """
        return self._retrieve_multiple(
            cls=TimeSeriesList, resource_path=self._RESOURCE_PATH, ids=id, external_ids=external_id, wrap_ids=True
        )

    def list(self, include_metadata: bool = False, asset_id: int = None, limit: int = None) -> TimeSeriesList:
        """Iterate over time series

        Fetches time series as they are iterated over, so you keep a limited number of objects in memory.

        Args:
            include_metadata (bool, optional): Whether or not to include metadata
            asset_id (int, optional): List time series related to this asset.
            limit (int, optional): Max number of time series to return.

        Returns:
            TimeSeriesList: The requested time series.

        Examples:

            List time series::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.list(limit=5)

            Iterate over time series::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for ts in c.time_series:
                ...     ts # do something with the time_series

            Iterate over chunks of time series to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for ts_list in c.time_series(chunk_size=2500):
                ...     ts_list # do something with the time_series
        """
        filter = {"includeMetadata": include_metadata, "assetId": asset_id}
        return self._list(
            cls=TimeSeriesList, resource_path=self._RESOURCE_PATH, method="GET", filter=filter, limit=limit
        )

    def create(self, time_series: Union[TimeSeries, List[TimeSeries]]) -> Union[TimeSeries, TimeSeriesList]:
        """Create one or more time series.

        Args:
            time_series (Union[TimeSeries, List[TimeSeries]]): TimeSeries or list of TimeSeries to create.

        Returns:
            Union[TimeSeries, TimeSeriesList]: The created time series.

        Examples:

            Create a new time series::

                >>> from cognite.client import CogniteClient, TimeSeries
                >>> c = CogniteClient()
                >>> ts = c.time_series.create(TimeSeries(name="my ts"))
        """
        return self._create_multiple(cls=TimeSeriesList, resource_path=self._RESOURCE_PATH, items=time_series)

    def delete(self, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None) -> None:
        """Delete one or more time series.

        Args:
            id (Union[int, List[int]): Id or list of ids
            external_id (Union[str, List[str]]): External ID or list of external ids

        Returns:
            None

        Examples:

            Delete time series by id or external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.delete(id=[1,2,3], external_id="3")
        """
        self._delete_multiple(resource_path=self._RESOURCE_PATH, wrap_ids=True, ids=id, external_ids=external_id)

    def update(
        self, item: Union[TimeSeries, TimeSeriesUpdate, List[Union[TimeSeries, TimeSeriesUpdate]]]
    ) -> Union[TimeSeries, TimeSeriesList]:
        """Update one or more time series.

        Args:
            item (Union[TimeSeries, TimeSeriesUpdate, List[Union[TimeSeries, TimeSeriesUpdate]]]): Time series to update

        Returns:
            Union[TimeSeries, TimeSeriesList]: Updated time series.

        Examples:

            Update a time series that you have fetched. This will perform a full update of the time series::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.get(id=1)
                >>> res.description = "New description"
                >>> res = c.time_series.update(res)

            Perform a partial update on a time series, updating the description and adding a new field to metadata::

                >>> from cognite.client import CogniteClient, TimeSeriesUpdate
                >>> c = CogniteClient()
                >>> my_update = TimeSeriesUpdate(id=1).description.set("New description").metadata.add({"key": "value"})
                >>> res = c.time_series.update(my_update)
        """
        return self._update_multiple(cls=TimeSeriesList, resource_path=self._RESOURCE_PATH, items=item)

    def search(
        self,
        name: str = None,
        description: str = None,
        query: str = None,
        filter: TimeSeriesFilter = None,
        limit: int = None,
    ) -> TimeSeriesList:
        """Search for time series.

        Args:
            name (str, optional): Prefix and fuzzy search on name.
            description (str, optional): Prefix and fuzzy search on description.
            query (str, optional): Search on name and description using wildcard search on each of the words (separated
                by spaces). Retrieves results where at least one word must match. Example: 'some other'
            filter (TimeSeriesFilter, optional): Filter to apply. Performs exact match on these fields.
            limit (int, optional): Max number of results to return.

        Returns:
            TimeSeriesList: List of requested time series.

        Examples:

            Search for a time series::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.search(name="some name")
        """
        filter = filter.dump(camel_case=True) if filter else None
        return self._search(
            cls=TimeSeriesList,
            resource_path=self._RESOURCE_PATH,
            json={
                "search": {"name": name, "description": description, "query": query},
                "filter": filter,
                "limit": limit,
            },
        )
