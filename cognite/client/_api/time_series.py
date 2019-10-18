from typing import *

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import TimeSeries, TimeSeriesFilter, TimeSeriesList, TimeSeriesUpdate


class TimeSeriesAPI(APIClient):
    _RESOURCE_PATH = "/timeseries"
    _LIST_CLASS = TimeSeriesList

    def __call__(
        self,
        chunk_size: int = None,
        name: str = None,
        unit: str = None,
        is_string: bool = None,
        is_step: bool = None,
        asset_ids: List[int] = None,
        root_asset_ids: List[int] = None,
        metadata: Dict[str, Any] = None,
        external_id_prefix: str = None,
        created_time: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        limit: int = None,
        include_metadata=True,
    ) -> Generator[Union[TimeSeries, TimeSeriesList], None, None]:
        """Iterate over time series

        Fetches time series as they are iterated over, so you keep a limited number of objects in memory.

        Args:
            chunk_size (int, optional): Number of time series to return in each chunk. Defaults to yielding one time series a time.
            name (str): Name of the time series. Often referred to as tag.
            unit (str): Unit of the time series.
            is_string (bool): Whether the time series is an string time series.
            is_step (bool): Whether the time series is a step (piecewise constant) time series.
            asset_ids (List[int], optional): List time series related to these assets.
            root_asset_ids (List[int], optional): List time series related to assets under these root assets.
            metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value
            created_time (Dict[str, Any]): Range between two timestamps
            last_updated_time (Dict[str, Any]): Range between two timestamps
            external_id_prefix (str): Filter by this (case-sensitive) prefix for the external ID.
            limit (int, optional): Maximum number of time series to return. Defaults to return all items.
            include_metadata (bool, optional): Ignored. Only present in parameter list for backward compatibility.

        Yields:
            Union[TimeSeries, TimeSeriesList]: yields TimeSeries one by one if chunk is not specified, else TimeSeriesList objects.
        """
        filter = TimeSeriesFilter(
            name=name,
            unit=unit,
            is_step=is_step,
            is_string=is_string,
            asset_ids=asset_ids,
            root_asset_ids=root_asset_ids,
            metadata=metadata,
            created_time=created_time,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
        ).dump(camel_case=True)
        return self._list_generator(method="POST", chunk_size=chunk_size, filter=filter, limit=limit)

    def __iter__(self) -> Generator[TimeSeries, None, None]:
        """Iterate over time series

        Fetches time series as they are iterated over, so you keep a limited number of metadata objects in memory.

        Yields:
            TimeSeries: yields TimeSeries one by one.
        """
        return self.__call__()

    def retrieve(self, id: Optional[int] = None, external_id: Optional[str] = None) -> Optional[TimeSeries]:
        """`Retrieve a single time series by id. <https://docs.cognite.com/api/v1/#operation/getTimeSeriesByIds>`_

        Args:
            id (int, optional): ID
            external_id (str, optional): External ID

        Returns:
            Optional[TimeSeries]: Requested time series or None if it does not exist.

        Examples:

            Get time series by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.retrieve(id=1)

            Get time series by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.retrieve(external_id="1")
        """
        utils._auxiliary.assert_exactly_one_of_id_or_external_id(id, external_id)
        return self._retrieve_multiple(ids=id, external_ids=external_id, wrap_ids=True)

    def retrieve_multiple(
        self, ids: Optional[List[int]] = None, external_ids: Optional[List[str]] = None
    ) -> TimeSeriesList:
        """`Retrieve multiple time series by id. <https://docs.cognite.com/api/v1/#operation/getTimeSeriesByIds>`_

        Args:
            ids (List[int], optional): IDs
            external_ids (List[str], optional): External IDs

        Returns:
            TimeSeriesList: The requested time series.

        Examples:

            Get time series by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.retrieve_multiple(ids=[1, 2, 3])

            Get time series by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.retrieve_multiple(external_ids=["abc", "def"])
        """
        utils._auxiliary.assert_type(ids, "id", [List], allow_none=True)
        utils._auxiliary.assert_type(external_ids, "external_id", [List], allow_none=True)
        return self._retrieve_multiple(ids=ids, external_ids=external_ids, wrap_ids=True)

    def list(
        self,
        name: str = None,
        unit: str = None,
        is_string: bool = None,
        is_step: bool = None,
        asset_ids: List[int] = None,
        root_asset_ids: List[int] = None,
        metadata: Dict[str, Any] = None,
        external_id_prefix: str = None,
        created_time: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        partitions: int = None,
        limit: int = 25,
        include_metadata=True,
    ) -> TimeSeriesList:
        """`List over time series <https://docs.cognite.com/api/v1/#operation/listTimeSeries>`_

        Fetches time series as they are iterated over, so you keep a limited number of objects in memory.

        Args:
            name (str): Name of the time series. Often referred to as tag.
            unit (str): Unit of the time series.
            is_string (bool): Whether the time series is an string time series.
            is_step (bool): Whether the time series is a step (piecewise constant) time series.
            asset_ids (List[int], optional): List time series related to these assets.
            root_asset_ids (List[int], optional): List time series related to assets under these root assets.
            metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value
            created_time (Dict[str, Any]): Range between two timestamps
            last_updated_time (Dict[str, Any]): Range between two timestamps
            external_id_prefix (str): Filter by this (case-sensitive) prefix for the external ID.
            limit (int, optional): Maximum number of time series to return.  Defaults to 25. Set to -1, float("inf") or None to return all items.
            partitions (int): Retrieve time series in parallel using this number of workers. Also requires `limit=None` to be passed.
            include_metadata (bool, optional): Ignored. Only present in parameter list for backward compatibility.


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
        filter = TimeSeriesFilter(
            name=name,
            unit=unit,
            is_step=is_step,
            is_string=is_string,
            asset_ids=asset_ids,
            root_asset_ids=root_asset_ids,
            metadata=metadata,
            created_time=created_time,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
        ).dump(camel_case=True)
        return self._list(method="POST", filter=filter, limit=limit, partitions=partitions)

    def create(self, time_series: Union[TimeSeries, List[TimeSeries]]) -> Union[TimeSeries, TimeSeriesList]:
        """`Create one or more time series. <https://docs.cognite.com/api/v1/#operation/postTimeSeries>`_

        Args:
            time_series (Union[TimeSeries, List[TimeSeries]]): TimeSeries or list of TimeSeries to create.

        Returns:
            Union[TimeSeries, TimeSeriesList]: The created time series.

        Examples:

            Create a new time series::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TimeSeries
                >>> c = CogniteClient()
                >>> ts = c.time_series.create(TimeSeries(name="my ts"))
        """
        return self._create_multiple(items=time_series)

    def delete(self, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None) -> None:
        """`Delete one or more time series. <https://docs.cognite.com/api/v1/#operation/deleteTimeSeries>`_

        Args:
            id (Union[int, List[int]): Id or list of ids
            external_id (Union[str, List[str]]): External ID or list of external ids

        Returns:
            None

        Examples:

            Delete time series by id or external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.time_series.delete(id=[1,2,3], external_id="3")
        """
        self._delete_multiple(wrap_ids=True, ids=id, external_ids=external_id)

    def update(
        self, item: Union[TimeSeries, TimeSeriesUpdate, List[Union[TimeSeries, TimeSeriesUpdate]]]
    ) -> Union[TimeSeries, TimeSeriesList]:
        """`Update one or more time series. <https://docs.cognite.com/api/v1/#operation/alterTimeSeries>`_

        Args:
            item (Union[TimeSeries, TimeSeriesUpdate, List[Union[TimeSeries, TimeSeriesUpdate]]]): Time series to update

        Returns:
            Union[TimeSeries, TimeSeriesList]: Updated time series.

        Examples:

            Update a time series that you have fetched. This will perform a full update of the time series::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.retrieve(id=1)
                >>> res.description = "New description"
                >>> res = c.time_series.update(res)

            Perform a partial update on a time series, updating the description and adding a new field to metadata::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TimeSeriesUpdate
                >>> c = CogniteClient()
                >>> my_update = TimeSeriesUpdate(id=1).description.set("New description").metadata.add({"key": "value"})
                >>> res = c.time_series.update(my_update)
        """
        return self._update_multiple(items=item)

    def search(
        self,
        name: str = None,
        description: str = None,
        query: str = None,
        filter: Union[TimeSeriesFilter, Dict] = None,
        limit: int = None,
    ) -> TimeSeriesList:
        """`Search for time series. <https://docs.cognite.com/api/v1/#operation/searchTimeSeries>`_

        Args:
            name (str, optional): Prefix and fuzzy search on name.
            description (str, optional): Prefix and fuzzy search on description.
            query (str, optional): Search on name and description using wildcard search on each of the words (separated
                by spaces). Retrieves results where at least one word must match. Example: 'some other'
            filter (Union[TimeSeriesFilter, Dict], optional): Filter to apply. Performs exact match on these fields.
            limit (int, optional): Max number of results to return.

        Returns:
            TimeSeriesList: List of requested time series.

        Examples:

            Search for a time series::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.search(name="some name")

            Search for all time series connected to asset with id 123::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.search(filter={"asset_ids":[123]})
        """
        return self._search(
            search={"name": name, "description": description, "query": query}, filter=filter, limit=limit
        )
