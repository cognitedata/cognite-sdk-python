import copy
from typing import *

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import Relationship, RelationshipFilter, RelationshipList
from cognite.client.data_classes.labels import LabelFilter


class RelationshipsAPI(APIClient):
    _RESOURCE_PATH = "/relationships"
    _LIST_CLASS = RelationshipList

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._CREATE_LIMIT = 1000
        self._LIST_SUBQUERY_LIMIT = 1000

    def _create_filter(
        self,
        source_external_ids: List[str] = None,
        source_types: List[str] = None,
        target_external_ids: List[str] = None,
        target_types: List[str] = None,
        data_set_ids: List[Dict[str, Any]] = None,
        start_time: Dict[str, int] = None,
        end_time: Dict[str, int] = None,
        confidence: Dict[str, int] = None,
        last_updated_time: Dict[str, int] = None,
        created_time: Dict[str, int] = None,
        active_at_time: Dict[str, int] = None,
        labels: LabelFilter = None,
    ):
        return RelationshipFilter(
            source_external_ids=source_external_ids,
            source_types=source_types,
            target_external_ids=target_external_ids,
            target_types=target_types,
            data_set_ids=data_set_ids,
            start_time=start_time,
            end_time=end_time,
            confidence=confidence,
            last_updated_time=last_updated_time,
            created_time=created_time,
            active_at_time=active_at_time,
            labels=labels,
        ).dump(camel_case=True)

    def __call__(
        self,
        source_external_ids: List[str] = None,
        source_types: List[str] = None,
        target_external_ids: List[str] = None,
        target_types: List[str] = None,
        data_set_ids: List[int] = None,
        data_set_external_ids: List[str] = None,
        start_time: Dict[str, int] = None,
        end_time: Dict[str, int] = None,
        confidence: Dict[str, int] = None,
        last_updated_time: Dict[str, int] = None,
        created_time: Dict[str, int] = None,
        active_at_time: Dict[str, int] = None,
        labels: LabelFilter = None,
        limit: int = None,
        fetch_resources: bool = False,
    ) -> Generator[Union[Relationship, RelationshipList], None, None]:
        """Iterate over relationships

        Fetches relationships as they are iterated over, so you keep a limited number of relationships in memory.

        Args:
            source_external_ids (List[str]): Include relationships that have any of these values in their source External Id field
            source_types (List[str]): Include relationships that have any of these values in their source Type field
            target_external_ids (List[str]): Include relationships that have any of these values in their target External Id field
            target_types (List[str]): Include relationships that have any of these values in their target Type field
            data_set_ids (List[int]): Return only relationships in the specified data sets with these ids.
            data_set_external_ids (List[str]): Return only relationships in the specified data sets with these external ids.
            start_time (Dict[str, int]): Range between two timestamps, minimum and maximum milli seconds (inclusive)
            end_time (Dict[str, int]): Range between two timestamps, minimum and maximum milli seconds (inclusive)
            confidence (Dict[str, int]): Range to filter the field for. (inclusive)
            last_updated_time (Dict[str, Any]): Range to filter the field for. (inclusive)
            created_time (Dict[str, int]): Range to filter the field for. (inclusive)
            active_at_time (Dict[str, int]): Limits results to those active at any point within the given time range, i.e. if there is any overlap in the intervals [activeAtTime.min, activeAtTime.max] and [startTime, endTime], where both intervals are inclusive. If a relationship does not have a startTime, it is regarded as active from the begining of time by this filter. If it does not have an endTime is will be regarded as active until the end of time. Similarly, if a min is not supplied to the filter, the min will be implicitly set to the beginning of time, and if a max is not supplied, the max will be implicitly set to the end of time.
            labels (LabelFilter): Return only the resource matching the specified label constraints.

        Yields:
            Union[Relationship, RelationshipList]: yields Relationship one by one if chunk is not specified, else RelationshipList objects.
        """
        if data_set_ids or data_set_external_ids:
            data_set_ids = self._process_ids(data_set_ids, data_set_external_ids, wrap_ids=True)

        filter = self._create_filter(
            source_external_ids=source_external_ids,
            source_types=source_types,
            target_external_ids=target_external_ids,
            target_types=target_types,
            data_set_ids=data_set_ids,
            start_time=start_time,
            end_time=end_time,
            confidence=confidence,
            last_updated_time=last_updated_time,
            created_time=created_time,
            active_at_time=active_at_time,
            labels=labels,
        )
        if (
            len(filter.get("targetExternalIds", [])) > self._LIST_SUBQUERY_LIMIT
            or len(filter.get("sourceExternalIds", [])) > self._LIST_SUBQUERY_LIMIT
        ):
            raise ValueError(
                "For queries with more than {} source_external_ids or target_external_ids, only list is supported".format(
                    self._LIST_SUBQUERY_LIMIT
                )
            )

        return self._list_generator(
            method="POST", limit=limit, filter=filter, other_params={"fetchResources": fetch_resources}
        )

    def __iter__(self) -> Generator[Relationship, None, None]:
        """Iterate over relationships

        Fetches relationships as they are iterated over, so you keep a limited number of relationships in memory.

        Yields:
            Relationship: yields Relationships one by one.
        """
        return self.__call__()

    def retrieve(self, external_id: str, fetch_resources: bool = False) -> Optional[Relationship]:
        """Retrieve a single relationship by external id.

        Args:
            external_id (str): External ID
            fetch_resources (bool): if true, will try to return the full resources referenced by the relationship in the
                source and target fields.

        Returns:
            Optional[Relationship]: Requested relationship or None if it does not exist.

        Examples:

            Get relationship by external id:

                >>> from cognite.client.beta import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.relationships.retrieve(external_id="1")
        """
        return self._retrieve_multiple(
            external_ids=external_id, wrap_ids=True, other_params={"fetchResources": fetch_resources}
        )

    def retrieve_multiple(self, external_ids: List[str], fetch_resources: bool = False) -> RelationshipList:
        """`Retrieve multiple relationships by external id.  <https://docs.cognite.com/api/v1/#operation/byidsRelationships>`_

        Args:
            external_ids (List[str]): External IDs
            fetch_resources (bool): if true, will try to return the full resources referenced by the relationship in the
                source and target fields.

        Returns:
            RelationshipList: The requested relationships.

        Examples:

            Get relationships by external id::

                >>> from cognite.client.beta import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.relationships.retrieve_multiple(external_ids=["abc", "def"])
        """
        utils._auxiliary.assert_type(external_ids, "external_id", [List], allow_none=False)
        return self._retrieve_multiple(
            external_ids=external_ids, wrap_ids=True, other_params={"fetchResources": fetch_resources}
        )

    def list(
        self,
        source_external_ids: List[str] = None,
        source_types: List[str] = None,
        target_external_ids: List[str] = None,
        target_types: List[str] = None,
        data_set_ids: List[int] = None,
        data_set_external_ids: List[str] = None,
        start_time: Dict[str, int] = None,
        end_time: Dict[str, int] = None,
        confidence: Dict[str, int] = None,
        last_updated_time: Dict[str, int] = None,
        created_time: Dict[str, int] = None,
        active_at_time: Dict[str, int] = None,
        labels: LabelFilter = None,
        limit: int = 100,
        fetch_resources: bool = False,
    ) -> RelationshipList:
        """`Lists relationships stored in the project based on a query filter given in the payload of this request. Up to 1000 relationships can be retrieved in one operation.  <https://docs.cognite.com/api/v1/#operation/listRelationships>`_

        Args:
            source_external_ids (List[str]): Include relationships that have any of these values in their source External Id field
            source_types (List[str]): Include relationships that have any of these values in their source Type field
            target_external_ids (List[str]): Include relationships that have any of these values in their target External Id field
            target_types (List[str]): Include relationships that have any of these values in their target Type field
            data_set_ids (List[int]): Return only relationships in the specified data sets with these ids.
            data_set_external_ids (List[str]): Return only relationships in the specified data sets with these external ids.
            start_time (Dict[str, int]): Range between two timestamps, minimum and maximum milli seconds (inclusive)
            end_time (Dict[str, int]): Range between two timestamps, minimum and maximum milli seconds (inclusive)
            confidence (Dict[str, int]): Range to filter the field for. (inclusive)
            last_updated_time (Dict[str, Any]): Range to filter the field for. (inclusive)
            created_time (Dict[str, int]): Range to filter the field for. (inclusive)
            active_at_time (Dict[str, int]): Limits results to those active at any point within the given time range, i.e. if there is any overlap in the intervals [activeAtTime.min, activeAtTime.max] and [startTime, endTime], where both intervals are inclusive. If a relationship does not have a startTime, it is regarded as active from the begining of time by this filter. If it does not have an endTime is will be regarded as active until the end of time. Similarly, if a min is not supplied to the filter, the min will be implicitly set to the beginning of time, and if a max is not supplied, the max will be implicitly set to the end of time.
            labels (LabelFilter): Return only the resource matching the specified label constraints.
            limit (int): Maximum number of relationships to return. Defaults to 100. Set to -1, float("inf") or None
                to return all items.
            fetch_resources (bool): if true, will try to return the full resources referenced by the relationship in the
                source and target fields.

        Returns:
            RelationshipList: List of requested relationships

        Examples:

            List relationships::

                >>> from cognite.client.beta import CogniteClient
                >>> c = CogniteClient()
                >>> relationship_list = c.relationships.list(limit=5)

            Iterate over relationships::

                >>> from cognite.client.beta import CogniteClient
                >>> c = CogniteClient()
                >>> for relationship in c.relationships:
                ...     relationship # do something with the relationship
        """

        if data_set_ids or data_set_external_ids:
            data_set_ids = self._process_ids(data_set_ids, data_set_external_ids, wrap_ids=True)

        filter = self._create_filter(
            source_external_ids=source_external_ids,
            source_types=source_types,
            target_external_ids=target_external_ids,
            target_types=target_types,
            data_set_ids=data_set_ids,
            start_time=start_time,
            end_time=end_time,
            confidence=confidence,
            last_updated_time=last_updated_time,
            created_time=created_time,
            active_at_time=active_at_time,
            labels=labels,
        )
        target_external_ids = filter.get("targetExternalIds", [])
        source_external_ids = filter.get("sourceExternalIds", [])
        if len(target_external_ids) > self._LIST_SUBQUERY_LIMIT or len(source_external_ids) > self._LIST_SUBQUERY_LIMIT:
            if limit not in [-1, None, float("inf")]:
                raise ValueError(
                    "Querying more than {} source_external_ids/target_external_ids only supported for queries without limit (pass -1 / None / inf instead of {}".format(
                        self._LIST_SUBQUERY_LIMIT, limit
                    )
                )
            tasks = []

            for ti in range(0, max(1, len(target_external_ids)), self._LIST_SUBQUERY_LIMIT):
                for si in range(0, max(1, len(source_external_ids)), self._LIST_SUBQUERY_LIMIT):
                    task_filter = copy.copy(filter)
                    if target_external_ids:  # keep null if it was
                        task_filter["targetExternalIds"] = target_external_ids[ti : ti + self._LIST_SUBQUERY_LIMIT]
                    if source_external_ids:  # keep null if it was
                        task_filter["sourceExternalIds"] = source_external_ids[si : si + self._LIST_SUBQUERY_LIMIT]
                    tasks.append((task_filter,))

            tasks_summary = utils._concurrency.execute_tasks_concurrently(
                lambda filter: self._list(
                    method="POST", limit=limit, filter=filter, other_params={"fetchResources": fetch_resources}
                ),
                tasks,
                max_workers=self._config.max_workers,
            )
            tasks_summary.raise_compound_exception_if_failed_tasks()
            rels = RelationshipList([rel for result in tasks_summary.joined_results() for rel in result])
            return rels
        return self._list(method="POST", limit=limit, filter=filter, other_params={"fetchResources": fetch_resources})

    def create(self, relationship: Union[Relationship, List[Relationship]]) -> Union[Relationship, RelationshipList]:
        """`Create one or more relationships. <https://docs.cognite.com/api/v1/#operation/createRelationships>`_

        Args:
            relationship (Union[Relationship, List[Relationship]]): Relationship or list of relationships to create.
                Note: the source_type and target_type field in the Relationship(s) can be any string among "Asset", "TimeSeries", "FileMetadata", "Event", "Sequence"

        Returns:
            Union[Relationship, RelationshipList]: Created relationship(s)

        Examples:

            Create a new relationship specifying object type and external id for source and target::

                >>> from cognite.client.beta import CogniteClient
                >>> from cognite.client.data_classes import Relationship
                >>> c = CogniteClient()
                >>> assets = c.assets.retrieve_multiple(id=[1,2,3])
                >>> flowrel1 = Relationship(external_id="flow_1", source_external_id="source_ext_id", source_type="asset", target_external_id="target_ext_id", target_type="event", confidence=0.1, data_set_id=1234)
                >>> flowrel2 = Relationship(external_id="flow_2", source_external_id="source_ext_id", source_type="asset", target_external_id="target_ext_id", target_type="event", confidence=0.1, data_set_id=1234)
                >>> res = c.relationships.create([flowrel1,flowrel2])
        """
        utils._auxiliary.assert_type(relationship, "relationship", [Relationship, list])
        if isinstance(relationship, list):
            relationship = [r._validate_resource_types() for r in relationship]
        else:
            relationship = relationship._validate_resource_types()

        return self._create_multiple(items=relationship)

    def delete(self, external_id: Union[str, List[str]]) -> None:
        """`Delete one or more relationships. <https://docs.cognite.com/api/v1/#operation/deleteRelationships>`_

        Args:
            external_id (Union[str, List[str]]): External ID or list of external ids

        Returns:
            None

        Examples:

            Delete relationships by external id::

                >>> from cognite.client.beta import CogniteClient
                >>> c = CogniteClient()
                >>> c.relationships.delete(external_id=["a","b"])
        """
        self._delete_multiple(external_ids=external_id, wrap_ids=True)
