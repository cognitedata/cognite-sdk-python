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
        data_set_ids: List[Dict[str, Any]] = None,
        start_time: Dict[str, int] = None,
        end_time: Dict[str, int] = None,
        confidence: Dict[str, int] = None,
        last_updated_time: Dict[str, int] = None,
        created_time: Dict[str, int] = None,
        active_at_time: Dict[str, int] = None,
        labels: LabelFilter = None,
        limit: int = None,
    ) -> Generator[Union[Relationship, RelationshipList], None, None]:
        """Iterate over relationships

        Fetches relationships as they are iterated over, so you keep a limited number of relationships in memory.

        Args:
            source_external_ids (List[str]): Include relationships that have any of these values in their source External Id field
            source_types (List[str]): Include relationships that have any of these values in their source Type field
            target_external_ids (List[str]): Include relationships that have any of these values in their target External Id field
            target_types (List[str]): Include relationships that have any of these values in their target Type field
            data_set_ids (List[Dict[str, Any]]): Either one of internalId (int) or externalId (str)
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
        return self._list_generator(method="POST", limit=limit, filter=filter)

    def __iter__(self) -> Generator[Relationship, None, None]:
        """Iterate over relationships

        Fetches relationships as they are iterated over, so you keep a limited number of relationships in memory.

        Yields:
            Relationship: yields Relationships one by one.
        """
        return self.__call__()

    def retrieve(self, external_id: str) -> Optional[Relationship]:
        """Retrieve a single relationship by external id.

        Args:
            external_id (str): External ID

        Returns:
            Optional[Relationship]: Requested relationship or None if it does not exist.

        Examples:

            Get relationship by external id::

                >>> from cognite.client.beta import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.relationships.retrieve(external_id="1")
        """
        return self._retrieve_multiple(external_ids=external_id, wrap_ids=True)

    def retrieve_multiple(self, external_ids: List[str]) -> RelationshipList:
        """Retrieve multiple relationships by external id.

        Args:
            external_ids (List[str]): External IDs

        Returns:
            RelationshipList: The requested relationships.

        Examples:

            Get relationships by external id::

                >>> from cognite.client.beta import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.relationships.retrieve_multiple(external_ids=["abc", "def"])
        """
        utils._auxiliary.assert_type(external_ids, "external_id", [List], allow_none=False)
        return self._retrieve_multiple(external_ids=external_ids, wrap_ids=True)

    def list(
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
        limit: int = 100,
    ) -> RelationshipList:
        """Lists relationships stored in the project based on a query filter given in the payload of this request. Up to 1000 relationships can be retrieved in one operation.

        Args:
            source_external_ids (List[str]): Include relationships that have any of these values in their source External Id field
            source_types (List[str]): Include relationships that have any of these values in their source Type field
            target_external_ids (List[str]): Include relationships that have any of these values in their target External Id field
            target_types (List[str]): Include relationships that have any of these values in their target Type field
            data_set_ids (List[Dict[str, Any]]): Either one of internalId (int) or externalId (str)
            start_time (Dict[str, int]): Range between two timestamps, minimum and maximum milli seconds (inclusive)
            end_time (Dict[str, int]): Range between two timestamps, minimum and maximum milli seconds (inclusive)
            confidence (Dict[str, int]): Range to filter the field for. (inclusive)
            last_updated_time (Dict[str, Any]): Range to filter the field for. (inclusive)
            created_time (Dict[str, int]): Range to filter the field for. (inclusive)
            active_at_time (Dict[str, int]): Limits results to those active at any point within the given time range, i.e. if there is any overlap in the intervals [activeAtTime.min, activeAtTime.max] and [startTime, endTime], where both intervals are inclusive. If a relationship does not have a startTime, it is regarded as active from the begining of time by this filter. If it does not have an endTime is will be regarded as active until the end of time. Similarly, if a min is not supplied to the filter, the min will be implicitly set to the beginning of time, and if a max is not supplied, the max will be implicitly set to the end of time.
            labels (LabelFilter): Return only the resource matching the specified label constraints.
            limit (int): Maximum number of relationships to return. Defaults to 100. Set to -1, float("inf") or None
                to return all items.

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
        return self._list(method="POST", limit=limit, filter=filter)

    def create(self, relationship: Union[Relationship, List[Relationship]]) -> Union[Relationship, RelationshipList]:
        """Create one or more relationships.

        Args:
            relationship (Union[Relationship, List[Relationship]]): Relationship or list of relationships to create.
                Note: the source and target field in the Relationship(s) can be of the form shown below, or objects of type Asset, TimeSeries, FileMetadata, Event, Sequence

        Returns:
            Union[Relationship, RelationshipList]: Created relationship(s)

        Examples:

            Create a new relationship specifying object type and external id for source and target::

                >>> from cognite.client.beta import CogniteClient
                >>> from cognite.client.data_classes import Relationship
                >>> c = CogniteClient()
                >>> rel = Relationship(external_id="rel", source_external_id="source_ext_id", source_type="asset", target_external_id="target_ext_id", target_type="event", confidence=0.9, data_set_id={"id": "ds_name"})
                >>> res = c.relationships.create(rel)

            Create a new relationship using objects directly as source and target::

                >>> from cognite.client.beta import CogniteClient
                >>> from cognite.client.data_classes import Relationship
                >>> c = CogniteClient()
                >>> assets = c.assets.retrieve_multiple(id=[1,2,3])
                >>> flowrel1 = Relationship(external_id="flow_1", source_external_id="source_ext_id", source_type="asset", target_external_id="target_ext_id", target_type="event", confidence=0.1, data_set_id={"id": "ds_name"})
                >>> flowrel2 = Relationship(external_id="flow_2", source_external_id="source_ext_id", source_type="asset", target_external_id="target_ext_id", target_type="event", confidence=0.1, data_set_id={"id": "ds_name"})
                >>> res = c.relationships.create([flowrel1,flowrel2])
        """
        utils._auxiliary.assert_type(relationship, "relationship", [Relationship, list])
        if isinstance(relationship, list):
            relationship = [r._validate_resource_types() for r in relationship]
        else:
            relationship = relationship._validate_resource_types()

        return self._create_multiple(items=relationship)

    def delete(self, external_id: Union[str, List[str]]) -> None:
        """Delete one or more relationships

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
