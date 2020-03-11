from typing import *

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import Relationship, RelationshipFilter, RelationshipList


class RelationshipsAPI(APIClient):
    _RESOURCE_PATH = "/relationships"
    _LIST_CLASS = RelationshipList

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._CREATE_LIMIT = 1000

    def _create_filter(
        self,
        source_resource: str = None,
        source_resource_id: str = None,
        sources: List[Dict[str, Any]] = None,
        target_resource: str = None,
        target_resource_id: str = None,
        targets: List[Dict[str, Any]] = None,
        start_time: Dict[str, Any] = None,
        end_time: Dict[str, Any] = None,
        confidence: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        created_time: Dict[str, Any] = None,
        data_set: Optional[Union[str, List[str]]] = None,
        relationship_type: Optional[Union[str, List[str]]] = None,
    ):
        if sources and (source_resource or source_resource_id):
            raise ValueError("Can not set both sources and source_resource/source_resource_id.")
        if targets and (target_resource_id or target_resource):
            raise ValueError("Can not set both targets and target_resource/target_resource_id.")
        if isinstance(relationship_type, str):
            relationship_type = [relationship_type]
        if isinstance(data_set, str):
            data_set = [data_set]

        if source_resource or source_resource_id:
            sources = [{"resource": source_resource, "resourceId": source_resource_id}]
        if target_resource or target_resource_id:
            targets = [{"resource": target_resource, "resourceId": target_resource_id}]
        if sources:  # remove keys with null values
            sources = [{k: v for k, v in source.items() if v is not None} for source in sources]
        if targets:
            targets = [{k: v for k, v in target.items() if v is not None} for target in targets]

        return RelationshipFilter(
            sources=sources,
            targets=targets,
            start_time=start_time,
            end_time=end_time,
            confidence=confidence,
            last_updated_time=last_updated_time,
            created_time=created_time,
            data_sets=data_set,
            relationship_types=relationship_type,
        ).dump(camel_case=True)

    def __call__(
        self,
        chunk_size: int = None,
        source_resource: str = None,
        source_resource_id: str = None,
        sources: List[Dict[str, Any]] = None,
        target_resource: str = None,
        target_resource_id: str = None,
        targets: List[Dict[str, Any]] = None,
        start_time: Dict[str, Any] = None,
        end_time: Dict[str, Any] = None,
        confidence: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        created_time: Dict[str, Any] = None,
        data_set: Optional[Union[str, List[str]]] = None,
        relationship_type: Optional[Union[str, List[str]]] = None,
        limit: int = None,
    ) -> Generator[Union[Relationship, RelationshipList], None, None]:
        """Iterate over relationships

        Fetches relationships as they are iterated over, so you keep a limited number of relationships in memory.

        Args:
            chunk_size (int, optional): Number of relationships to return in each chunk. Defaults to yielding one relationship a time.
            source_resource (str): Resource type of the source node.
            source_resource_id (str): Resource ID of the source node.
            sources (List[Dict[str, Any]]): List of multiple sources in the format `[{"resourceId":externalId,"resource":"Asset"},...]`
            target_resource (str): Resource type of the target node.
            target_resource_id (str): Resource ID of the target node.
            targets (List[Dict[str, Any]]): List of multiple targets in the format `[{"resourceId":externalId,"resource":"Asset"},...]`
            start_time (Dict[str, Any]): Range to filter the field for. (inclusive)
            end_time (Dict[str, Any]): Range to filter the field for. (inclusive)
            confidence (Dict[str, Any]): Range to filter the field for. (inclusive)
            last_updated_time (Dict[str, Any]): Range to filter the field for. (inclusive)
            created_time (Dict[str, Any]): Range to filter the field for. (inclusive)
            data_set (Union[str,List[str]): Filter on any of a given list of dataSets.
            relationship_type (Union[str,List[str]):  Filter on any of a given list o relationship types.
            limit (int, optional): Maximum number of relationships to return. Defaults to 100. Set to -1, float("inf") or None
                to return all items.

        Yields:
            Union[Relationship, RelationshipList]: yields Relationship one by one if chunk is not specified, else RelationshipList objects.
        """
        filter = self._create_filter(
            source_resource=source_resource,
            source_resource_id=source_resource_id,
            sources=sources,
            target_resource=target_resource,
            target_resource_id=target_resource_id,
            targets=targets,
            start_time=start_time,
            end_time=end_time,
            confidence=confidence,
            last_updated_time=last_updated_time,
            created_time=created_time,
            data_set=data_set,
            relationship_type=relationship_type,
        )
        return self._list_generator(method="POST", chunk_size=chunk_size, limit=limit, filter=filter)

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

                >>> from cognite.client.experimental import CogniteClient
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

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.relationships.retrieve_multiple(external_ids=["abc", "def"])
        """
        utils._auxiliary.assert_type(external_ids, "external_id", [List], allow_none=False)
        return self._retrieve_multiple(external_ids=external_ids, wrap_ids=True)

    def list(
        self,
        source_resource: str = None,
        source_resource_id: str = None,
        sources: List[Dict[str, Any]] = None,
        target_resource: str = None,
        target_resource_id: str = None,
        targets: List[Dict[str, Any]] = None,
        start_time: Dict[str, Any] = None,
        end_time: Dict[str, Any] = None,
        confidence: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        created_time: Dict[str, Any] = None,
        data_set: Optional[Union[str, List[str]]] = None,
        relationship_type: Optional[Union[str, List[str]]] = None,
        limit: int = 25,
    ) -> RelationshipList:
        """List relationships

        Args:
            source_resource (str): Resource type of the source node.
            source_resource_id (str): Resource ID of the source node.
            sources (List[Dict[str, Any]]): List of multiple sources in the format `[{"resourceId":externalId,"resource":"Asset"},...]`
            target_resource (str): Resource type of the target node.
            target_resource_id (str): Resource ID of the target node.
            targets (List[Dict[str, Any]]): List of multiple targets in the format `[{"resourceId":externalId,"resource":"Asset"},...]`
            start_time (Dict[str, Any]): Range to filter the field for. (inclusive)
            end_time (Dict[str, Any]): Range to filter the field for. (inclusive)
            confidence (Dict[str, Any]): Range to filter the field for. (inclusive)
            last_updated_time (Dict[str, Any]): Range to filter the field for. (inclusive)
            created_time (Dict[str, Any]): Range to filter the field for. (inclusive)
            data_set (Union[str,List[str]): Filter on any of a given list of dataSets.
            relationship_type (Union[str,List[str]):  Filter on any of a given list o relationship types.
            limit (int, optional): Maximum number of relationships to return. Defaults to 100. Set to -1, float("inf") or None
                to return all items.

        Returns:
            RelationshipList: List of requested relationships

        Examples:

            List relationships::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> relationship_list = c.relationships.list(limit=5)

            Iterate over relationships::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> for relationship in c.relationships:
                ...     relationship # do something with the relationship

            Iterate over chunks of relationships to reduce memory load::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> for relationship_list in c.relationships(chunk_size=2500):
                ...     relationship_list # do something with the relationships
        """
        filter = self._create_filter(
            source_resource=source_resource,
            source_resource_id=source_resource_id,
            sources=sources,
            target_resource=target_resource,
            target_resource_id=target_resource_id,
            targets=targets,
            start_time=start_time,
            end_time=end_time,
            confidence=confidence,
            last_updated_time=last_updated_time,
            created_time=created_time,
            data_set=data_set,
            relationship_type=relationship_type,
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

                >>> from cognite.client.experimental import CogniteClient
                >>> from cognite.client.data_classes import Relationship
                >>> c = CogniteClient()
                >>> rel = Relationship(external_id="rel",source={"resource":"TimeSeries", "resourceId": "ts"},target={"resource":"Asset", "resourceId": "a"},relationship_type="belongsTo",confidence=0.9,data_set="ds_name")
                >>> res = c.relationships.create(rel)

            Create a new relationship using objects directly as source and target::

                >>> from cognite.client.experimental import CogniteClient
                >>> from cognite.client.data_classes import Relationship
                >>> c = CogniteClient()
                >>> assets = c.assets.retrieve_multiple(id=[1,2,3])
                >>> flowrel1 = Relationship(external_id="flow_1",source=assets[0],target=assets[1] ,relationship_type="flowsTo",confidence=0.1,data_set="ds_flow")
                >>> flowrel2 = Relationship(external_id="flow_2",source=assets[1],target=assets[2] ,relationship_type="flowsTo",confidence=0.1,data_set="ds_flow")
                >>> res = c.relationships.create([flowrel1,flowrel2])
        """
        utils._auxiliary.assert_type(relationship, "relationship", [Relationship, list])
        if isinstance(relationship, list):
            relationship = [r._copy_resolve_targets() for r in relationship]
        else:
            relationship = relationship._copy_resolve_targets()

        return self._create_multiple(items=relationship)

    def delete(self, external_id: Union[str, List[str]]) -> None:
        """Delete one or more relationships

        Args:
            external_id (Union[str, List[str]]): External ID or list of external ids

        Returns:
            None

        Examples:

            Delete relationships by external id::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> c.relationships.delete(external_id=["a","b"])
        """
        self._delete_multiple(external_ids=external_id, wrap_ids=True)
