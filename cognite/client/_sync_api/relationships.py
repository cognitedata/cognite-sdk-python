"""
===============================================================================
dc8a46cd5a243f74255baac0d3c5a05c
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import (
    Relationship,
    RelationshipList,
    RelationshipUpdate,
    RelationshipWrite,
)
from cognite.client.data_classes.labels import LabelFilter
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncRelationshipsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        source_external_ids: SequenceNotStr[str] | None = None,
        source_types: SequenceNotStr[str] | None = None,
        target_external_ids: SequenceNotStr[str] | None = None,
        target_types: SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        start_time: dict[str, int] | None = None,
        end_time: dict[str, int] | None = None,
        confidence: dict[str, int] | None = None,
        last_updated_time: dict[str, int] | None = None,
        created_time: dict[str, int] | None = None,
        active_at_time: dict[str, int] | None = None,
        labels: LabelFilter | None = None,
        limit: int | None = None,
        fetch_resources: bool = False,
    ) -> Iterator[Relationship]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        source_external_ids: SequenceNotStr[str] | None = None,
        source_types: SequenceNotStr[str] | None = None,
        target_external_ids: SequenceNotStr[str] | None = None,
        target_types: SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        start_time: dict[str, int] | None = None,
        end_time: dict[str, int] | None = None,
        confidence: dict[str, int] | None = None,
        last_updated_time: dict[str, int] | None = None,
        created_time: dict[str, int] | None = None,
        active_at_time: dict[str, int] | None = None,
        labels: LabelFilter | None = None,
        limit: int | None = None,
        fetch_resources: bool = False,
    ) -> Iterator[RelationshipList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        source_external_ids: SequenceNotStr[str] | None = None,
        source_types: SequenceNotStr[str] | None = None,
        target_external_ids: SequenceNotStr[str] | None = None,
        target_types: SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        start_time: dict[str, int] | None = None,
        end_time: dict[str, int] | None = None,
        confidence: dict[str, int] | None = None,
        last_updated_time: dict[str, int] | None = None,
        created_time: dict[str, int] | None = None,
        active_at_time: dict[str, int] | None = None,
        labels: LabelFilter | None = None,
        limit: int | None = None,
        fetch_resources: bool = False,
    ) -> Iterator[Relationship] | Iterator[RelationshipList]:
        """
        Iterate over relationships

        Fetches relationships as they are iterated over, so you keep a limited number of relationships in memory.

        Args:
            chunk_size (int | None): Number of Relationships to return in each chunk. Defaults to yielding one relationship at a time.
            source_external_ids (SequenceNotStr[str] | None): Include relationships that have any of these values in their source External Id field
            source_types (SequenceNotStr[str] | None): Include relationships that have any of these values in their source Type field
            target_external_ids (SequenceNotStr[str] | None): Include relationships that have any of these values in their target External Id field
            target_types (SequenceNotStr[str] | None): Include relationships that have any of these values in their target Type field
            data_set_ids (int | Sequence[int] | None): Return only relationships in the specified data set(s) with this id / these ids.
            data_set_external_ids (str | SequenceNotStr[str] | None): Return only relationships in the specified data set(s) with this external id / these external ids.
            start_time (dict[str, int] | None): Range between two timestamps, minimum and maximum milliseconds (inclusive)
            end_time (dict[str, int] | None): Range between two timestamps, minimum and maximum milliseconds (inclusive)
            confidence (dict[str, int] | None): Range to filter the field for (inclusive).
            last_updated_time (dict[str, int] | None): Range to filter the field for (inclusive).
            created_time (dict[str, int] | None): Range to filter the field for (inclusive).
            active_at_time (dict[str, int] | None): Limits results to those active at any point within the given time range, i.e. if there is any overlap in the intervals [activeAtTime.min, activeAtTime.max] and [startTime, endTime], where both intervals are inclusive. If a relationship does not have a startTime, it is regarded as active from the beginning of time by this filter. If it does not have an endTime is will be regarded as active until the end of time. Similarly, if a min is not supplied to the filter, the min will be implicitly set to the beginning of time, and if a max is not supplied, the max will be implicitly set to the end of time.
            labels (LabelFilter | None): Return only the resource matching the specified label constraints.
            limit (int | None): No description.
            fetch_resources (bool): No description.

        Yields:
            Relationship | RelationshipList: yields Relationship one by one if chunk_size is not specified, else RelationshipList objects.
        """  # noqa: DOC404
        yield from SyncIterator(
            self.__async_client.relationships(
                chunk_size=chunk_size,
                source_external_ids=source_external_ids,
                source_types=source_types,
                target_external_ids=target_external_ids,
                target_types=target_types,
                data_set_ids=data_set_ids,
                data_set_external_ids=data_set_external_ids,
                start_time=start_time,
                end_time=end_time,
                confidence=confidence,
                last_updated_time=last_updated_time,
                created_time=created_time,
                active_at_time=active_at_time,
                labels=labels,
                limit=limit,
                fetch_resources=fetch_resources,
            )
        )  # type: ignore [misc]

    def retrieve(self, external_id: str, fetch_resources: bool = False) -> Relationship | None:
        """
        Retrieve a single relationship by external id.

        Args:
            external_id (str): External ID
            fetch_resources (bool): If true, will try to return the full resources referenced by the relationship in the source and target fields.

        Returns:
            Relationship | None: Requested relationship or None if it does not exist.

        Examples:

            Get relationship by external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.relationships.retrieve(external_id="1")
        """
        return run_sync(
            self.__async_client.relationships.retrieve(external_id=external_id, fetch_resources=fetch_resources)
        )

    def retrieve_multiple(
        self, external_ids: SequenceNotStr[str], fetch_resources: bool = False, ignore_unknown_ids: bool = False
    ) -> RelationshipList:
        """
        `Retrieve multiple relationships by external id.  <https://api-docs.cognite.com/20230101/tag/Relationships/operation/byidsRelationships>`_

        Args:
            external_ids (SequenceNotStr[str]): External IDs
            fetch_resources (bool): If true, will try to return the full resources referenced by the relationship in the
                source and target fields.
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            RelationshipList: The requested relationships.

        Examples:

            Get relationships by external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.relationships.retrieve_multiple(external_ids=["abc", "def"])
        """
        return run_sync(
            self.__async_client.relationships.retrieve_multiple(
                external_ids=external_ids, fetch_resources=fetch_resources, ignore_unknown_ids=ignore_unknown_ids
            )
        )

    def list(
        self,
        source_external_ids: SequenceNotStr[str] | None = None,
        source_types: SequenceNotStr[str] | None = None,
        target_external_ids: SequenceNotStr[str] | None = None,
        target_types: SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        start_time: dict[str, int] | None = None,
        end_time: dict[str, int] | None = None,
        confidence: dict[str, int] | None = None,
        last_updated_time: dict[str, int] | None = None,
        created_time: dict[str, int] | None = None,
        active_at_time: dict[str, int] | None = None,
        labels: LabelFilter | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        partitions: int | None = None,
        fetch_resources: bool = False,
    ) -> RelationshipList:
        """
        `Lists relationships stored in the project based on a query filter given in the payload of this request. Up to 1000 relationships can be retrieved in one operation.  <https://api-docs.cognite.com/20230101/tag/Relationships/operation/listRelationships>`_

        Args:
            source_external_ids (SequenceNotStr[str] | None): Include relationships that have any of these values in their source External Id field
            source_types (SequenceNotStr[str] | None): Include relationships that have any of these values in their source Type field
            target_external_ids (SequenceNotStr[str] | None): Include relationships that have any of these values in their target External Id field
            target_types (SequenceNotStr[str] | None): Include relationships that have any of these values in their target Type field
            data_set_ids (int | Sequence[int] | None): Return only relationships in the specified data set(s) with this id / these ids.
            data_set_external_ids (str | SequenceNotStr[str] | None): Return only relationships in the specified data set(s) with this external id / these external ids.
            start_time (dict[str, int] | None): Range between two timestamps, minimum and maximum milliseconds (inclusive)
            end_time (dict[str, int] | None): Range between two timestamps, minimum and maximum milliseconds (inclusive)
            confidence (dict[str, int] | None): Range to filter the field for (inclusive).
            last_updated_time (dict[str, int] | None): Range to filter the field for (inclusive).
            created_time (dict[str, int] | None): Range to filter the field for (inclusive).
            active_at_time (dict[str, int] | None): Limits results to those active at any point within the given time range, i.e. if there is any overlap in the intervals [activeAtTime.min, activeAtTime.max] and [startTime, endTime], where both intervals are inclusive. If a relationship does not have a startTime, it is regarded as active from the beginning of time by this filter. If it does not have an endTime is will be regarded as active until the end of time. Similarly, if a min is not supplied to the filter, the min will be implicitly set to the beginning of time, and if a max is not supplied, the max will be implicitly set to the end of time.
            labels (LabelFilter | None): Return only the resource matching the specified label constraints.
            limit (int | None): Maximum number of relationships to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            partitions (int | None): Retrieve relationships in parallel using this number of workers. Also requires `limit=None` to be passed.
            fetch_resources (bool): if true, will try to return the full resources referenced by the relationship in the source and target fields.

        Returns:
            RelationshipList: List of requested relationships

        Examples:

            List relationships:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> relationship_list = client.relationships.list(limit=5)

            Iterate over relationships, one-by-one:

                >>> for relationship in client.relationships():
                ...     relationship  # do something with the relationship
        """
        return run_sync(
            self.__async_client.relationships.list(
                source_external_ids=source_external_ids,
                source_types=source_types,
                target_external_ids=target_external_ids,
                target_types=target_types,
                data_set_ids=data_set_ids,
                data_set_external_ids=data_set_external_ids,
                start_time=start_time,
                end_time=end_time,
                confidence=confidence,
                last_updated_time=last_updated_time,
                created_time=created_time,
                active_at_time=active_at_time,
                labels=labels,
                limit=limit,
                partitions=partitions,
                fetch_resources=fetch_resources,
            )
        )

    @overload
    def create(self, relationship: Relationship | RelationshipWrite) -> Relationship: ...

    @overload
    def create(self, relationship: Sequence[Relationship | RelationshipWrite]) -> RelationshipList: ...

    def create(
        self, relationship: Relationship | RelationshipWrite | Sequence[Relationship | RelationshipWrite]
    ) -> Relationship | RelationshipList:
        """
        `Create one or more relationships. <https://api-docs.cognite.com/20230101/tag/Relationships/operation/createRelationships>`_

        Args:
            relationship (Relationship | RelationshipWrite | Sequence[Relationship | RelationshipWrite]): Relationship or list of relationships to create.

        Returns:
            Relationship | RelationshipList: Created relationship(s)

        Note:
            - The source_type and target_type field in the Relationship(s) can be any string among "Asset", "TimeSeries", "File", "Event", "Sequence".
            - Do not provide the value for the source and target arguments of the Relationship class, only source_external_id / source_type and target_external_id / target_type. These (source and target) are used as part of fetching actual resources specified in other fields.

        Examples:

            Create a new relationship specifying object type and external id for source and target:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import RelationshipWrite
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> flowrel1 = RelationshipWrite(
                ...     external_id="flow_1",
                ...     source_external_id="source_ext_id",
                ...     source_type="asset",
                ...     target_external_id="target_ext_id",
                ...     target_type="event",
                ...     confidence=0.1,
                ...     data_set_id=1234
                ... )
                >>> flowrel2 = RelationshipWrite(
                ...     external_id="flow_2",
                ...     source_external_id="source_ext_id",
                ...     source_type="asset",
                ...     target_external_id="target_ext_id",
                ...     target_type="event",
                ...     confidence=0.1,
                ...     data_set_id=1234
                ... )
                >>> res = client.relationships.create([flowrel1,flowrel2])
        """
        return run_sync(self.__async_client.relationships.create(relationship=relationship))

    @overload
    def update(self, item: Relationship | RelationshipWrite | RelationshipUpdate) -> Relationship: ...

    @overload
    def update(self, item: Sequence[Relationship | RelationshipWrite | RelationshipUpdate]) -> RelationshipList: ...

    def update(
        self,
        item: Relationship
        | RelationshipWrite
        | RelationshipUpdate
        | Sequence[Relationship | RelationshipWrite | RelationshipUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> Relationship | RelationshipList:
        """
        `Update one or more relationships <https://api-docs.cognite.com/20230101/tag/Relationships/operation/updateRelationships>`_
        Currently, a full replacement of labels on a relationship is not supported (only partial add/remove updates). See the example below on how to perform partial labels update.

        Args:
            item (Relationship | RelationshipWrite | RelationshipUpdate | Sequence[Relationship | RelationshipWrite | RelationshipUpdate]): Relationship(s) to update
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (Relationship or -Write). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            Relationship | RelationshipList: Updated relationship(s)

        Examples:
            Update a data set that you have fetched. This will perform a full update of the data set:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> rel = client.relationships.retrieve(external_id="flow1")
                >>> rel.confidence = 0.75
                >>> res = client.relationships.update(rel)

            Perform a partial update on a relationship, setting a source_external_id and a confidence:

                >>> from cognite.client.data_classes import RelationshipUpdate
                >>> my_update = RelationshipUpdate(external_id="flow_1").source_external_id.set("alternate_source").confidence.set(0.97)
                >>> res1 = client.relationships.update(my_update)
                >>> # Remove an already set optional field like so
                >>> another_update = RelationshipUpdate(external_id="flow_1").confidence.set(None)
                >>> res2 = client.relationships.update(another_update)

            Attach labels to a relationship:

                >>> from cognite.client.data_classes import RelationshipUpdate
                >>> my_update = RelationshipUpdate(external_id="flow_1").labels.add(["PUMP", "VERIFIED"])
                >>> res = client.relationships.update(my_update)

            Detach a single label from a relationship:

                >>> from cognite.client.data_classes import RelationshipUpdate
                >>> my_update = RelationshipUpdate(external_id="flow_1").labels.remove("PUMP")
                >>> res = client.relationships.update(my_update)
        """
        return run_sync(self.__async_client.relationships.update(item=item, mode=mode))  # type: ignore [call-overload]

    @overload
    def upsert(
        self, item: Sequence[Relationship | RelationshipWrite], mode: Literal["patch", "replace"] = "patch"
    ) -> RelationshipList: ...

    @overload
    def upsert(
        self, item: Relationship | RelationshipWrite, mode: Literal["patch", "replace"] = "patch"
    ) -> Relationship: ...

    def upsert(
        self,
        item: Relationship | RelationshipWrite | Sequence[Relationship | RelationshipWrite],
        mode: Literal["patch", "replace"] = "patch",
    ) -> Relationship | RelationshipList:
        """
        Upsert relationships, i.e., update if it exists, and create if it does not exist.
            Note this is a convenience method that handles the upserting for you by first calling update on all items,
            and if any of them fail because they do not exist, it will create them instead.

            For more details, see :ref:`appendix-upsert`.

        Args:
            item (Relationship | RelationshipWrite | Sequence[Relationship | RelationshipWrite]): Relationship or list of relationships to upsert.
            mode (Literal['patch', 'replace']): Whether to patch or replace in the case the relationships are existing. If you set 'patch', the call will only update fields with non-null values (default). Setting 'replace' will unset any fields that are not specified.

        Returns:
            Relationship | RelationshipList: The upserted relationship(s).

        Examples:

            Upsert for relationships:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import RelationshipWrite
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> existing_relationship = client.relationships.retrieve(external_id="foo")
                >>> existing_relationship.description = "New description"
                >>> new_relationship = RelationshipWrite(
                ...     external_id="new_relationship",
                ...     source_external_id="new_source",
                ...     source_type="asset",
                ...     target_external_id="new_target",
                ...     target_type="event"
                ... )
                >>> res = client.relationships.upsert([existing_relationship, new_relationship], mode="replace")
        """
        return run_sync(self.__async_client.relationships.upsert(item=item, mode=mode))

    def delete(self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> None:
        """
        `Delete one or more relationships. <https://api-docs.cognite.com/20230101/tag/Relationships/operation/deleteRelationships>`_

        Args:
            external_id (str | SequenceNotStr[str]): External ID or list of external ids
            ignore_unknown_ids (bool): Ignore external IDs that are not found rather than throw an exception.
        Examples:

            Delete relationships by external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.relationships.delete(external_id=["a","b"])
        """
        return run_sync(
            self.__async_client.relationships.delete(external_id=external_id, ignore_unknown_ids=ignore_unknown_ids)
        )
