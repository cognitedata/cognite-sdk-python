from __future__ import annotations

import itertools
import warnings
from collections.abc import Iterator, Sequence
from functools import partial
from typing import TYPE_CHECKING, Literal, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    Relationship,
    RelationshipFilter,
    RelationshipList,
    RelationshipUpdate,
    RelationshipWrite,
)
from cognite.client.data_classes.labels import LabelFilter
from cognite.client.data_classes.relationships import RelationshipCore
from cognite.client.utils._auxiliary import is_unlimited, split_into_chunks
from cognite.client.utils._concurrency import execute_tasks
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._validation import assert_type, process_data_set_ids
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class RelationshipsAPI(APIClient):
    _RESOURCE_PATH = "/relationships"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._LIST_SUBQUERY_LIMIT = 1000

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
        partitions: int | None = None,
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
        partitions: int | None = None,
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
        partitions: int | None = None,
    ) -> Iterator[Relationship] | Iterator[RelationshipList]:
        """Iterate over relationships

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
            partitions (int | None): Retrieve relationships in parallel using this number of workers. Also requires `limit=None` to be passed.

        Returns:
            Iterator[Relationship] | Iterator[RelationshipList]: yields Relationship one by one if chunk_size is not specified, else RelationshipList objects.
        """
        data_set_ids_processed = process_data_set_ids(data_set_ids, data_set_external_ids)
        filter = RelationshipFilter(
            source_external_ids=source_external_ids,
            source_types=source_types,
            target_external_ids=target_external_ids,
            target_types=target_types,
            data_set_ids=data_set_ids_processed,
            start_time=start_time,
            end_time=end_time,
            confidence=confidence,
            last_updated_time=last_updated_time,
            created_time=created_time,
            active_at_time=active_at_time,
            labels=labels,
        ).dump(camel_case=True)
        n_target_xids, n_source_xids = len(target_external_ids or []), len(source_external_ids or [])
        if n_target_xids > self._LIST_SUBQUERY_LIMIT or n_source_xids > self._LIST_SUBQUERY_LIMIT:
            raise ValueError(
                f"For queries with more than {self._LIST_SUBQUERY_LIMIT} source_external_ids or "
                "target_external_ids, only `list` is supported"
            )
        return self._list_generator(
            list_cls=RelationshipList,
            resource_cls=Relationship,
            method="POST",
            limit=limit,
            filter=filter,
            chunk_size=chunk_size,
            partitions=partitions,
            other_params={"fetchResources": fetch_resources},
        )

    def __iter__(self) -> Iterator[Relationship]:
        """Iterate over relationships

        Fetches relationships as they are iterated over, so you keep a limited number of relationships in memory.

        Returns:
            Iterator[Relationship]: yields Relationships one by one.
        """
        return self()

    def retrieve(self, external_id: str, fetch_resources: bool = False) -> Relationship | None:
        """Retrieve a single relationship by external id.

        Args:
            external_id (str): External ID
            fetch_resources (bool): if true, will try to return the full resources referenced by the relationship in the source and target fields.

        Returns:
            Relationship | None: Requested relationship or None if it does not exist.

        Examples:

            Get relationship by external id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.relationships.retrieve(external_id="1")
        """
        identifiers = IdentifierSequence.load(ids=None, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=RelationshipList,
            resource_cls=Relationship,
            identifiers=identifiers,
            other_params={"fetchResources": fetch_resources},
        )

    def retrieve_multiple(
        self, external_ids: SequenceNotStr[str], fetch_resources: bool = False, ignore_unknown_ids: bool = False
    ) -> RelationshipList:
        """`Retrieve multiple relationships by external id.  <https://developer.cognite.com/api#tag/Relationships/operation/byidsRelationships>`_

        Args:
            external_ids (SequenceNotStr[str]): External IDs
            fetch_resources (bool): if true, will try to return the full resources referenced by the relationship in the
                source and target fields.
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            RelationshipList: The requested relationships.

        Examples:

            Get relationships by external id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.relationships.retrieve_multiple(external_ids=["abc", "def"])
        """
        identifiers = IdentifierSequence.load(ids=None, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=RelationshipList,
            resource_cls=Relationship,
            identifiers=identifiers,
            other_params={"fetchResources": fetch_resources},
            ignore_unknown_ids=ignore_unknown_ids,
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
        """`Lists relationships stored in the project based on a query filter given in the payload of this request. Up to 1000 relationships can be retrieved in one operation.  <https://developer.cognite.com/api#tag/Relationships/operation/listRelationships>`_

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

            List relationships::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> relationship_list = client.relationships.list(limit=5)

            Iterate over relationships::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for relationship in client.relationships:
                ...     relationship # do something with the relationship
        """
        data_set_ids_processed = process_data_set_ids(data_set_ids, data_set_external_ids)
        filter = RelationshipFilter(
            source_external_ids=source_external_ids,
            source_types=source_types,
            target_external_ids=target_external_ids,
            target_types=target_types,
            data_set_ids=data_set_ids_processed,
            start_time=start_time,
            end_time=end_time,
            confidence=confidence,
            last_updated_time=last_updated_time,
            created_time=created_time,
            active_at_time=active_at_time,
            labels=labels,
        ).dump(camel_case=True)

        target_external_ids, source_external_ids = target_external_ids or [], source_external_ids or []
        if all(len(xids) <= self._LIST_SUBQUERY_LIMIT for xids in (target_external_ids, source_external_ids)):
            return self._list(
                list_cls=RelationshipList,
                resource_cls=Relationship,
                method="POST",
                limit=limit,
                filter=filter,
                partitions=partitions,
                other_params={"fetchResources": fetch_resources},
            )
        if not is_unlimited(limit):
            raise ValueError(
                f"Querying more than {self._LIST_SUBQUERY_LIMIT} source_external_ids/target_external_ids is only "
                f"supported for unlimited queries (pass -1 / None / inf instead of {limit})"
            )
        tasks = []
        target_chunks = split_into_chunks(target_external_ids, self._LIST_SUBQUERY_LIMIT) or [[]]
        source_chunks = split_into_chunks(source_external_ids, self._LIST_SUBQUERY_LIMIT) or [[]]

        # All sources (if any) must be checked against all targets (if any). When either is not
        # given, we must exhaustively list all matching just the source or the target:
        for target_xids, source_xids in itertools.product(target_chunks, source_chunks):
            task_filter = filter.copy()
            if target_external_ids:  # keep null if it was
                task_filter["targetExternalIds"] = target_xids
            if source_external_ids:
                task_filter["sourceExternalIds"] = source_xids
            tasks.append({"filter": task_filter})

        if partitions is not None:
            warnings.warn(
                f"When one or both of source/target external IDs have more than {self._LIST_SUBQUERY_LIMIT} "
                "elements, `partitions` is ignored",
                UserWarning,
            )
        tasks_summary = execute_tasks(
            partial(
                self._list,
                list_cls=RelationshipList,
                resource_cls=Relationship,
                method="POST",
                limit=None,
                partitions=None,  # Otherwise, workers will spawn workers -> deadlock (singleton threadpool)
                other_params={"fetchResources": fetch_resources},
            ),
            tasks,
            max_workers=self._config.max_workers,
        )
        tasks_summary.raise_compound_exception_if_failed_tasks()
        return RelationshipList(tasks_summary.joined_results(), cognite_client=self._cognite_client)

    @overload
    def create(self, relationship: Relationship | RelationshipWrite) -> Relationship: ...

    @overload
    def create(self, relationship: Sequence[Relationship | RelationshipWrite]) -> RelationshipList: ...

    def create(
        self, relationship: Relationship | RelationshipWrite | Sequence[Relationship | RelationshipWrite]
    ) -> Relationship | RelationshipList:
        """`Create one or more relationships. <https://developer.cognite.com/api#tag/Relationships/operation/createRelationships>`_

        Args:
            relationship (Relationship | RelationshipWrite | Sequence[Relationship | RelationshipWrite]): Relationship or list of relationships to create.

        Returns:
            Relationship | RelationshipList: Created relationship(s)

        Note:
            - The source_type and target_type field in the Relationship(s) can be any string among "Asset", "TimeSeries", "File", "Event", "Sequence";
            - Do not provide the value for the source and target arguments of the Relationship class, only source_external_id / source_type and target_external_id / target_type. These (source and target) are used as part of fetching actual resources specified in other fields.

        Examples:

            Create a new relationship specifying object type and external id for source and target::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import Relationship
                >>> client = CogniteClient()
                >>> flowrel1 = Relationship(
                ...     external_id="flow_1",
                ...     source_external_id="source_ext_id",
                ...     source_type="asset",
                ...     target_external_id="target_ext_id",
                ...     target_type="event",
                ...     confidence=0.1,
                ...     data_set_id=1234
                ... )
                >>> flowrel2 = Relationship(
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
        assert_type(relationship, "relationship", [RelationshipCore, Sequence])
        if isinstance(relationship, Sequence):
            relationship = [r._validate_resource_types() for r in relationship]
        else:
            relationship = relationship._validate_resource_types()

        return self._create_multiple(
            list_cls=RelationshipList,
            resource_cls=Relationship,
            items=relationship,
            input_resource_cls=RelationshipWrite,
        )

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
        """`Update one or more relationships <https://developer.cognite.com/api#tag/Relationships/operation/updateRelationships>`_
        Currently, a full replacement of labels on a relationship is not supported (only partial add/remove updates). See the example below on how to perform partial labels update.

        Args:
            item (Relationship | RelationshipWrite | RelationshipUpdate | Sequence[Relationship | RelationshipWrite | RelationshipUpdate]): Relationship(s) to update
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (Relationship or -Write). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            Relationship | RelationshipList: Updated relationship(s)

        Examples:
            Update a data set that you have fetched. This will perform a full update of the data set::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> rel = client.relationships.retrieve(external_id="flow1")
                >>> rel.confidence = 0.75
                >>> res = client.relationships.update(rel)

            Perform a partial update on a relationship, setting a source_external_id and a confidence::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import RelationshipUpdate
                >>> client = CogniteClient()
                >>> my_update = RelationshipUpdate(external_id="flow_1").source_external_id.set("alternate_source").confidence.set(0.97)
                >>> res1 = client.relationships.update(my_update)
                >>> # Remove an already set optional field like so
                >>> another_update = RelationshipUpdate(external_id="flow_1").confidence.set(None)
                >>> res2 = client.relationships.update(another_update)

            Attach labels to a relationship::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import RelationshipUpdate
                >>> client = CogniteClient()
                >>> my_update = RelationshipUpdate(external_id="flow_1").labels.add(["PUMP", "VERIFIED"])
                >>> res = client.relationships.update(my_update)

            Detach a single label from a relationship::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import RelationshipUpdate
                >>> client = CogniteClient()
                >>> my_update = RelationshipUpdate(external_id="flow_1").labels.remove("PUMP")
                >>> res = client.relationships.update(my_update)
        """
        return self._update_multiple(
            list_cls=RelationshipList, resource_cls=Relationship, update_cls=RelationshipUpdate, items=item, mode=mode
        )

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
        """Upsert relationships, i.e., update if it exists, and create if it does not exist.
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
                >>> from cognite.client.data_classes import Relationship
                >>> client = CogniteClient()
                >>> existing_relationship = client.relationships.retrieve(id=1)
                >>> existing_relationship.description = "New description"
                >>> new_relationship = Relationship(external_id="new_relationship", source_external_id="new_source")
                >>> res = client.relationships.upsert([existing_relationship, new_relationship], mode="replace")
        """
        return self._upsert_multiple(
            item,
            list_cls=RelationshipList,
            resource_cls=Relationship,
            update_cls=RelationshipUpdate,
            input_resource_cls=Relationship,
            mode=mode,
        )

    def delete(self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> None:
        """`Delete one or more relationships. <https://developer.cognite.com/api#tag/Relationships/operation/deleteRelationships>`_

        Args:
            external_id (str | SequenceNotStr[str]): External ID or list of external ids
            ignore_unknown_ids (bool): Ignore external IDs that are not found rather than throw an exception.
        Examples:

            Delete relationships by external id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.relationships.delete(external_id=["a","b"])
        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )
