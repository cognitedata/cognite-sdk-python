"""
===============================================================================
13a7de2b1a61099448b86ddf1135519c
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.data_classes.simulators.filters import PropertySort
from cognite.client.data_classes.simulators.models import (
    SimulatorModelRevision,
    SimulatorModelRevisionDataList,
    SimulatorModelRevisionList,
    SimulatorModelRevisionWrite,
)
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncSimulatorModelRevisionsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def list(
        self,
        limit: int = DEFAULT_LIMIT_READ,
        sort: PropertySort | None = None,
        model_external_ids: str | SequenceNotStr[str] | None = None,
        all_versions: bool | None = None,
        created_time: TimestampRange | None = None,
        last_updated_time: TimestampRange | None = None,
    ) -> SimulatorModelRevisionList:
        """
        `Filter simulator model revisions <https://developer.cognite.com/api#tag/Simulator-Models/operation/filter_simulator_model_revisions_simulators_models_revisions_list_post>`_

        Retrieves a list of simulator model revisions that match the given criteria.

        Args:
            limit (int): Maximum number of results to return. Defaults to 25. Set to -1, float(“inf”) or None to return all items.
            sort (PropertySort | None): The criteria to sort by.
            model_external_ids (str | SequenceNotStr[str] | None): The external ids of the simulator models to filter by.
            all_versions (bool | None): If True, all versions of the simulator model revisions are returned. If False, only the latest version is returned.
            created_time (TimestampRange | None): Filter by created time.
            last_updated_time (TimestampRange | None): Filter by last updated time.

        Returns:
            SimulatorModelRevisionList: List of simulator model revisions

        Examples:
            List simulator model revisions:
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.simulators.models.revisions.list(limit=10)

            Specify filter and sort order:
                >>> from cognite.client.data_classes.simulators.filters import PropertySort
                >>> from cognite.client.data_classes.shared import TimestampRange
                >>> res = client.simulators.models.revisions.list(
                ...     model_external_ids=["model1", "model2"],
                ...     all_versions=True,
                ...     created_time=TimestampRange(min="1d-ago", max="now"),
                ...     last_updated_time=TimestampRange(min="1d-ago", max="now"),
                ...     sort=PropertySort(order="asc", property="createdTime"),
                ...     limit=10
                ... )
        """
        return run_sync(
            self.__async_client.simulators.models.revisions.list(
                limit=limit,
                sort=sort,
                model_external_ids=model_external_ids,
                all_versions=all_versions,
                created_time=created_time,
                last_updated_time=last_updated_time,
            )
        )

    @overload
    def retrieve(self, *, ids: int) -> SimulatorModelRevision | None: ...

    @overload
    def retrieve(self, *, external_ids: str) -> SimulatorModelRevision | None: ...

    @overload
    def retrieve(self, *, ids: Sequence[int]) -> SimulatorModelRevisionList: ...

    @overload
    def retrieve(self, *, external_ids: SequenceNotStr[str]) -> SimulatorModelRevisionList: ...

    def retrieve(
        self, *, ids: int | Sequence[int] | None = None, external_ids: str | SequenceNotStr[str] | None = None
    ) -> SimulatorModelRevision | SimulatorModelRevisionList | None:
        """
        `Retrieve simulator model revisions <https://developer.cognite.com/api#tag/Simulator-Models/operation/retrieve_simulator_model_revisions_simulators_models_revisions_byids_post>`_

        Retrieve one or more simulator model revisions by ID(s) or external ID(s).

        Args:
            ids (int | Sequence[int] | None): The ids of the simulator model revisions.
            external_ids (str | SequenceNotStr[str] | None): The external ids of the simulator model revisions.

        Returns:
            SimulatorModelRevision | SimulatorModelRevisionList | None: Requested simulator model revision(s)

        Examples:
            Get simulator model revision by id:
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.simulators.models.revisions.retrieve(ids=1)

            Get simulator model revision by external id:
                >>> res = client.simulators.models.revisions.retrieve(
                ...     external_ids="revision_external_id"
                ... )

            Get multiple simulator model revisions by ids:
                >>> res = client.simulators.models.revisions.retrieve(ids=[1,2])

            Get multiple simulator model revisions by external ids:
                >>> res = client.simulators.models.revisions.retrieve(
                ...     external_ids=["revision1", "revision2"]
                ... )
        """
        return run_sync(
            self.__async_client.simulators.models.revisions.retrieve(  # type: ignore [call-overload]
                ids=ids, external_ids=external_ids
            )
        )

    @overload
    def __call__(
        self,
        chunk_size: int,
        sort: PropertySort | None = None,
        model_external_ids: str | SequenceNotStr[str] | None = None,
        all_versions: bool | None = None,
        created_time: TimestampRange | None = None,
        last_updated_time: TimestampRange | None = None,
        limit: int | None = None,
    ) -> Iterator[SimulatorModelRevisionList]: ...

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        sort: PropertySort | None = None,
        model_external_ids: str | SequenceNotStr[str] | None = None,
        all_versions: bool | None = None,
        created_time: TimestampRange | None = None,
        last_updated_time: TimestampRange | None = None,
        limit: int | None = None,
    ) -> Iterator[SimulatorModelRevision]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        sort: PropertySort | None = None,
        model_external_ids: str | SequenceNotStr[str] | None = None,
        all_versions: bool | None = None,
        created_time: TimestampRange | None = None,
        last_updated_time: TimestampRange | None = None,
        limit: int | None = None,
    ) -> Iterator[SimulatorModelRevision] | Iterator[SimulatorModelRevisionList]:
        """
        Iterate over simulator simulator model revisions

        Fetches simulator model revisions as they are iterated over, so you keep a limited number of simulator model revisions in memory.

        Args:
            chunk_size (int | None): Number of simulator model revisions to return in each chunk. Defaults to yielding one simulator model revision a time.
            sort (PropertySort | None): The criteria to sort by.
            model_external_ids (str | SequenceNotStr[str] | None): The external ids of the simulator models to filter by.
            all_versions (bool | None): If True, all versions of the simulator model revisions are returned. If False, only the latest version is returned.
            created_time (TimestampRange | None): Filter by created time.
            last_updated_time (TimestampRange | None): Filter by last updated time.
            limit (int | None): Maximum number of results to return. Defaults to 25. Set to -1, float(“inf”) or None to return all items.

        Yields:
            SimulatorModelRevision | SimulatorModelRevisionList: yields SimulatorModelRevision one by one if chunk is not specified, else SimulatorModelRevisionList objects.
        """  # noqa: DOC404
        yield from SyncIterator(
            self.__async_client.simulators.models.revisions(
                chunk_size=chunk_size,
                sort=sort,
                model_external_ids=model_external_ids,
                all_versions=all_versions,
                created_time=created_time,
                last_updated_time=last_updated_time,
                limit=limit,
            )
        )  # type: ignore [misc]

    @overload
    def create(self, items: SimulatorModelRevisionWrite) -> SimulatorModelRevision: ...

    @overload
    def create(self, items: Sequence[SimulatorModelRevisionWrite]) -> SimulatorModelRevisionList: ...

    def create(
        self, items: SimulatorModelRevisionWrite | Sequence[SimulatorModelRevisionWrite]
    ) -> SimulatorModelRevision | SimulatorModelRevisionList:
        """
        `Create simulator model revisions <https://api-docs.cognite.com/20230101-beta/tag/Simulator-Models/operation/create_simulator_model_revision_simulators_models_revisions_post>`_

        Args:
            items (SimulatorModelRevisionWrite | Sequence[SimulatorModelRevisionWrite]): The model revision(s) to create.

        Returns:
            SimulatorModelRevision | SimulatorModelRevisionList: Created simulator model revision(s)

        Examples:
            Create new simulator model revisions:
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.simulators import SimulatorModelRevisionWrite, SimulatorModelDependencyFileId, SimulatorModelRevisionDependency
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> revisions = [
                ...     SimulatorModelRevisionWrite(
                ...         external_id="revision1",
                ...         file_id=1,
                ...         model_external_id="a_1",
                ...     ),
                ...     SimulatorModelRevisionWrite(
                ...         external_id="revision2",
                ...         file_id=2,
                ...         model_external_id="a_2",
                ...         external_dependencies = [
                ...             SimulatorModelRevisionDependency(
                ...                 file=SimulatorModelDependencyFileId(id=123),
                ...                 arguments={
                ...                     "fieldA": "value1",
                ...                     "fieldB": "value2",
                ...                 },
                ...             )
                ...         ]
                ...     ),
                ... ]
                >>> res = client.simulators.models.revisions.create(revisions)
        """
        return run_sync(self.__async_client.simulators.models.revisions.create(items=items))

    def retrieve_data(self, model_revision_external_id: str) -> SimulatorModelRevisionDataList:
        """
        `Filter simulator model revision data <https://api-docs.cognite.com/20230101-alpha/tag/Simulator-Models/operation/get_simulator_model_revision_data_by_id>`_

        Retrieves a list of simulator model revisions data that match the given criteria.

        Args:
            model_revision_external_id (str): The external id of the simulator model revision to filter by.
        Returns:
            SimulatorModelRevisionDataList: List of simulator model revision data

        Examples:
            List simulator model revision data:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.simulators.models.revisions.retrieve_data("model_revision_1")
        """
        return run_sync(
            self.__async_client.simulators.models.revisions.retrieve_data(
                model_revision_external_id=model_revision_external_id
            )
        )
