"""
===============================================================================
27569e19ffd1c836ddbe8b968ab02c0c
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
    TransformationSchedule,
    TransformationScheduleList,
    TransformationScheduleUpdate,
    TransformationScheduleWrite,
)
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncTransformationSchedulesAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(
        self, chunk_size: None = None, include_public: bool = True, limit: int | None = None
    ) -> Iterator[TransformationSchedule]: ...

    @overload
    def __call__(
        self, chunk_size: int, include_public: bool = True, limit: int | None = None
    ) -> Iterator[TransformationScheduleList]: ...

    def __call__(
        self, chunk_size: int | None = None, include_public: bool = True, limit: int | None = None
    ) -> Iterator[TransformationSchedule | TransformationScheduleList]:
        """
        Iterate over transformation schedules

        Args:
            chunk_size (int | None): The number of schedules to return in each chunk. Defaults to yielding one schedule a time.
            include_public (bool):  Whether public transformations should be included in the results. (default true).
            limit (int | None):  Limits the number of results to be returned. Defaults to yielding all schedules.

        Yields:
            TransformationSchedule | TransformationScheduleList: Yields schedules one by one if chunk_size is None, otherwise yields lists of schedules.
        """
        yield from SyncIterator(
            self.__async_client.transformations.schedules(
                chunk_size=chunk_size, include_public=include_public, limit=limit
            )
        )  # type: ignore [misc]

    @overload
    def create(self, schedule: TransformationSchedule | TransformationScheduleWrite) -> TransformationSchedule: ...

    @overload
    def create(
        self, schedule: Sequence[TransformationSchedule] | Sequence[TransformationScheduleWrite]
    ) -> TransformationScheduleList: ...

    def create(
        self,
        schedule: TransformationSchedule
        | TransformationScheduleWrite
        | Sequence[TransformationSchedule]
        | Sequence[TransformationScheduleWrite],
    ) -> TransformationSchedule | TransformationScheduleList:
        """
        `Schedule the specified transformation with the specified configuration(s). <https://developer.cognite.com/api#tag/Transformation-Schedules/operation/createTransformationSchedules>`_

        Args:
            schedule (TransformationSchedule | TransformationScheduleWrite | Sequence[TransformationSchedule] | Sequence[TransformationScheduleWrite]): Configuration or list of configurations of the schedules to create.

        Returns:
            TransformationSchedule | TransformationScheduleList: Created schedule(s)

        Examples:

            Create new schedules:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TransformationScheduleWrite
                >>> client = CogniteClient()
                >>> schedules = [TransformationScheduleWrite(id = 1, interval = "0 * * * *"), TransformationScheduleWrite(external_id="transformation2", interval = "5 * * * *"))]
                >>> res = client.transformations.schedules.create(schedules)
        """
        return run_sync(self.__async_client.transformations.schedules.create(schedule=schedule))

    def retrieve(self, id: int | None = None, external_id: str | None = None) -> TransformationSchedule | None:
        """
        `Retrieve a single transformation schedule by the id or external id of its transformation. <https://developer.cognite.com/api#tag/Transformation-Schedules/operation/getTransformationSchedulesByIds>`_

        Args:
            id (int | None): transformation ID
            external_id (str | None): transformation External ID

        Returns:
            TransformationSchedule | None: Requested transformation schedule or None if it does not exist.

        Examples:

            Get transformation schedule by transformation id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.transformations.schedules.retrieve(id=1)

            Get transformation schedule by transformation external id:

                >>> res = client.transformations.schedules.retrieve(external_id="1")
        """
        return run_sync(self.__async_client.transformations.schedules.retrieve(id=id, external_id=external_id))

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> TransformationScheduleList:
        """
        `Retrieve multiple transformation schedules by the ids or external ids of the corresponding transformations. <https://developer.cognite.com/api#tag/Transformation-Schedules/operation/getTransformationSchedulesByIds>`_

        Args:
            ids (Sequence[int] | None): transformation IDs
            external_ids (SequenceNotStr[str] | None): transformation External IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            TransformationScheduleList: Requested transformation schedules.

        Examples:

            Get transformation schedules by transformation ids:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.transformations.schedules.retrieve_multiple(ids=[1, 2, 3])

            Get transformation schedules by transformation external ids:

                >>> res = client.transformations.schedules.retrieve_multiple(external_ids=["t1", "t2"])
        """
        return run_sync(
            self.__async_client.transformations.schedules.retrieve_multiple(
                ids=ids, external_ids=external_ids, ignore_unknown_ids=ignore_unknown_ids
            )
        )

    def list(self, include_public: bool = True, limit: int | None = DEFAULT_LIMIT_READ) -> TransformationScheduleList:
        """
        `List all transformation schedules. <https://developer.cognite.com/api#tag/Transformation-Schedules/operation/getTransformationSchedules>`_

        Args:
            include_public (bool): Whether public transformations should be included in the results. (default true).
            limit (int | None): Limits the number of results to be returned. To retrieve all results use limit=-1, default limit is 25.

        Returns:
            TransformationScheduleList: List of schedules

        Example:

            List schedules::

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> schedules_list = client.transformations.schedules.list()
        """
        return run_sync(self.__async_client.transformations.schedules.list(include_public=include_public, limit=limit))

    def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """
        `Unschedule one or more transformations <https://developer.cognite.com/api#tag/Transformation-Schedules/operation/deleteTransformationSchedules>`_

        Args:
            id (int | Sequence[int] | None): Id or list of ids
            external_id (str | SequenceNotStr[str] | None): External ID or list of external ids
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Examples:

            Delete schedules by id or external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.transformations.schedules.delete(id=[1,2,3], external_id="3")
        """
        return run_sync(
            self.__async_client.transformations.schedules.delete(
                id=id, external_id=external_id, ignore_unknown_ids=ignore_unknown_ids
            )
        )

    @overload
    def update(
        self,
        item: TransformationSchedule | TransformationScheduleWrite | TransformationScheduleUpdate,
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> TransformationSchedule: ...

    @overload
    def update(
        self,
        item: Sequence[TransformationSchedule | TransformationScheduleWrite | TransformationScheduleUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> TransformationScheduleList: ...

    def update(
        self,
        item: TransformationSchedule
        | TransformationScheduleWrite
        | TransformationScheduleUpdate
        | Sequence[TransformationSchedule | TransformationScheduleWrite | TransformationScheduleUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> TransformationSchedule | TransformationScheduleList:
        """
        `Update one or more transformation schedules <https://developer.cognite.com/api#tag/Transformation-Schedules/operation/updateTransformationSchedules>`_

        Args:
            item (TransformationSchedule | TransformationScheduleWrite | TransformationScheduleUpdate | Sequence[TransformationSchedule | TransformationScheduleWrite | TransformationScheduleUpdate]): Transformation schedule(s) to update
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (TransformationSchedule or -Write). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            TransformationSchedule | TransformationScheduleList: Updated transformation schedule(s)

        Examples:

            Update a transformation schedule that you have fetched. This will perform a full update of the schedule:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> transformation_schedule = client.transformations.schedules.retrieve(id=1)
                >>> transformation_schedule.is_paused = True
                >>> res = client.transformations.schedules.update(transformation_schedule)

            Perform a partial update on a transformation schedule, updating the interval and unpausing it:

                >>> from cognite.client.data_classes import TransformationScheduleUpdate
                >>> my_update = TransformationScheduleUpdate(id=1).interval.set("0 * * * *").is_paused.set(False)
                >>> res = client.transformations.schedules.update(my_update)
        """
        return run_sync(self.__async_client.transformations.schedules.update(item=item, mode=mode))
