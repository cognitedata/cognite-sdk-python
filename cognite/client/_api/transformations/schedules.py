from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import TransformationSchedule, TransformationScheduleList, TransformationScheduleUpdate
from cognite.client.data_classes.transformations import TransformationFilter
from cognite.client.utils._auxiliary import assert_type
from cognite.client.utils._identifier import IdentifierSequence

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class TransformationSchedulesAPI(APIClient):
    _RESOURCE_PATH = "/transformations/schedules"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._CREATE_LIMIT = 5
        self._DELETE_LIMIT = 5
        self._UPDATE_LIMIT = 5

    def create(
        self, schedule: TransformationSchedule | Sequence[TransformationSchedule]
    ) -> TransformationSchedule | TransformationScheduleList:
        """`Schedule the specified transformation with the specified configuration(s). <https://developer.cognite.com/api#tag/Transformation-Schedules/operation/createTransformationSchedules>`_

        Args:
            schedule (TransformationSchedule | Sequence[TransformationSchedule]): Configuration or list of configurations of the schedules to create.

        Returns:
            TransformationSchedule | TransformationScheduleList: Created schedule(s)

        Examples:

            Create new schedules:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TransformationSchedule
                >>> c = CogniteClient()
                >>> schedules = [TransformationSchedule(id = 1, interval = "0 * * * *"), TransformationSchedule(external_id="transformation2", interval = "5 * * * *"))]
                >>> res = c.transformations.schedules.create(schedules)
        """
        assert_type(schedule, "schedule", [TransformationSchedule, list])
        return self._create_multiple(
            list_cls=TransformationScheduleList, resource_cls=TransformationSchedule, items=schedule
        )

    def retrieve(self, id: int | None = None, external_id: str | None = None) -> TransformationSchedule | None:
        """`Retrieve a single transformation schedule by the id or external id of its transformation. <https://developer.cognite.com/api#tag/Transformation-Schedules/operation/getTransformationSchedulesByIds>`_

        Args:
            id (int | None): transformation ID
            external_id (str | None): transformation External ID

        Returns:
            TransformationSchedule | None: Requested transformation schedule or None if it does not exist.

        Examples:

            Get transformation schedule by transformation id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.transformations.schedules.retrieve(id=1)

            Get transformation schedule by transformation external id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.transformations.schedules.retrieve(external_id="1")
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=TransformationScheduleList, resource_cls=TransformationSchedule, identifiers=identifiers
        )

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: Sequence[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> TransformationScheduleList:
        """`Retrieve multiple transformation schedules by the ids or external ids of the corresponding transformations. <https://developer.cognite.com/api#tag/Transformation-Schedules/operation/getTransformationSchedulesByIds>`_

        Args:
            ids (Sequence[int] | None): transformation IDs
            external_ids (Sequence[str] | None): transformation External IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            TransformationScheduleList: Requested transformation schedules.

        Examples:

            Get transformation schedules by transformation ids:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.transformations.schedules.retrieve_multiple(ids=[1, 2, 3])

            Get transformation schedules by transformation external ids:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.transformations.schedules.retrieve_multiple(external_ids=["t1", "t2"])
        """
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=TransformationScheduleList,
            resource_cls=TransformationSchedule,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    def list(self, include_public: bool = True, limit: int | None = DEFAULT_LIMIT_READ) -> TransformationScheduleList:
        """`List all transformation schedules. <https://developer.cognite.com/api#tag/Transformation-Schedules/operation/getTransformationSchedules>`_

        Args:
            include_public (bool): Whether public transformations should be included in the results. (default true).
            limit (int | None): Limits the number of results to be returned. To retrieve all results use limit=-1, default limit is 25.

        Returns:
            TransformationScheduleList: List of schedules

        Example:

            List schedules::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> schedules_list = c.transformations.schedules.list()
        """
        filter = TransformationFilter(include_public=include_public).dump(camel_case=True)

        return self._list(
            list_cls=TransformationScheduleList,
            resource_cls=TransformationSchedule,
            method="GET",
            limit=limit,
            filter=filter,
        )

    def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | Sequence[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Unschedule one or more transformations <https://developer.cognite.com/api#tag/Transformation-Schedules/operation/deleteTransformationSchedules>`_

        Args:
            id (int | Sequence[int] | None): Id or list of ids
            external_id (str | Sequence[str] | None): External ID or list of external ids
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Examples:

            Delete schedules by id or external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.transformations.schedules.delete(id=[1,2,3], external_id="3")
        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=id, external_ids=external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )

    def update(
        self,
        item: TransformationSchedule
        | TransformationScheduleUpdate
        | Sequence[TransformationSchedule | TransformationScheduleUpdate],
    ) -> TransformationSchedule | TransformationScheduleList:
        """`Update one or more transformation schedules <https://developer.cognite.com/api#tag/Transformation-Schedules/operation/updateTransformationSchedules>`_

        Args:
            item (TransformationSchedule | TransformationScheduleUpdate | Sequence[TransformationSchedule | TransformationScheduleUpdate]): Transformation schedule(s) to update

        Returns:
            TransformationSchedule | TransformationScheduleList: Updated transformation schedule(s)

        Examples:

            Update a transformation schedule that you have fetched. This will perform a full update of the schedule::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> transformation_schedule = c.transformations.schedules.retrieve(id=1)
                >>> transformation_schedule.is_paused = True
                >>> res = c.transformations.schedules.update(transformation_schedule)

            Perform a partial update on a transformation schedule, updating the interval and unpausing it::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TransformationScheduleUpdate
                >>> c = CogniteClient()
                >>> my_update = TransformationScheduleUpdate(id=1).interval.set("0 * * * *").is_paused.set(False)
                >>> res = c.transformations.schedules.update(my_update)
        """
        return self._update_multiple(
            list_cls=TransformationScheduleList,
            resource_cls=TransformationSchedule,
            update_cls=TransformationScheduleUpdate,
            items=item,
        )
