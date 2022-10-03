from typing import Any, Optional, Sequence, Union

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import TransformationSchedule, TransformationScheduleList, TransformationScheduleUpdate
from cognite.client.data_classes.transformations import TransformationFilter
from cognite.client.utils._identifier import IdentifierSequence


class TransformationSchedulesAPI(APIClient):
    _RESOURCE_PATH = "/transformations/schedules"
    _LIST_CLASS = TransformationScheduleList

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self._CREATE_LIMIT = 5
        self._DELETE_LIMIT = 5
        self._UPDATE_LIMIT = 5

    def create(
        self, schedule: Union[TransformationSchedule, Sequence[TransformationSchedule]]
    ) -> Union[TransformationSchedule, TransformationScheduleList]:
        """`Schedule the specified transformation with the specified configuration(s). <https://docs.cognite.com/api/v1/#operation/createTransformationSchedules>`_

        Args:
            schedule (Union[TransformationSchedule, Sequence[TransformationSchedule]]): Configuration or list of configurations of the schedules to create.

        Returns:
            Created schedule(s)

        Examples:

            Create new schedules:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TransformationSchedule
                >>> c = CogniteClient()
                >>> schedules = [TransformationSchedule(id = 1, interval = "0 * * * *"), TransformationSchedule(external_id="transformation2", interval = "5 * * * *"))]
                >>> res = c.transformations.schedules.create(schedules)
        """
        utils._auxiliary.assert_type(schedule, "schedule", [TransformationSchedule, list])
        return self._create_multiple(
            list_cls=TransformationScheduleList, resource_cls=TransformationSchedule, items=schedule
        )

    def retrieve(self, id: Optional[int] = None, external_id: Optional[str] = None) -> Optional[TransformationSchedule]:
        """`Retrieve a single transformation schedule by the id or external id of its transformation. <https://docs.cognite.com/api/v1/#operation/getTransformationSchedulesByIds>`_

        Args:
            id (int, optional): transformation ID
            external_id (str, optional): transformation External ID

        Returns:
            Optional[TransformationSchedule]: Requested transformation schedule or None if it does not exist.

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
        ids: Optional[Sequence[int]] = None,
        external_ids: Optional[Sequence[str]] = None,
        ignore_unknown_ids: bool = False,
    ) -> TransformationScheduleList:
        """`Retrieve multiple transformation schedules by the ids or external ids of the corresponding transformations. <https://docs.cognite.com/api/v1/#operation/getTransformationSchedulesByIds>`_

        Args:
            ids (int, optional): transformation IDs
            external_ids (str, optional): transformation External IDs
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

    def list(self, include_public: bool = True, limit: Optional[int] = 25) -> TransformationScheduleList:
        """`List all transformation schedules. <https://docs.cognite.com/api/v1/#operation/getTransformationSchedules>`_

        Args:
            include_public (bool): Whether public transformations should be included in the results. (default true).
            cursor (str): Cursor for paging through results.
            limit (int): Limits the number of results to be returned. To retrieve all results use limit=-1, default limit is 25.

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
        id: Union[int, Sequence[int]] = None,
        external_id: Union[str, Sequence[str]] = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Unschedule one or more transformations <https://docs.cognite.com/api/v1/#operation/deleteTransformationSchedules>`_

        Args:
            id (Union[int, Sequence[int]): Id or list of ids
            external_id (Union[str, Sequence[str]]): External ID or list of external ids
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            None

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
        item: Union[
            TransformationSchedule,
            TransformationScheduleUpdate,
            Sequence[Union[TransformationSchedule, TransformationScheduleUpdate]],
        ],
    ) -> Union[TransformationSchedule, TransformationScheduleList]:
        """`Update one or more transformation schedules <https://docs.cognite.com/api/v1/#operation/updateTransformationSchedules>`_

        Args:
            item (Union[TransformationSchedule, TransformationScheduleUpdate, Sequence[Union[TransformationSchedule, TransformationScheduleUpdate]]]): Transformation schedule(s) to update

        Returns:
            Union[TransformationSchedule, TransformationScheduleList]: Updated transformation schedule(s)

        Examples:

            Update a transformation schedule that you have fetched. This will perform a full update of the schedule::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> transformation_schedule = c.transformations.schedules.retrieve(id=1)
                >>> transformation_schedule.is_paused = True
                >>> res = c.transformations.update(transformation)

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
