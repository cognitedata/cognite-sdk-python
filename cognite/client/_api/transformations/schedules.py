from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import TransformationSchedule, TransformationScheduleList, TransformationScheduleUpdate
from cognite.client.data_classes.transformations import TransformationFilter
from cognite.client.utils._identifier import IdentifierSequence


class TransformationSchedulesAPI(APIClient):
    _RESOURCE_PATH = "/transformations/schedules"
    _LIST_CLASS = TransformationScheduleList

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._CREATE_LIMIT = 5
        self._DELETE_LIMIT = 5
        self._UPDATE_LIMIT = 5

    def create(self, schedule):
        utils._auxiliary.assert_type(schedule, "schedule", [TransformationSchedule, list])
        return self._create_multiple(
            list_cls=TransformationScheduleList, resource_cls=TransformationSchedule, items=schedule
        )

    def retrieve(self, id=None, external_id=None):
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=TransformationScheduleList, resource_cls=TransformationSchedule, identifiers=identifiers
        )

    def retrieve_multiple(self, ids=None, external_ids=None, ignore_unknown_ids=False):
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=TransformationScheduleList,
            resource_cls=TransformationSchedule,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    def list(self, include_public=True, limit=25):
        filter = TransformationFilter(include_public=include_public).dump(camel_case=True)
        return self._list(
            list_cls=TransformationScheduleList,
            resource_cls=TransformationSchedule,
            method="GET",
            limit=limit,
            filter=filter,
        )

    def delete(self, id=None, external_id=None, ignore_unknown_ids=False):
        self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=id, external_ids=external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )

    def update(self, item):
        return self._update_multiple(
            list_cls=TransformationScheduleList,
            resource_cls=TransformationSchedule,
            update_cls=TransformationScheduleUpdate,
            items=item,
        )
