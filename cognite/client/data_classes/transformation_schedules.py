from cognite.client.data_classes._base import *


class TransformationSchedule(CogniteResource):
    """The transformation schedules resource allows running recurrent transformations.

    Args:
        id (int): Transformation id.
        external_id (str): Transformation externalId.
        created_time (int): Time when the schedule was created.
        last_updated_time (int): Time when the schedule was last updated.
        interval (str): Cron expression describes when the function should be called. Use http://www.cronmaker.com to create a cron expression.
        is_paused (bool): If true, the transformation is not scheduled.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        id: int = None,
        external_id: str = None,
        created_time: int = None,
        last_updated_time: int = None,
        interval: str = None,
        is_paused: bool = False,
        cognite_client=None,
    ):
        self.id = id
        self.external_id = external_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.interval = interval
        self.is_paused = is_paused
        self.cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        instance = super(TransformationSchedule, cls)._load(resource, cognite_client)
        return instance

    def __hash__(self):
        return hash(self.id)


class TransformationScheduleUpdate(CogniteUpdate):
    """Changes applied to transformation schedule

    Args:
        id (int): Transformation id.
        external_id (str): Transformation externalId.
    """

    class _PrimitiveTransformationScheduleUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> "TransformationScheduleUpdate":
            return self._set(value)

    @property
    def interval(self):
        return TransformationScheduleUpdate._PrimitiveTransformationScheduleUpdate(self, "interval")

    @property
    def is_paused(self):
        return TransformationScheduleUpdate._PrimitiveTransformationScheduleUpdate(self, "isPaused")


class TransformationScheduleList(CogniteResourceList):
    _RESOURCE = TransformationSchedule
    _UPDATE = TransformationScheduleUpdate
