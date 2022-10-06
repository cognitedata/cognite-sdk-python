from typing import TYPE_CHECKING, Any, Dict, Union, cast

from cognite.client import utils
from cognite.client.data_classes._base import (
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient


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
        cognite_client: "CogniteClient" = None,
    ):
        self.id = id
        self.external_id = external_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.interval = interval
        self.is_paused = is_paused
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client: "CogniteClient" = None) -> "TransformationSchedule":
        instance = super(TransformationSchedule, cls)._load(resource, cognite_client)
        return instance

    def __hash__(self) -> int:
        return hash(self.id)

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            Dict[str, Any]: A dictionary representation of the instance.
        """
        ret = super().dump(camel_case=camel_case)

        if camel_case:
            return {utils._auxiliary.to_camel_case(key): value for key, value in ret.items()}
        return ret


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
    def interval(self) -> _PrimitiveTransformationScheduleUpdate:
        return TransformationScheduleUpdate._PrimitiveTransformationScheduleUpdate(self, "interval")

    @property
    def is_paused(self) -> _PrimitiveTransformationScheduleUpdate:
        return TransformationScheduleUpdate._PrimitiveTransformationScheduleUpdate(self, "isPaused")


class TransformationScheduleList(CogniteResourceList):
    _RESOURCE = TransformationSchedule
