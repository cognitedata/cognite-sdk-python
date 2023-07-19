from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, cast

from cognite.client.data_classes._base import (
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    PropertySpec,
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
        id: Optional[int] = None,
        external_id: Optional[str] = None,
        created_time: Optional[int] = None,
        last_updated_time: Optional[int] = None,
        interval: Optional[str] = None,
        is_paused: bool = False,
        cognite_client: Optional[CogniteClient] = None,
    ):
        self.id = id
        self.external_id = external_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.interval = interval
        self.is_paused = is_paused
        self._cognite_client = cast("CogniteClient", cognite_client)

    def __hash__(self) -> int:
        return hash(self.id)


class TransformationScheduleUpdate(CogniteUpdate):
    """Changes applied to transformation schedule

    Args:
        id (int): Transformation id.
        external_id (str): Transformation externalId.
    """

    class _PrimitiveTransformationScheduleUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> TransformationScheduleUpdate:
            return self._set(value)

    @property
    def interval(self) -> _PrimitiveTransformationScheduleUpdate:
        return TransformationScheduleUpdate._PrimitiveTransformationScheduleUpdate(self, "interval")

    @property
    def is_paused(self) -> _PrimitiveTransformationScheduleUpdate:
        return TransformationScheduleUpdate._PrimitiveTransformationScheduleUpdate(self, "isPaused")

    @classmethod
    def _get_update_properties(cls) -> list[PropertySpec]:
        return [
            PropertySpec("interval", is_nullable=False),
            PropertySpec("is_paused", is_nullable=False),
        ]


class TransformationScheduleList(CogniteResourceList[TransformationSchedule]):
    _RESOURCE = TransformationSchedule
