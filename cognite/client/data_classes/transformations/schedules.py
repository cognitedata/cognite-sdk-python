from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

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
        id (int | None): Transformation id.
        external_id (str | None): Transformation externalId.
        created_time (int | None): Time when the schedule was created.
        last_updated_time (int | None): Time when the schedule was last updated.
        interval (str | None): Cron expression describes when the function should be called. Use http://www.cronmaker.com to create a cron expression.
        is_paused (bool): If true, the transformation is not scheduled.
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        id: int | None = None,
        external_id: str | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        interval: str | None = None,
        is_paused: bool = False,
        cognite_client: CogniteClient | None = None,
    ) -> None:
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
