from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Any, cast

from cognite.client.data_classes._base import (
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    IdTransformerMixin,
    PropertySpec,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.utils._auxiliary import exactly_one_is_not_none

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class TransformationScheduleCore(WriteableCogniteResource["TransformationScheduleWrite"], ABC):
    """The transformation schedules resource allows running recurrent transformations.

    Args:
        id (int | None): Transformation id.
        external_id (str | None): Transformation external id.
        interval (str | None): Cron expression controls when the transformation will be run. Use http://www.cronmaker.com to create one.
        is_paused (bool): If true, the transformation is not scheduled.
    """

    def __init__(
        self,
        id: int | None = None,
        external_id: str | None = None,
        interval: str | None = None,
        is_paused: bool = False,
    ) -> None:
        self.id = id
        self.external_id = external_id
        self.interval = interval
        self.is_paused = is_paused


class TransformationSchedule(TransformationScheduleCore):
    """The transformation schedules resource allows running recurrent transformations.

    Args:
        id (int | None): Transformation id.
        external_id (str | None): Transformation external id.
        created_time (int | None): Time when the schedule was created.
        last_updated_time (int | None): Time when the schedule was last updated.
        interval (str | None): Cron expression controls when the transformation will be run. Use http://www.cronmaker.com to create one.
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
        super().__init__(
            id=id,
            external_id=external_id,
            interval=interval,
            is_paused=is_paused,
        )
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cast("CogniteClient", cognite_client)

    def as_write(self) -> TransformationScheduleWrite:
        """Returns this TransformationSchedule as a TransformationScheduleWrite"""
        if self.interval is None:
            raise ValueError("interval is required to create a TransformationSchedule")
        id_, external_id = self.id, self.external_id
        if id_ is not None and external_id is not None:
            id_ = None
        return TransformationScheduleWrite(
            id=id_,
            external_id=external_id,
            interval=self.interval,
            is_paused=self.is_paused,
        )

    def __hash__(self) -> int:
        return hash(self.id)


class TransformationScheduleWrite(TransformationScheduleCore):
    """The transformation schedules resource allows running recurrent transformations.

    Args:
        interval (str): Cron expression controls when the transformation will be run. Use http://www.cronmaker.com to create one.
        id (int | None): Transformation id.
        external_id (str | None): Transformation external id.
        is_paused (bool): If true, the transformation is not scheduled.
    """

    def __init__(
        self,
        interval: str,
        id: int | None = None,
        external_id: str | None = None,
        is_paused: bool = False,
    ) -> None:
        super().__init__(
            id=id,
            external_id=external_id,
            interval=interval,
            is_paused=is_paused,
        )
        if not exactly_one_is_not_none(id, external_id):
            raise ValueError(f"Either id or external_id must be specified (but not both), got {id=} and {external_id=}")

    @classmethod
    def _load(
        cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None
    ) -> TransformationScheduleWrite:
        return cls(
            interval=resource["interval"],
            id=resource.get("id"),
            external_id=resource.get("externalId"),
            is_paused=resource.get("isPaused", False),
        )

    def as_write(self) -> TransformationScheduleWrite:
        """Returns this TransformationScheduleWrite instance."""
        return self


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
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("interval", is_nullable=False),
            PropertySpec("is_paused", is_nullable=False),
        ]


class TransformationScheduleWriteList(CogniteResourceList[TransformationScheduleWrite], IdTransformerMixin):
    _RESOURCE = TransformationScheduleWrite


class TransformationScheduleList(
    WriteableCogniteResourceList[TransformationScheduleWrite, TransformationSchedule], IdTransformerMixin
):
    _RESOURCE = TransformationSchedule

    def as_write(self) -> TransformationScheduleWriteList:
        return TransformationScheduleWriteList(
            [x.as_write() for x in self.data], cognite_client=self._get_cognite_client()
        )
