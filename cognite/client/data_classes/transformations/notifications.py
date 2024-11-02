from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Any, cast

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteResourceList,
    InternalIdTransformerMixin,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.utils._auxiliary import exactly_one_is_not_none

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class TransformationNotificationCore(WriteableCogniteResource["TransformationNotificationWrite"], ABC):
    """The transformation notification resource allows configuring email alerts on events related to a transformation run.

    Args:
        destination (str | None): Email address where notifications should be sent.
    """

    def __init__(
        self,
        destination: str | None = None,
    ) -> None:
        self.destination = destination


class TransformationNotification(TransformationNotificationCore):
    """The transformation notification resource allows configuring email alerts on events related to a transformation run.
    This is the read format of a transformation notification.

    Args:
        id (int | None): A server-generated ID for the object.
        transformation_id (int | None): Transformation Id.
        transformation_external_id (str | None): Transformation external Id.
        destination (str | None): Email address where notifications should be sent.
        created_time (int | None): Time when the notification was created.
        last_updated_time (int | None): Time when the notification was last updated.
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        id: int | None = None,
        transformation_id: int | None = None,
        transformation_external_id: str | None = None,
        destination: str | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(destination)
        # id/created_time/last_updated_time are required when using the class to read,
        # but don't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        # TODO: In the next major version we can make these properties required in the constructor
        self.id: int = id  # type: ignore
        self.transformation_id: int = transformation_id  # type: ignore
        self.transformation_external_id: str = transformation_external_id  # type: ignore
        self.created_time: int = created_time  # type: ignore
        self.last_updated_time: int = last_updated_time  # type: ignore
        self._cognite_client = cast("CogniteClient", cognite_client)

    def __hash__(self) -> int:
        return hash(self.id)

    def as_write(self) -> TransformationNotificationWrite:
        """Returns this Asset in its writing format."""
        if self.destination is None:
            raise ValueError("The write format requires destination to be set")
        return TransformationNotificationWrite(
            destination=self.destination,
            transformation_id=self.transformation_id,
        )


class TransformationNotificationWrite(TransformationNotificationCore):
    """The transformation notification resource allows configuring email alerts on events related to a transformation run.
    This is the write format of a transformation notification.

    Args:
        destination (str): Email address where notifications should be sent.
        transformation_id (int | None): Transformation ID.
        transformation_external_id (str | None): Transformation external ID.
    """

    def __init__(
        self,
        destination: str,
        transformation_id: int | None = None,
        transformation_external_id: str | None = None,
    ) -> None:
        super().__init__(destination)
        if not exactly_one_is_not_none(transformation_id, transformation_external_id):
            raise ValueError("exactly one of transformation_id, transformation_external_id must be specified")
        self.transformation_id = transformation_id
        self.transformation_external_id = transformation_external_id

    def as_write(self) -> TransformationNotificationWrite:
        """Returns this TransformationNotification instance"""
        return self

    @classmethod
    def _load(
        cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None
    ) -> TransformationNotificationWrite:
        return cls(
            destination=resource["destination"],
            transformation_id=resource.get("transformationId"),
            transformation_external_id=resource.get("transformationExternalId"),
        )


class TransformationNotificationWriteList(CogniteResourceList[TransformationNotificationWrite]):
    _RESOURCE = TransformationNotificationWrite


class TransformationNotificationList(
    WriteableCogniteResourceList[TransformationNotificationWrite, TransformationNotification],
    InternalIdTransformerMixin,
):
    _RESOURCE = TransformationNotification

    def as_write(self) -> TransformationNotificationWriteList:
        """Returns this TransformationNotificationList instance"""
        return TransformationNotificationWriteList(
            [item.as_write() for item in self.data], cognite_client=self._get_cognite_client()
        )


class TransformationNotificationFilter(CogniteFilter):
    """TransformationNotificationFilter

    Args:
        transformation_id (int | None): Filter by transformation internal numeric ID.
        transformation_external_id (str | None): Filter by transformation externalId.
        destination (str | None): Filter by notification destination.
    """

    def __init__(
        self,
        transformation_id: int | None = None,
        transformation_external_id: str | None = None,
        destination: str | None = None,
    ) -> None:
        self.transformation_id = transformation_id
        self.transformation_external_id = transformation_external_id
        self.destination = destination
