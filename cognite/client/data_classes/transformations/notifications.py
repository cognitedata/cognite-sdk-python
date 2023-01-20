from __future__ import annotations

from typing import TYPE_CHECKING, Optional, cast

from cognite.client.data_classes._base import CogniteFilter, CogniteResource, CogniteResourceList

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class TransformationNotification(CogniteResource):
    """The transformation notification resource allows configuring email alerts on events related to a transformation run.

    Args:
        id (int): A server-generated ID for the object.
        transformation_id (int): Transformation Id.
        transformation_external_id (str): Transformation external Id.
        destination (str): Email address where notifications should be sent.
        created_time (int): Time when the notification was created.
        last_updated_time (int): Time when the notification was last updated.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        id: int = None,
        transformation_id: int = None,
        transformation_external_id: str = None,
        destination: str = None,
        created_time: int = None,
        last_updated_time: int = None,
        cognite_client: CogniteClient = None,
    ):
        self.id = id
        self.transformation_id = transformation_id
        self.transformation_external_id = transformation_external_id
        self.destination = destination
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cast("CogniteClient", cognite_client)

    def __hash__(self) -> int:
        return hash(self.id)


class TransformationNotificationList(CogniteResourceList):
    _RESOURCE = TransformationNotification


class TransformationNotificationFilter(CogniteFilter):
    """

    Args:
        transformation_id (Optional[int]): Filter by transformation internal numeric ID.
        transformation_external_id (str): Filter by transformation externalId.
        destination (str): Filter by notification destination.
    """

    def __init__(
        self, transformation_id: Optional[int] = None, transformation_external_id: str = None, destination: str = None
    ):
        self.transformation_id = transformation_id
        self.transformation_external_id = transformation_external_id
        self.destination = destination
