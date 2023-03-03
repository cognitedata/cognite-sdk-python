from cognite.client.data_classes._base import CogniteFilter, CogniteResource, CogniteResourceList

if TYPE_CHECKING:
    pass


class TransformationNotification(CogniteResource):
    def __init__(
        self,
        id=None,
        transformation_id=None,
        transformation_external_id=None,
        destination=None,
        created_time=None,
        last_updated_time=None,
        cognite_client=None,
    ):
        self.id = id
        self.transformation_id = transformation_id
        self.transformation_external_id = transformation_external_id
        self.destination = destination
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cast("CogniteClient", cognite_client)

    def __hash__(self):
        return hash(self.id)


class TransformationNotificationList(CogniteResourceList):
    _RESOURCE = TransformationNotification


class TransformationNotificationFilter(CogniteFilter):
    def __init__(self, transformation_id=None, transformation_external_id=None, destination=None):
        self.transformation_id = transformation_id
        self.transformation_external_id = transformation_external_id
        self.destination = destination
