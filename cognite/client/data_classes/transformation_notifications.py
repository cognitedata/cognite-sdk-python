from cognite.client.data_classes._base import *


class TransformationNotification(CogniteResource):
    """The transformation notification resource allows configuring email alerts on events related to a transformation run.

    Args:
        id (int): A server-generated ID for the object.
        config_id (int): Transformation Id.
        config_external_id (str): Transformation external Id.
        destination (str): Email address where notifications should be sent.
        created_time (int): Time when the notification was created.
        last_updated_time (int): Time when the notification was last updated.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        id: int = None,
        config_id: int = None,
        config_external_id: str = None,
        destination: str = None,
        created_time: int = None,
        last_updated_time: int = None,
        cognite_client=None,
    ):
        self.id = id
        self.config_id = config_id
        self.config_external_id = config_external_id
        self.destination = destination
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        instance = super(TransformationNotification, cls)._load(resource, cognite_client)
        return instance

    def __hash__(self):
        return hash(self.id)


class TransformationNotificationList(CogniteResourceList):
    _RESOURCE = TransformationNotification
    _ASSERT_CLASSES = False


class TransformationNotificationFilter(CogniteFilter):
    """No description.

    Args:
        config_id (Optional[int]): List only notifications for the specified transformation. The transformation is identified by internal numeric ID.
        config_external_id (str): List only notifications for the specified transformation. The transformation is identified by externalId.
    """

    def __init__(self, config_id: Optional[int] = None, config_external_id: str = None):
        self.config_id = config_id
        self.config_external_id = config_external_id
