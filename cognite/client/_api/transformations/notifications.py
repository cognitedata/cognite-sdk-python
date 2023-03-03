
from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import TransformationNotification, TransformationNotificationList
from cognite.client.data_classes.transformations.notifications import TransformationNotificationFilter
from cognite.client.utils._identifier import IdentifierSequence

class TransformationNotificationsAPI(APIClient):
    _RESOURCE_PATH = '/transformations/notifications'
    _LIST_CLASS = TransformationNotificationList

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

    def create(self, notification):
        utils._auxiliary.assert_type(notification, 'notification', [TransformationNotification, Sequence])
        return self._create_multiple(list_cls=TransformationNotificationList, resource_cls=TransformationNotification, items=notification)

    def list(self, transformation_id=None, transformation_external_id=None, destination=None, limit=25):
        filter = TransformationNotificationFilter(transformation_id=transformation_id, transformation_external_id=transformation_external_id, destination=destination).dump(camel_case=True)
        return self._list(list_cls=TransformationNotificationList, resource_cls=TransformationNotification, method='GET', limit=limit, filter=filter)

    def delete(self, id=None):
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id), wrap_ids=True)
