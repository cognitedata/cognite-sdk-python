import random
import string

import pytest

from cognite.experimental import CogniteClient
from cognite.experimental.data_classes import Transformation, TransformationDestination, TransformationNotification

COGNITE_CLIENT = CogniteClient()
prefix = "".join(random.choice(string.ascii_letters) for i in range(6))


@pytest.fixture
def new_transformation():
    transform = Transformation(
        external_id=f"newTransformation-{prefix}",
        name="any",
        destination=TransformationDestination.assets(),
        query="select * from _cdf.assets",
        source_api_key=COGNITE_CLIENT.config.api_key,
        destination_api_key=COGNITE_CLIENT.config.api_key,
    )
    ts = COGNITE_CLIENT.transformations.create(transform)

    yield ts

    COGNITE_CLIENT.transformations.delete(id=ts.id)
    assert COGNITE_CLIENT.transformations.retrieve(ts.id) is None


@pytest.fixture
def new_notification(new_transformation):
    notification = TransformationNotification(config_id=new_transformation.id, destination="my@email.com")
    tn = COGNITE_CLIENT.transformations.notifications.create(notification)

    yield tn

    COGNITE_CLIENT.transformations.notifications.delete(id=tn.id)
    assert len(COGNITE_CLIENT.transformations.notifications.list(config_id=new_transformation.id)) == 0
    assert tn.config_id == new_transformation.id


@pytest.fixture
def new_notification_by_external_id(new_transformation):
    notification = TransformationNotification(
        config_external_id=new_transformation.external_id, destination="my@email.com"
    )
    tn = COGNITE_CLIENT.transformations.notifications.create(notification)

    yield (tn, new_transformation.external_id)

    COGNITE_CLIENT.transformations.notifications.delete(id=tn.id)
    assert (
        len(COGNITE_CLIENT.transformations.notifications.list(config_external_id=new_transformation.external_id)) == 0
    )


class TestTransformationNotificationsAPI:
    def test_create(self, new_notification: TransformationNotification):
        assert (
            new_notification.destination == "my@email.com"
            and new_notification.id is not None
            and new_notification.created_time is not None
            and new_notification.last_updated_time is not None
        )

    def test_create_by_external_id(self, new_notification_by_external_id: TransformationNotification):
        new_notification = new_notification_by_external_id[0]
        assert (
            new_notification.destination == "my@email.com"
            and new_notification.id is not None
            and new_notification.created_time is not None
            and new_notification.last_updated_time is not None
        )

    def test_list_all(self, new_notification):
        retrieved_notifications = COGNITE_CLIENT.transformations.notifications.list()
        assert new_notification.id in [notification.id for notification in retrieved_notifications]

    def test_list_by_id(self, new_notification):
        retrieved_notifications = COGNITE_CLIENT.transformations.notifications.list(
            config_id=new_notification.config_id
        )
        assert new_notification.id in [notification.id for notification in retrieved_notifications]
        assert len(retrieved_notifications) == 1

    def test_list_by_external_id(self, new_notification_by_external_id):
        new_notification = new_notification_by_external_id[0]
        external_id = new_notification_by_external_id[1]
        retrieved_notifications = COGNITE_CLIENT.transformations.notifications.list(config_external_id=external_id)
        assert new_notification.id in [notification.id for notification in retrieved_notifications]
        assert len(retrieved_notifications) == 1
