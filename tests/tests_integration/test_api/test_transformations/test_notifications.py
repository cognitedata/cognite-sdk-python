import string

import pytest

from cognite.client.data_classes import (
    OidcCredentials,
    Transformation,
    TransformationDestination,
    TransformationNotification,
)
from cognite.client.utils._text import random_string

prefix = random_string(6, string.ascii_letters)


@pytest.fixture
def new_transformation(cognite_client):
    creds = cognite_client.config.credentials
    transform = Transformation(
        name="any",
        external_id=f"{prefix}-transformation",
        destination=TransformationDestination.assets(),
        query="select * from _cdf.assets",
        source_oidc_credentials=OidcCredentials(
            client_id=creds.client_id,
            client_secret=creds.client_secret,
            scopes=",".join(creds.scopes),
            token_uri=creds.token_url,
            cdf_project_name=cognite_client.config.project,
        ),
        destination_oidc_credentials=OidcCredentials(
            client_id=creds.client_id,
            client_secret=creds.client_secret,
            scopes=",".join(creds.scopes),
            token_uri=creds.token_url,
            cdf_project_name=cognite_client.config.project,
        ),
    )
    ts = cognite_client.transformations.create(transform)

    yield ts

    cognite_client.transformations.delete(id=ts.id)
    assert cognite_client.transformations.retrieve(ts.id) is None


@pytest.fixture
def new_notification(cognite_client, new_transformation):
    notification = TransformationNotification(
        transformation_id=new_transformation.id, destination=f"my_{prefix}@email.com"
    )
    tn = cognite_client.transformations.notifications.create(notification)

    yield tn

    cognite_client.transformations.notifications.delete(id=tn.id)
    assert len(cognite_client.transformations.notifications.list(transformation_id=new_transformation.id)) == 0
    assert tn.transformation_id == new_transformation.id


@pytest.fixture
def new_notification_by_external_id(cognite_client, new_transformation):
    notification = TransformationNotification(
        transformation_external_id=new_transformation.external_id, destination=f"my_{prefix}@email.com"
    )
    tn = cognite_client.transformations.notifications.create(notification)

    yield (tn, new_transformation.external_id)

    cognite_client.transformations.notifications.delete(id=tn.id)
    assert (
        len(
            cognite_client.transformations.notifications.list(transformation_external_id=new_transformation.external_id)
        )
        == 0
    )


@pytest.mark.xfail
class TestTransformationNotificationsAPI:
    def test_create(self, new_notification: TransformationNotification):
        assert (
            new_notification.destination == f"my_{prefix}@email.com"
            and new_notification.id is not None
            and new_notification.created_time is not None
            and new_notification.last_updated_time is not None
        )

    def test_create_by_external_id(self, new_notification_by_external_id: TransformationNotification):
        new_notification = new_notification_by_external_id[0]
        assert (
            new_notification.destination == f"my_{prefix}@email.com"
            and new_notification.id is not None
            and new_notification.created_time is not None
            and new_notification.last_updated_time is not None
        )

    def test_list_all(self, cognite_client, new_notification):
        retrieved_notifications = cognite_client.transformations.notifications.list()
        assert new_notification.id in [notification.id for notification in retrieved_notifications]

    def test_list_by_id(self, cognite_client, new_notification):
        retrieved_notifications = cognite_client.transformations.notifications.list(
            transformation_id=new_notification.transformation_id
        )
        assert new_notification.id in [notification.id for notification in retrieved_notifications]
        assert len(retrieved_notifications) == 1

    def test_list_by_external_id(self, cognite_client, new_notification_by_external_id):
        new_notification = new_notification_by_external_id[0]
        external_id = new_notification_by_external_id[1]
        retrieved_notifications = cognite_client.transformations.notifications.list(
            transformation_external_id=external_id
        )
        assert new_notification.id in [notification.id for notification in retrieved_notifications]
        assert len(retrieved_notifications) == 1

    def test_list_by_destination(self, cognite_client, new_notification):
        retrieved_notifications = cognite_client.transformations.notifications.list(
            destination=f"my_{prefix}@email.com"
        )
        assert new_notification.id in [notification.id for notification in retrieved_notifications]
        assert len(retrieved_notifications) == 1

    def test_notification_to_string(self, new_notification):
        # just make sure it doesn't throw exceptions
        str(new_notification)
