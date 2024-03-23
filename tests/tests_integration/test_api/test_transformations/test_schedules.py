import os
import string

import pytest

from cognite.client import CogniteClient
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import (
    OidcCredentials,
    Transformation,
    TransformationDestination,
    TransformationSchedule,
    TransformationScheduleUpdate,
)
from cognite.client.utils._text import random_string


@pytest.fixture
def new_transformation(cognite_client: CogniteClient) -> Transformation:
    prefix = random_string(6, string.ascii_letters)
    creds = cognite_client.config.credentials
    assert isinstance(creds, OAuthClientCredentials)
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


other_transformation = new_transformation


def schedule_from_transformation(
    cognite_client: CogniteClient, transformation: Transformation
) -> TransformationSchedule:
    schedule = TransformationSchedule(id=transformation.id, interval="0 * * * *")
    tsc = cognite_client.transformations.schedules.create(schedule)

    yield tsc

    cognite_client.transformations.schedules.delete(id=tsc.id)
    assert cognite_client.transformations.schedules.retrieve(tsc.id) is None
    assert tsc.id == transformation.id


@pytest.fixture
def new_schedule(cognite_client: CogniteClient, new_transformation: Transformation) -> TransformationSchedule:
    yield from schedule_from_transformation(cognite_client, new_transformation)


@pytest.fixture
def other_schedule(cognite_client: CogniteClient, other_transformation: Transformation) -> TransformationSchedule:
    yield from schedule_from_transformation(cognite_client, other_transformation)


@pytest.mark.skipif(
    os.getenv("LOGIN_FLOW") != "client_credentials", reason="This test requires client_credentials auth"
)
class TestTransformationSchedulesAPI:
    def test_create(self, new_schedule: TransformationSchedule):
        assert (
            new_schedule.interval == "0 * * * *"
            and new_schedule.is_paused is False
            and new_schedule.created_time is not None
            and new_schedule.last_updated_time is not None
        )

    def test_schedule_member(self, cognite_client, new_schedule: TransformationSchedule):
        retrieved_transformation = cognite_client.transformations.retrieve(id=new_schedule.id)
        assert (
            retrieved_transformation.schedule.interval == "0 * * * *"
            and retrieved_transformation.schedule.is_paused is False
        )

    def test_retrieve(self, cognite_client, new_schedule: TransformationSchedule):
        retrieved_schedule = cognite_client.transformations.schedules.retrieve(new_schedule.id)
        assert (
            new_schedule.id == retrieved_schedule.id
            and new_schedule.interval == retrieved_schedule.interval
            and new_schedule.is_paused == retrieved_schedule.is_paused
        )

    def test_retrieve_multiple(
        self, cognite_client, new_schedule: TransformationSchedule, other_schedule: TransformationSchedule
    ):
        assert new_schedule.id != other_schedule.id
        ids = [new_schedule.id, other_schedule.id]
        retrieved_schedules = cognite_client.transformations.schedules.retrieve_multiple(ids=ids)
        assert len(retrieved_schedules) == 2
        for retrieved_schedule in retrieved_schedules:
            assert (
                new_schedule.id == retrieved_schedule.id
                and new_schedule.interval == retrieved_schedule.interval
                and new_schedule.is_paused == retrieved_schedule.is_paused
            ) or (
                other_schedule.id == retrieved_schedule.id
                and other_schedule.interval == retrieved_schedule.interval
                and other_schedule.is_paused == retrieved_schedule.is_paused
            )

    def test_update_full(self, cognite_client, new_schedule):
        new_schedule.interval = "5 * * * *"
        new_schedule.is_paused = True
        updated_schedule = cognite_client.transformations.schedules.update(new_schedule)
        retrieved_schedule = cognite_client.transformations.schedules.retrieve(new_schedule.id)
        assert updated_schedule.interval == retrieved_schedule.interval == "5 * * * *"
        assert updated_schedule.is_paused is True
        assert retrieved_schedule.is_paused is True

    def test_update_partial(self, cognite_client, new_schedule):
        update_schedule = TransformationScheduleUpdate(id=new_schedule.id).interval.set("5 * * * *").is_paused.set(True)
        updated_schedule = cognite_client.transformations.schedules.update(update_schedule)
        retrieved_schedule = cognite_client.transformations.schedules.retrieve(new_schedule.id)
        assert updated_schedule.interval == retrieved_schedule.interval == "5 * * * *"
        assert updated_schedule.is_paused is True
        assert retrieved_schedule.is_paused is True

    def test_list(self, cognite_client, new_schedule):
        retrieved_schedules = cognite_client.transformations.schedules.list()
        assert new_schedule.id in [schedule.id for schedule in retrieved_schedules]
