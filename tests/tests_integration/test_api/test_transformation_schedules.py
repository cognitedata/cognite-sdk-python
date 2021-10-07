import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import (
    OidcCredentials,
    Transformation,
    TransformationDestination,
    TransformationSchedule,
    TransformationScheduleUpdate,
)

COGNITE_CLIENT = CogniteClient()


@pytest.fixture
def new_transformation():
    transform = Transformation(
        name="any",
        destination=TransformationDestination.assets(),
        query="select * from _cdf.assets",
        source_oidc_credentials=OidcCredentials(
            client_id=COGNITE_CLIENT.config.token_client_id,
            client_secret=COGNITE_CLIENT.config.token_client_secret,
            scopes=",".join(COGNITE_CLIENT.config.token_scopes),
            token_uri=COGNITE_CLIENT.config.token_url,
            cdf_project_name=COGNITE_CLIENT.config.project,
        ),
        destination_oidc_credentials=OidcCredentials(
            client_id=COGNITE_CLIENT.config.token_client_id,
            client_secret=COGNITE_CLIENT.config.token_client_secret,
            scopes=",".join(COGNITE_CLIENT.config.token_scopes),
            token_uri=COGNITE_CLIENT.config.token_url,
            cdf_project_name=COGNITE_CLIENT.config.project,
        ),
    )
    ts = COGNITE_CLIENT.transformations.create(transform)

    yield ts

    COGNITE_CLIENT.transformations.delete(id=ts.id)
    assert COGNITE_CLIENT.transformations.retrieve(ts.id) is None


other_transformation = new_transformation


def schedule_from_transformation(transformation):
    schedule = TransformationSchedule(id=transformation.id, interval="0 * * * *")
    tsc = COGNITE_CLIENT.transformations.schedules.create(schedule)

    yield tsc

    COGNITE_CLIENT.transformations.schedules.delete(id=tsc.id)
    assert COGNITE_CLIENT.transformations.schedules.retrieve(tsc.id) is None
    assert tsc.id == transformation.id


@pytest.fixture
def new_schedule(new_transformation):
    yield from schedule_from_transformation(new_transformation)


@pytest.fixture
def other_schedule(other_transformation):
    yield from schedule_from_transformation(other_transformation)


class TestTransformationSchedulesAPI:
    def test_create(self, new_schedule: TransformationSchedule):
        assert (
            new_schedule.interval == "0 * * * *"
            and new_schedule.is_paused == False
            and new_schedule.created_time is not None
            and new_schedule.last_updated_time is not None
        )

    def test_retrieve(self, new_schedule: TransformationSchedule):
        retrieved_schedule = COGNITE_CLIENT.transformations.schedules.retrieve(new_schedule.id)
        assert (
            new_schedule.id == retrieved_schedule.id
            and new_schedule.interval == retrieved_schedule.interval
            and new_schedule.is_paused == retrieved_schedule.is_paused
        )

    def test_retrieve_multiple(self, new_schedule: TransformationSchedule, other_schedule: TransformationSchedule):
        assert new_schedule.id != other_schedule.id
        ids = [new_schedule.id, other_schedule.id]
        retrieved_schedules = COGNITE_CLIENT.transformations.schedules.retrieve_multiple(ids=ids)
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

    def test_update_full(self, new_schedule):
        new_schedule.interval = "5 * * * *"
        new_schedule.is_paused = True
        updated_schedule = COGNITE_CLIENT.transformations.schedules.update(new_schedule)
        retrieved_schedule = COGNITE_CLIENT.transformations.schedules.retrieve(new_schedule.id)
        assert (
            updated_schedule.interval == retrieved_schedule.interval == "5 * * * *"
            and updated_schedule.is_paused == retrieved_schedule.is_paused == True
        )

    def test_update_partial(self, new_schedule):
        update_schedule = TransformationScheduleUpdate(id=new_schedule.id).interval.set("5 * * * *").is_paused.set(True)
        updated_schedule = COGNITE_CLIENT.transformations.schedules.update(update_schedule)
        retrieved_schedule = COGNITE_CLIENT.transformations.schedules.retrieve(new_schedule.id)
        assert (
            updated_schedule.interval == retrieved_schedule.interval == "5 * * * *"
            and updated_schedule.is_paused == retrieved_schedule.is_paused == True
        )

    def test_list(self, new_schedule):
        retrieved_schedules = COGNITE_CLIENT.transformations.schedules.list()
        assert new_schedule.id in [schedule.id for schedule in retrieved_schedules]
