import asyncio
import random
import string

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import Transformation, TransformationDestination, TransformationJobStatus

COGNITE_CLIENT = CogniteClient()


@pytest.fixture
def new_transformation():
    prefix = "".join(random.choice(string.ascii_letters) for i in range(6))
    transform = Transformation(
        name="any",
        external_id=f"{prefix}-transformation",
        destination=TransformationDestination.assets(),
        query="select id, name from _cdf.assets limit 1000",
        source_api_key=COGNITE_CLIENT.config.api_key,
        destination_api_key=COGNITE_CLIENT.config.api_key,
        ignore_null_fields=True,
    )
    ts = COGNITE_CLIENT.transformations.create(transform)

    yield ts

    COGNITE_CLIENT.transformations.delete(id=ts.id)
    assert COGNITE_CLIENT.transformations.retrieve(ts.id) is None


other_transformation = new_transformation


async def run_transformation_without_waiting(new_transformation):
    job = new_transformation.run(wait=False)

    yield (job, new_transformation)

    await job.wait_async()
    assert job.status == TransformationJobStatus.COMPLETED


@pytest.fixture
async def new_running_transformation(new_transformation):
    async for transform in run_transformation_without_waiting(new_transformation):
        yield transform


@pytest.fixture
async def other_running_transformation(other_transformation):
    async for transform in run_transformation_without_waiting(other_transformation):
        yield transform


class TestTransformationJobsAPI:
    @pytest.mark.asyncio
    async def test_run_without_wait(self, new_running_transformation):
        (job, new_transformation) = new_running_transformation
        assert (
            job.id is not None
            and job.uuid is not None
            and job.status == TransformationJobStatus.CREATED
            and job.transformation_id == new_transformation.id
            and job.source_project == COGNITE_CLIENT.config.project
            and job.destination_project == COGNITE_CLIENT.config.project
            and job.destination.type == "assets"
            and job.conflict_mode == "upsert"
            and job.raw_query == new_transformation.query
            and job.error is None
            and job.ignore_null_fields
        )

    def test_run(self, new_transformation: Transformation):
        job = new_transformation.run()

        assert (
            job.id is not None
            and job.uuid is not None
            and job.status == TransformationJobStatus.COMPLETED
            and job.transformation_id == new_transformation.id
            and job.source_project == COGNITE_CLIENT.config.project
            and job.destination_project == COGNITE_CLIENT.config.project
            and job.destination.type == "assets"
            and job.conflict_mode == "upsert"
            and job.raw_query == new_transformation.query
            and job.error is None
            and job.ignore_null_fields
        )

    @pytest.mark.asyncio
    async def test_run_async(self, new_transformation: Transformation):
        job = await new_transformation.run_async()

        assert (
            job.id is not None
            and job.uuid is not None
            and job.status == TransformationJobStatus.COMPLETED
            and job.transformation_id == new_transformation.id
            and job.source_project == COGNITE_CLIENT.config.project
            and job.destination_project == COGNITE_CLIENT.config.project
            and job.destination.type == "assets"
            and job.conflict_mode == "upsert"
            and job.raw_query == new_transformation.query
            and job.error is None
            and job.ignore_null_fields
        )

    @pytest.mark.asyncio
    async def test_run_by_external_id_async(self, new_transformation: Transformation):
        job = await COGNITE_CLIENT.transformations.run_async(transformation_external_id=new_transformation.external_id)

        assert (
            job.id is not None
            and job.uuid is not None
            and job.status == TransformationJobStatus.COMPLETED
            and job.transformation_id == new_transformation.id
            and job.source_project == COGNITE_CLIENT.config.project
            and job.destination_project == COGNITE_CLIENT.config.project
            and job.destination.type == "assets"
            and job.conflict_mode == "upsert"
            and job.raw_query == new_transformation.query
            and job.error is None
            and job.ignore_null_fields
        )

    @pytest.mark.asyncio
    async def test_list_jobs_by_transformation_id(self, new_running_transformation):
        (new_job, new_transformation) = new_running_transformation

        retrieved_jobs = new_transformation.jobs()
        assert new_job.id in [job.id for job in retrieved_jobs]
        assert all(job.transformation_id == new_transformation.id for job in retrieved_jobs)

    @pytest.mark.asyncio
    async def test_list_jobs(self, new_running_transformation, other_running_transformation):
        (new_job, _) = new_running_transformation
        (other_job, _) = other_running_transformation

        retrieved_jobs = COGNITE_CLIENT.transformations.jobs.list()
        assert new_job.id in [job.id for job in retrieved_jobs]
        assert other_job.id in [job.id for job in retrieved_jobs]

    @pytest.mark.asyncio
    async def test_metrics(self, new_running_transformation):
        (job, _) = new_running_transformation
        await asyncio.sleep(1.0)
        metrics = job.metrics()
        assert metrics is not None

    @pytest.mark.asyncio
    async def test_retrieve_multiple(self, new_running_transformation, other_running_transformation):
        (new_job, _) = new_running_transformation
        (other_job, _) = other_running_transformation

        retrieved_jobs = COGNITE_CLIENT.transformations.jobs.retrieve_multiple(ids=[new_job.id, other_job.id])
        assert new_job.id in [job.id for job in retrieved_jobs]
        assert other_job.id in [job.id for job in retrieved_jobs]
        assert len(retrieved_jobs) == 2
