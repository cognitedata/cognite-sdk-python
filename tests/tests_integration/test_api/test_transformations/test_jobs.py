from __future__ import annotations

import os
import string
import time
from collections.abc import Iterator

import pytest

from cognite.client import CogniteClient
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import (
    OidcCredentials,
    Transformation,
    TransformationDestination,
    TransformationJob,
    TransformationJobStatus,
    TransformationWrite,
)
from cognite.client.utils._text import random_string


@pytest.fixture
def new_transformation(cognite_client: CogniteClient) -> Iterator[Transformation]:
    prefix = random_string(6, string.ascii_letters)
    creds = cognite_client.config.credentials
    assert isinstance(creds, OAuthClientCredentials)
    transform = TransformationWrite(
        name="any",
        external_id=f"{prefix}-transformation",
        destination=TransformationDestination.assets(),
        query="select 'test-sdk-transformations' as externalId, 'test-sdk-transformations' as name",
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
        ignore_null_fields=True,
    )
    ts = cognite_client.transformations.create(transform)

    yield ts

    cognite_client.transformations.delete(id=ts.id)
    assert cognite_client.transformations.retrieve(ts.id) is None


@pytest.fixture
def new_raw_transformation(
    cognite_client: CogniteClient, new_transformation: Transformation
) -> Iterator[Transformation]:
    new_transformation.query = "select 1 as key, 'example2' as name, 'example' as externalId"
    new_transformation.destination = TransformationDestination.raw("my_db", "my_table")
    ts = cognite_client.transformations.update(new_transformation)

    yield ts


other_transformation = new_transformation


@pytest.fixture
def longer_transformation(cognite_client: CogniteClient, new_transformation: Transformation) -> Transformation:
    new_transformation.query = "select id, name from _cdf.assets limit 5000"
    ts = cognite_client.transformations.update(new_transformation)
    return ts


def run_transformation_without_waiting(
    new_transformation: Transformation,
) -> Iterator[tuple[TransformationJob, Transformation]]:
    job = new_transformation.run(wait=False)
    yield (job, new_transformation)
    job.wait()
    assert job.status in [TransformationJobStatus.COMPLETED, TransformationJobStatus.FAILED]


@pytest.fixture
def new_running_transformation(
    cognite_client: CogniteClient, new_transformation: Transformation
) -> Iterator[tuple[TransformationJob, Transformation]]:
    new_transformation.query = "SELECT cast(sequence(1, 1000000) as string) as description, 'running-transformation-asset' as name, 'running-transformation-asset' as externalId"
    cognite_client.transformations.update(new_transformation)
    yield from run_transformation_without_waiting(new_transformation)


@pytest.fixture
def other_running_transformation(
    other_transformation: Transformation,
) -> Iterator[tuple[TransformationJob, Transformation]]:
    yield from run_transformation_without_waiting(other_transformation)


@pytest.mark.skipif(
    os.getenv("LOGIN_FLOW") != "client_credentials", reason="This test requires client_credentials auth"
)
class TestTransformationJobsAPI:
    def test_run_without_wait(
        self, cognite_client: CogniteClient, new_running_transformation: tuple[TransformationJob, Transformation]
    ) -> None:
        (job, new_transformation) = new_running_transformation
        assert job.id is not None
        assert job.status == TransformationJobStatus.CREATED
        assert job.transformation_id == new_transformation.id
        assert job.transformation_external_id == new_transformation.external_id
        assert job.source_project == cognite_client.config.project
        assert job.destination_project == cognite_client.config.project
        assert job.destination == TransformationDestination.assets()
        assert job.conflict_mode == "upsert"
        assert job.query == new_transformation.query
        assert job.error is None
        assert job.ignore_null_fields
        time.sleep(0.8)

        retrieved_transformation = cognite_client.transformations.retrieve(id=new_transformation.id)
        assert retrieved_transformation
        assert retrieved_transformation.running_job is not None and retrieved_transformation.running_job.id == job.id

    def test_run(self, cognite_client: CogniteClient, new_transformation: Transformation) -> None:
        job = new_transformation.run()

        assert job.id is not None
        assert job.status == TransformationJobStatus.COMPLETED
        assert job.transformation_id == new_transformation.id
        assert job.transformation_external_id == new_transformation.external_id
        assert job.source_project == cognite_client.config.project
        assert job.destination_project == cognite_client.config.project
        assert job.destination == TransformationDestination.assets()
        assert job.conflict_mode == "upsert"
        assert job.query == new_transformation.query
        assert job.error is None
        assert job.ignore_null_fields

        retrieved_transformation = cognite_client.transformations.retrieve(id=new_transformation.id)

        assert (
            retrieved_transformation
            and retrieved_transformation.last_finished_job is not None
            and retrieved_transformation.last_finished_job.id == job.id
        )

    def test_run_with_timeout(self, longer_transformation: Transformation) -> None:
        init = time.time()
        timeout = 0.1
        job = longer_transformation.run(timeout=timeout)
        final = time.time()

        assert job.status == TransformationJobStatus.RUNNING and timeout <= final - init <= timeout + 1.5

    def test_run_by_external_id(self, cognite_client: CogniteClient, new_transformation: Transformation) -> None:
        job = cognite_client.transformations.run(transformation_external_id=new_transformation.external_id)

        assert job.id is not None
        assert job.status == TransformationJobStatus.COMPLETED
        assert job.transformation_id == new_transformation.id
        assert job.transformation_external_id == new_transformation.external_id
        assert job.source_project == cognite_client.config.project
        assert job.destination_project == cognite_client.config.project
        assert job.destination == TransformationDestination.assets()
        assert job.conflict_mode == "upsert"
        assert job.query == new_transformation.query
        assert job.error is None
        assert job.ignore_null_fields

    def test_run_raw_transformation(
        self, cognite_client: CogniteClient, new_raw_transformation: Transformation
    ) -> None:
        job = new_raw_transformation.run(timeout=60)

        assert job.id is not None
        assert job.status not in [TransformationJobStatus.CREATED, TransformationJobStatus.RUNNING]
        assert job.transformation_id == new_raw_transformation.id
        assert job.transformation_external_id == new_raw_transformation.external_id
        assert job.source_project == cognite_client.config.project
        assert job.destination_project == cognite_client.config.project
        assert job.destination == TransformationDestination.raw("my_db", "my_table")
        assert job.conflict_mode == "upsert"
        assert job.query == new_raw_transformation.query
        assert job.ignore_null_fields

    def test_cancel_job(self, new_running_transformation: tuple[TransformationJob, Transformation]) -> None:
        (new_job, _) = new_running_transformation
        time.sleep(0.5)
        new_job.cancel()
        new_job.wait()
        assert new_job.status == TransformationJobStatus.FAILED and new_job.error == "Job cancelled by the user."

    def test_cancel_transformation(self, new_running_transformation: tuple[TransformationJob, Transformation]) -> None:
        (new_job, new_transformation) = new_running_transformation
        time.sleep(0.5)
        new_transformation.cancel()
        new_job.wait()
        assert new_job.status == TransformationJobStatus.FAILED and new_job.error == "Job cancelled by the user."

    def test_list_jobs_by_transformation_id(
        self, new_running_transformation: tuple[TransformationJob, Transformation]
    ) -> None:
        (new_job, new_transformation) = new_running_transformation

        retrieved_jobs = new_transformation.jobs()
        assert new_job.id in [job.id for job in retrieved_jobs]
        assert all(job.transformation_id == new_transformation.id for job in retrieved_jobs)

    def test_list_jobs(
        self,
        cognite_client: CogniteClient,
        new_running_transformation: tuple[TransformationJob, Transformation],
        other_running_transformation: tuple[TransformationJob, Transformation],
    ) -> None:
        (new_job, _) = new_running_transformation
        (other_job, _) = other_running_transformation

        retrieved_jobs = cognite_client.transformations.jobs.list()
        assert new_job.id in [job.id for job in retrieved_jobs]
        assert other_job.id in [job.id for job in retrieved_jobs]

    def test_metrics(self, new_running_transformation: tuple[TransformationJob, Transformation]) -> None:
        (job, _) = new_running_transformation
        time.sleep(1.0)
        metrics = job.metrics()
        assert metrics is not None

    def test_retrieve_multiple(
        self,
        cognite_client: CogniteClient,
        new_running_transformation: tuple[TransformationJob, Transformation],
        other_running_transformation: tuple[TransformationJob, Transformation],
    ) -> None:
        (new_job, _) = new_running_transformation
        (other_job, _) = other_running_transformation

        retrieved_jobs = cognite_client.transformations.jobs.retrieve_multiple(ids=[new_job.id, other_job.id])
        assert new_job.id in [job.id for job in retrieved_jobs]
        assert other_job.id in [job.id for job in retrieved_jobs]
        assert len(retrieved_jobs) == 2
