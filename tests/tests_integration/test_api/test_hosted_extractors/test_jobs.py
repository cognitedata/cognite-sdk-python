from __future__ import annotations

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.hosted_extractors import (
    CogniteFormat,
    Destination,
    Job,
    JobList,
    JobLogsList,
    JobMetricsList,
    JobUpdate,
    JobWrite,
    Source,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._text import random_string

# @pytest.fixture
# def one_source(cognite_client: CogniteClient) -> Source:
#     my_source = EventHubSourceWrite(
#         external_id=f"myNewSource-{random_string(10)}",
#         host="myhost",
#         event_hub_name="myeventhub",
#         key_name="mykeyname",
#         key_value="secret",
#     )
#     retrieved = cognite_client.hosted_extractors.sources.retrieve(my_source.external_id, ignore_unknown_ids=True)
#     if retrieved:
#         yield retrieved
#     created = cognite_client.hosted_extractors.sources.create(my_source)
#     yield created
#
#     cognite_client.hosted_extractors.sources.delete(created.external_id, ignore_unknown_ids=True)


@pytest.fixture
def one_job(cognite_client: CogniteClient, one_event_hub_source: Source, one_destination: Destination) -> Job:
    my_job = JobWrite(
        external_id=f"myJobForTesting-{random_string(10)}",
        destination_id=one_destination.external_id,
        source_id=one_event_hub_source.external_id,
        format=CogniteFormat(encoding="utf16"),
    )
    retrieved = cognite_client.hosted_extractors.jobs.retrieve(my_job.external_id, ignore_unknown_ids=True)
    if retrieved:
        yield retrieved
    created = cognite_client.hosted_extractors.jobs.create(my_job)
    yield created

    cognite_client.hosted_extractors.jobs.delete(created.external_id, ignore_unknown_ids=True)


class TestJobs:
    def test_create_update_retrieve_delete(
        self, cognite_client: CogniteClient, one_destination: Destination, one_event_hub_source: Source
    ) -> None:
        my_job = JobWrite(
            external_id=f"myJobForTesting-{random_string(10)}",
            destination_id=one_destination.external_id,
            source_id=one_event_hub_source.external_id,
            format=CogniteFormat(
                encoding="utf16",
            ),
        )
        created: Job | None = None
        try:
            created = cognite_client.hosted_extractors.jobs.create(my_job)
            assert isinstance(created, Job)
            update = JobUpdate(external_id=my_job.external_id).format.set(CogniteFormat(encoding="utf16le"))
            updated = cognite_client.hosted_extractors.jobs.update(update)
            format_ = updated.format
            assert isinstance(format_, CogniteFormat)
            assert format_.encoding == "utf16le"
            retrieved = cognite_client.hosted_extractors.jobs.retrieve(created.external_id)
            assert retrieved is not None
            assert retrieved.external_id == created.external_id

            cognite_client.hosted_extractors.jobs.delete(created.external_id)

            with pytest.raises(CogniteAPIError):
                cognite_client.hosted_extractors.jobs.retrieve(created.external_id)

            cognite_client.hosted_extractors.jobs.retrieve(created.external_id, ignore_unknown_ids=True)

        finally:
            if created:
                cognite_client.hosted_extractors.jobs.delete(created.external_id, ignore_unknown_ids=True)

    @pytest.mark.usefixtures("one_job")
    def test_list(self, cognite_client: CogniteClient) -> None:
        res = cognite_client.hosted_extractors.jobs.list(limit=1)
        assert len(res) == 1
        assert isinstance(res, JobList)

    def test_update_using_write_object(
        self, cognite_client: CogniteClient, one_destination: Destination, one_event_hub_source: Source
    ) -> None:
        my_job = JobWrite(
            external_id=f"myJobForTesting-{random_string(10)}",
            destination_id=one_destination.external_id,
            source_id=one_event_hub_source.external_id,
            format=CogniteFormat(
                encoding="utf16",
            ),
        )
        created: Job | None = None
        try:
            created = cognite_client.hosted_extractors.jobs.create(my_job)

            update = JobWrite(
                external_id=my_job.external_id,
                destination_id=one_destination.external_id,
                source_id=one_event_hub_source.external_id,
                format=CogniteFormat(
                    encoding="utf16le",
                ),
            )

            updated = cognite_client.hosted_extractors.jobs.update(update)
            format_ = updated.format
            assert isinstance(format_, CogniteFormat)
            assert format_.encoding == "utf16le"
        finally:
            if created:
                cognite_client.hosted_extractors.jobs.delete(created.external_id, ignore_unknown_ids=True)

    def test_list_logs(self, cognite_client: CogniteClient) -> None:
        res = cognite_client.hosted_extractors.jobs.list_logs(limit=1)
        assert isinstance(res, JobLogsList)

    def test_list_metrics(self, cognite_client: CogniteClient) -> None:
        res = cognite_client.hosted_extractors.jobs.list_metrics(limit=1)
        assert isinstance(res, JobMetricsList)
