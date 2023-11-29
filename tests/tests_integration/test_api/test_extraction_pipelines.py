from datetime import datetime, timedelta, timezone

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import ExtractionPipeline, ExtractionPipelineRun, ExtractionPipelineUpdate
from cognite.client.data_classes.extractionpipelines import ExtractionPipelineContact, ExtractionPipelineRunList
from cognite.client.exceptions import CogniteNotFoundError
from cognite.client.utils import datetime_to_ms
from cognite.client.utils._text import random_string


@pytest.fixture
def new_extpipe(cognite_client: CogniteClient):
    testid = random_string(50)
    dataset = cognite_client.data_sets.list()[0]
    extpipe = cognite_client.extraction_pipelines.create(
        ExtractionPipeline(
            external_id=f"testid-{testid}",
            name=f"Test extpipe {testid}",
            data_set_id=dataset.id,
            description="Short description",
            contacts=[
                ExtractionPipelineContact(
                    name="John Doe", email="john.doe@cognite.com", role="owner", send_notification=False
                )
            ],
            schedule="Continuous",
        )
    )
    yield extpipe
    try:
        cognite_client.extraction_pipelines.delete(id=extpipe.id)
    except Exception:
        pass
    assert cognite_client.extraction_pipelines.retrieve(extpipe.id) is None


@pytest.fixture
def populated_runs(cognite_client: CogniteClient, new_extpipe: ExtractionPipeline) -> ExtractionPipelineRunList:
    now = datetime_to_ms(datetime.now(timezone.utc))
    a_year_ago = datetime_to_ms(datetime.now(timezone.utc).replace(year=datetime.now(timezone.utc).year - 1))
    runs = [
        ExtractionPipelineRun(
            extpipe_external_id=new_extpipe.external_id, status="failure", message="lorem ipsum", created_time=now
        ),
        ExtractionPipelineRun(
            extpipe_external_id=new_extpipe.external_id,
            status="success",
            message="dolor sit amet",
            created_time=a_year_ago,
        ),
    ]
    created = []
    for run in runs:
        new_run = cognite_client.extraction_pipelines.runs.create(run)
        created.append(new_run)
    return ExtractionPipelineRunList(created)


class TestExtractionPipelinesAPI:
    def test_retrieve(self, cognite_client):
        res = cognite_client.extraction_pipelines.list(limit=1)
        assert res[0] == cognite_client.extraction_pipelines.retrieve(id=res[0].id)

    def test_retrieve_multiple(self, cognite_client):
        res_listed_ids = [e.id for e in cognite_client.extraction_pipelines.list(limit=2)]
        res_lookup_ids = [e.id for e in cognite_client.extraction_pipelines.retrieve_multiple(res_listed_ids)]
        for listed_id in res_listed_ids:
            assert listed_id in res_lookup_ids

    def test_retrieve_unknown(self, cognite_client):
        res = cognite_client.extraction_pipelines.list(limit=1)
        with pytest.raises(CogniteNotFoundError):
            cognite_client.extraction_pipelines.retrieve_multiple(ids=[res[0].id], external_ids=["this does not exist"])
        retr = cognite_client.extraction_pipelines.retrieve_multiple(
            ids=[res[0].id], external_ids=["this does not exist"], ignore_unknown_ids=True
        )
        assert 1 == len(retr)

    def test_update(self, cognite_client, new_extpipe: ExtractionPipeline):
        update_extraction_pipeline = ExtractionPipelineUpdate(id=new_extpipe.id).metadata.set({"hey": "now"})
        res = cognite_client.extraction_pipelines.update(update_extraction_pipeline)
        assert {"hey": "now"} == res.metadata
        update_extraction_pipeline2 = ExtractionPipelineUpdate(id=new_extpipe.id).metadata.set(None)
        res2 = cognite_client.extraction_pipelines.update(update_extraction_pipeline2)
        assert res2.metadata is None

    def test_delete(self, cognite_client, new_extpipe: ExtractionPipeline):
        cognite_client.extraction_pipelines.delete(id=new_extpipe.id)
        assert cognite_client.extraction_pipelines.retrieve(id=new_extpipe.id) is None

    def test_last_status_message(self, cognite_client, new_extpipe: ExtractionPipeline):
        intial_message = cognite_client.extraction_pipelines.retrieve(new_extpipe.id).last_message
        assert intial_message is None

        cognite_client.extraction_pipelines.runs.create(
            ExtractionPipelineRun(status="failure", message="Oh no!", extpipe_external_id=new_extpipe.external_id)
        )
        new_message = cognite_client.extraction_pipelines.retrieve(new_extpipe.id).last_message
        assert new_message == "Oh no!"

    def test_last_run_times(self, cognite_client, new_extpipe: ExtractionPipeline):
        retrieve = cognite_client.extraction_pipelines.retrieve
        extpipe = retrieve(external_id=new_extpipe.external_id)
        assert extpipe.last_failure == 0
        assert extpipe.last_success == 0
        assert extpipe.last_seen == 0

        # IDEA: Add convenience method to update an ext.pipe? (inspiration, see: FunctionsAPI)
        new_run = cognite_client.extraction_pipelines.runs.create
        new_run(ExtractionPipelineRun(extpipe_external_id=new_extpipe.external_id, status="seen"))
        extpipe = retrieve(external_id=new_extpipe.external_id)
        assert extpipe.last_failure == 0
        assert extpipe.last_success == 0
        assert extpipe.last_seen != 0

        new_run(ExtractionPipelineRun(extpipe_external_id=new_extpipe.external_id, status="success"))
        extpipe = retrieve(external_id=new_extpipe.external_id)
        assert extpipe.last_failure == 0
        assert extpipe.last_success != 0
        assert extpipe.last_seen != 0

        new_run(ExtractionPipelineRun(extpipe_external_id=new_extpipe.external_id, status="failure"))
        extpipe = retrieve(external_id=new_extpipe.external_id)
        assert extpipe.last_failure != 0
        assert extpipe.last_success != 0
        assert extpipe.last_seen != 0

    def test_list_extraction_pipeline_runs(
        self, cognite_client: CogniteClient, new_extpipe: ExtractionPipeline
    ) -> None:
        cognite_client.extraction_pipelines.runs.create(
            ExtractionPipelineRun(extpipe_external_id=new_extpipe.external_id, status="seen")
        )
        res = cognite_client.extraction_pipelines.runs.list(new_extpipe.external_id)
        for run in res:
            assert run.extpipe_external_id == new_extpipe.external_id

        # Make sure we can dump it without errors
        dumped = res.dump(camel_case=False)
        for run in dumped:
            assert run["external_id"] == new_extpipe.external_id

    def test_list_failed_extraction_pipeline_runs(
        self,
        cognite_client: CogniteClient,
        new_extpipe: ExtractionPipeline,
        populated_runs: ExtractionPipelineRunList,
    ) -> None:
        expected = ExtractionPipelineRunList([run for run in populated_runs if run.status == "failure"])

        filtered = cognite_client.extraction_pipelines.runs.list(
            external_id=new_extpipe.external_id, statuses="failure", limit=1
        )

        assert expected.dump() == filtered.dump()

    def test_filter_extraction_pipeline_runs_created_ago(
        self,
        cognite_client: CogniteClient,
        new_extpipe: ExtractionPipeline,
        populated_runs: ExtractionPipelineRunList,
    ) -> None:
        yesterday = datetime_to_ms(datetime.now(timezone.utc) - timedelta(days=1))
        expected = ExtractionPipelineRunList([run for run in populated_runs if run.created_time > yesterday])

        filtered = cognite_client.extraction_pipelines.runs.list(
            external_id=new_extpipe.external_id, created_time="24h-ago", limit=1
        )

        assert expected.dump() == filtered.dump()
