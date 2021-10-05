import pytest

from cognite.client.data_classes import ExtractionPipeline, ExtractionPipelineRun, ExtractionPipelineUpdate
from cognite.client.data_classes.extractionpipelines import ExtractionPipelineContact
from cognite.client.exceptions import CogniteNotFoundError
from cognite.client.utils._auxiliary import random_string


@pytest.fixture
def new_extpipe(cognite_client):
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
    except:
        pass
    assert cognite_client.extraction_pipelines.retrieve(extpipe.id) is None


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

        cognite_client.extraction_pipeline_runs.create(
            ExtractionPipelineRun(external_id=new_extpipe.external_id, status="failure", message="Oh no!")
        )
        new_message = cognite_client.extraction_pipelines.retrieve(new_extpipe.id).last_message
        assert new_message == "Oh no!"

    def test_last_run_times(self, cognite_client, new_extpipe: ExtractionPipeline):
        extpipe = cognite_client.extraction_pipelines.retrieve(external_id=new_extpipe.external_id)
        assert extpipe.last_failure == 0
        assert extpipe.last_success == 0
        assert extpipe.last_seen == 0

        cognite_client.extraction_pipeline_runs.create(
            ExtractionPipelineRun(external_id=new_extpipe.external_id, status="seen")
        )
        extpipe = cognite_client.extraction_pipelines.retrieve(external_id=new_extpipe.external_id)
        assert extpipe.last_failure == 0
        assert extpipe.last_success == 0
        assert extpipe.last_seen != 0

        cognite_client.extraction_pipeline_runs.create(
            ExtractionPipelineRun(external_id=new_extpipe.external_id, status="success")
        )
        extpipe = cognite_client.extraction_pipelines.retrieve(external_id=new_extpipe.external_id)
        assert extpipe.last_failure == 0
        assert extpipe.last_success != 0
        assert extpipe.last_seen != 0

        cognite_client.extraction_pipeline_runs.create(
            ExtractionPipelineRun(external_id=new_extpipe.external_id, status="failure")
        )
        extpipe = cognite_client.extraction_pipelines.retrieve(external_id=new_extpipe.external_id)
        assert extpipe.last_failure != 0
        assert extpipe.last_success != 0
        assert extpipe.last_seen != 0
