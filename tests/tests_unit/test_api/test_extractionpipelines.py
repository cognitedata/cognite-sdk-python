import re

import pytest

from cognite.client.data_classes import ExtractionPipelineConfig, ExtractionPipelineConfigRevisionList


@pytest.fixture
def mock_config_response(rsps, cognite_client):
    response_body = {
        "revision": 5,
        "externalId": "int-123",
        "description": "description",
        "createdTime": 1565965333132,
        "config": "config abc 123",
    }
    url_pattern = re.compile(
        re.escape(cognite_client.extraction_pipelines._get_base_url_with_base_path()) + r"/extpipes/config"
    )
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


@pytest.fixture
def mock_config_response_with_revision(rsps, cognite_client):
    response_body = {
        "revision": 4,
        "externalId": "int-123",
        "description": "description",
        "createdTime": 1565965333132,
        "config": "config abc 123 2",
    }
    url_pattern = re.compile(
        re.escape(cognite_client.extraction_pipelines._get_base_url_with_base_path())
        + r"/extpipes/config\?externalId=int-123&revision=4$"
    )
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


@pytest.fixture
def mock_config_list_response(rsps, cognite_client):
    response_body = {
        "items": [
            {"revision": 3, "externalId": "int-123", "description": "description 3", "createdTime": 1565965333132},
            {"revision": 2, "externalId": "int-123", "description": "description 2", "createdTime": 1565965233132},
            {"revision": 1, "externalId": "int-123", "description": "description 1", "createdTime": 1565965133132},
        ]
    }
    url_pattern = re.compile(
        re.escape(cognite_client.extraction_pipelines._get_base_url_with_base_path()) + r"/extpipes/config/revisions"
    )
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


@pytest.fixture
def mock_revert_config_response(rsps, cognite_client):
    response_body = {
        "revision": 6,
        "externalId": "int-123",
        "description": "description",
        "createdTime": 1565965333132,
        "config": "config abc 123",
    }
    url_pattern = re.compile(
        re.escape(cognite_client.extraction_pipelines._get_base_url_with_base_path()) + r"/extpipes/config/revert"
    )
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    yield rsps


class TestExtractionPipelines:
    def test_retrieve_config(self, cognite_client, mock_config_response):
        res = cognite_client.extraction_pipelines.config.retrieve(external_id="int-123")
        res.cognite_client = None
        assert isinstance(res, ExtractionPipelineConfig)
        assert mock_config_response.calls[0].response.json() == res.dump(camel_case=True)

    def test_retrieve_config_revision(self, cognite_client, mock_config_response_with_revision):
        res = cognite_client.extraction_pipelines.config.retrieve(external_id="int-123", revision=4)
        res.cognite_client = None
        assert isinstance(res, ExtractionPipelineConfig)
        assert mock_config_response_with_revision.calls[0].response.json() == res.dump(camel_case=True)

    def test_new_config(self, cognite_client, mock_config_response):
        res = cognite_client.extraction_pipelines.config.create(
            ExtractionPipelineConfig(external_id="int-123", config="config abc 123", description="description")
        )
        res.cognite_client = None
        assert isinstance(res, ExtractionPipelineConfig)
        assert mock_config_response.calls[0].response.json() == res.dump(camel_case=True)

    def test_revert_config(self, cognite_client, mock_revert_config_response):
        res = cognite_client.extraction_pipelines.config.revert(external_id="int-123", revision=3)
        res.cognite_client = None
        assert isinstance(res, ExtractionPipelineConfig)
        assert mock_revert_config_response.calls[0].response.json() == res.dump(camel_case=True)

    def test_list_revisions(self, cognite_client, mock_config_list_response):
        res = cognite_client.extraction_pipelines.config.list(external_id="int-123")
        res.cognite_client = None
        for r in res:
            r.cognite_client = None
        assert isinstance(res, ExtractionPipelineConfigRevisionList)
        assert mock_config_list_response.calls[0].response.json() == {"items": res.dump(camel_case=True)}
