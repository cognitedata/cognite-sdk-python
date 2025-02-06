import re

import pytest

from cognite.client.data_classes import ExtractionPipelineConfig, ExtractionPipelineConfigRevisionList
from tests.utils import get_url


@pytest.fixture
def mock_config_response(httpx_mock, cognite_client):
    response_body = {
        "revision": 5,
        "externalId": "int-123",
        "description": "description",
        "createdTime": 1565965333132,
        "config": "config abc 123",
    }
    url_pattern = re.compile(re.escape(get_url(cognite_client.extraction_pipelines)) + r"/extpipes/config")

    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    yield response_body


@pytest.fixture
def mock_config_response_with_revision(httpx_mock, cognite_client):
    response_body = {
        "revision": 4,
        "externalId": "int-123",
        "description": "description",
        "createdTime": 1565965333132,
        "config": "config abc 123 2",
    }
    url_pattern = re.compile(
        re.escape(get_url(cognite_client.extraction_pipelines)) + r"/extpipes/config\?externalId=int-123&revision=4$"
    )

    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    yield response_body


@pytest.fixture
def mock_config_list_response(httpx_mock, cognite_client):
    response_body = {
        "items": [
            {"revision": 3, "externalId": "int-123", "description": "description 3", "createdTime": 1565965333132},
            {"revision": 2, "externalId": "int-123", "description": "description 2", "createdTime": 1565965233132},
            {"revision": 1, "externalId": "int-123", "description": "description 1", "createdTime": 1565965133132},
        ]
    }
    url_pattern = re.compile(re.escape(get_url(cognite_client.extraction_pipelines)) + r"/extpipes/config/revisions")

    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    yield response_body


@pytest.fixture
def mock_revert_config_response(httpx_mock, cognite_client):
    response_body = {
        "revision": 6,
        "externalId": "int-123",
        "description": "description",
        "createdTime": 1565965333132,
        "config": "config abc 123",
    }
    url_pattern = re.compile(re.escape(get_url(cognite_client.extraction_pipelines)) + r"/extpipes/config/revert")
    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    yield response_body


class TestExtractionPipelines:
    def test_retrieve_config(self, cognite_client, mock_config_response):
        res = cognite_client.extraction_pipelines.config.retrieve(external_id="int-123")
        assert isinstance(res, ExtractionPipelineConfig)
        assert mock_config_response == res.dump(camel_case=True)

    def test_retrieve_config_revision(self, cognite_client, mock_config_response_with_revision):
        res = cognite_client.extraction_pipelines.config.retrieve(external_id="int-123", revision=4)
        assert isinstance(res, ExtractionPipelineConfig)
        assert mock_config_response_with_revision == res.dump(camel_case=True)

    def test_new_config(self, cognite_client, mock_config_response):
        res = cognite_client.extraction_pipelines.config.create(
            ExtractionPipelineConfig(external_id="int-123", config="config abc 123", description="description")
        )
        assert isinstance(res, ExtractionPipelineConfig)
        assert mock_config_response == res.dump(camel_case=True)

    def test_revert_config(self, cognite_client, mock_revert_config_response):
        res = cognite_client.extraction_pipelines.config.revert(external_id="int-123", revision=3)
        assert isinstance(res, ExtractionPipelineConfig)
        assert mock_revert_config_response == res.dump(camel_case=True)

    def test_list_revisions(self, cognite_client, mock_config_list_response):
        res = cognite_client.extraction_pipelines.config.list(external_id="int-123")
        assert isinstance(res, ExtractionPipelineConfigRevisionList)
        assert mock_config_list_response == {"items": res.dump(camel_case=True)}
