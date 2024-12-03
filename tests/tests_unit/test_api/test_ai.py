from __future__ import annotations

import re

import pytest

from cognite.client.data_classes import AnswerContent, AnswerLocation, AnswerReference, Summary


@pytest.fixture
def mock_summarize_response(rsps, cognite_client):
    response_body = {
        "items": [
            {
                "id": 1234,
                "summary": "Summary",
            }
        ]
    }

    url_pattern = re.compile(re.escape(cognite_client.ai.tools.documents._get_base_url_with_base_path()) + "/.+")
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    yield rsps


@pytest.fixture
def mock_ask_response(rsps, cognite_client):
    response_body = {
        "content": [
            {
                "text": "Content",
                "references": [
                    {
                        "fileId": 1234,
                        "fileName": "foo.pdf",
                        "locations": [
                            {
                                "pageNumber": 1,
                                "left": 0.0,
                                "right": 1.0,
                                "top": 0.0,
                                "bottom": 1.0,
                            }
                        ],
                    }
                ],
            }
        ]
    }

    url_pattern = re.compile(re.escape(cognite_client.ai.tools.documents._get_base_url_with_base_path()) + "/.+")
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    yield rsps


class TestAIAPI:
    def test_summarize(self, cognite_client, mock_summarize_response):
        summaries = cognite_client.ai.tools.documents.summarize(ids=[1234])
        assert len(summaries) == 1
        summary = summaries[0]
        assert isinstance(summary, Summary)
        assert summary.id == 1234
        assert summary.summary == "Summary"

    def test_ask_question(self, cognite_client, mock_ask_response):
        answer = cognite_client.ai.tools.documents.ask_question(question="How is the weather?", ids=[1234])
        assert len(answer.content) == 1
        content = answer.content[0]
        assert isinstance(content, AnswerContent)
        assert len(content.references) == 1
        reference = content.references[0]
        assert isinstance(reference, AnswerReference)
        assert reference.file_id == 1234
        assert reference.file_name == "foo.pdf"
        assert len(reference.locations) == 1
        location = reference.locations[0]
        assert isinstance(location, AnswerLocation)
        assert location.page_number == 1
        assert location.left == 0.0
        assert location.right == 1.0
        assert location.top == 0.0
        assert location.bottom == 1.0
