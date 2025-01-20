from __future__ import annotations

import re

import pytest

from cognite.client.data_classes.ai import AnswerContent, AnswerLocation, AnswerReference, Summary


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
                "text": "This is ",
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
            },
            {
                "text": "the answer.",
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
                    },
                    {
                        "fileId": 2345,
                        "fileName": "bar.pdf",
                        "locations": [
                            {
                                "pageNumber": 2,
                                "left": 1.0,
                                "right": 2.0,
                                "top": 1.0,
                                "bottom": 2.0,
                            }
                        ],
                    },
                ],
            },
        ]
    }

    url_pattern = re.compile(re.escape(cognite_client.ai.tools.documents._get_base_url_with_base_path()) + "/.+")
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    yield rsps


class TestAIAPI:
    @pytest.mark.usefixtures("mock_summarize_response")
    def test_summarize(self, cognite_client):
        summary = cognite_client.ai.tools.documents.summarize(id=1234)
        assert isinstance(summary, Summary)
        assert summary.id == 1234
        assert summary.summary == "Summary"

    @pytest.mark.usefixtures("mock_ask_response")
    def test_ask_question(self, cognite_client):
        answer = cognite_client.ai.tools.documents.ask_question(question="How is the weather?", id=[1234, 2345])
        assert len(answer.content) == 2
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

    @pytest.mark.usefixtures("mock_ask_response")
    def test_answer_methods(self, cognite_client):
        answer = cognite_client.ai.tools.documents.ask_question(question="How is the weather?", id=[1234, 2345])
        assert answer.full_answer == "This is the answer."
        all_refs = answer.all_references
        assert len(all_refs) == 2
        assert {ref.file_name for ref in all_refs} == {"foo.pdf", "bar.pdf"}
