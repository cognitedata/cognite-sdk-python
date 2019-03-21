# Temporary mock setup while waiting for a better way to do integration tests
from unittest.mock import patch

import pytest

from cognite.client import CogniteClient
from cognite.client.api.tagmatching import TagMatchingResponse
from tests.tests_old.conftest import MockReturnValue

tag_matching = CogniteClient().tag_matching.tag_matching


@pytest.fixture(scope="module")
@patch("requests.sessions.Session.post")
def tagmatching_result(mock_post):
    response = {
        "data": {
            "items": [
                {
                    "matches": [
                        {"platform": "a_platform", "score": 0, "tagId": "a_match"},
                        {"platform": "a_platform", "score": 0, "tagId": "a_match1"},
                        {"platform": "a_platform", "score": 0, "tagId": "a_match2"},
                    ],
                    "tagId": "a_tag",
                }
            ]
        }
    }
    mock_post.return_value = MockReturnValue(status=200, json_data=response)
    return tag_matching(tag_ids=["a_tag"])


def test_object(tagmatching_result):
    assert isinstance(tagmatching_result, TagMatchingResponse)


def test_json(tagmatching_result):
    assert isinstance(tagmatching_result.to_json(), list)


def test_pandas(tagmatching_result):
    import pandas as pd

    assert isinstance(tagmatching_result.to_pandas(), pd.DataFrame)


def test_list_len_first_matches(tagmatching_result):
    l = tagmatching_result.to_list(first_matches_only=False)
    assert len(l) == 3


def test_list_len_all_matches(tagmatching_result):
    l = tagmatching_result.to_list()
    assert len(l) == 1
