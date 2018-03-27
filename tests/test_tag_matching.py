# Temporary mock setup while waiting for a better way to do integration tests
from unittest.mock import patch

import pytest


def mocked_post_request():
    return {'cucoo': 'haha'}


@patch('requests.post')
@pytest.fixture(scope='module')
def tagmatching_result(mock_post):
    from cognite.tagmatching import tag_matching
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {'data': {'items': [{'matches': [
        {'platform': 'a_platform', 'score': 0, 'tagId': 'a_match'},
        {'platform': 'a_platform', 'score': 0, 'tagId': 'a_match1'},
        {'platform': 'a_platform', 'score': 0, 'tagId': 'a_match2'}], 'tagId': 'a_tag'}]}}
    return tag_matching(tag_ids=['a_tag'])


def test_object(tagmatching_result):
    from cognite.data_objects import TagMatchingObject
    assert isinstance(tagmatching_result, TagMatchingObject)


def test_json(tagmatching_result):
    assert isinstance(tagmatching_result.to_json(), list)


def test_pandas(tagmatching_result):
    import pandas as pd
    assert isinstance(tagmatching_result.to_pandas(), pd.DataFrame)


def test_ndarray(tagmatching_result):
    import numpy as np
    assert isinstance(tagmatching_result.to_ndarray(), np.ndarray)


def test_list_len_first_matches(tagmatching_result):
    l = tagmatching_result.to_list(first_matches_only=False)
    assert len(l) == 3


def test_list_len_all_matches(tagmatching_result):
    l = tagmatching_result.to_list()
    assert len(l) == 1
