import os

import pytest


@pytest.fixture(scope='module')
def tagmatching_result():
    from cognite.tagmatching import tag_matching
    return tag_matching(tag_ids=['skap18pi2317'], api_key=os.getenv('COGNITE_API_KEY'), project='akerbp')


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

# class TimeseriesTestCase(unittest.TestCase):
#     def setUp(self):
#         self.response = timeseries_response
#
#     def test_multi_tag(self):
#         self.assertIsInstance(self.response, list)
#
#     def test_object(self):
#         from cognite.data_objects import DatapointsObject
#         self.assertIsInstance(self.response[0], DatapointsObject)
#
#     def test_multi_tag_element_json(self):
#         self.assertIsInstance(self.response[0].to_json(), dict)
#
#     def test_pandas(self):
#         import pandas as pd
#         self.assertIsInstance(self.response[0].to_pandas(), pd.DataFrame)
#
#     def test_ndarray(self):
#         import numpy as np
#         self.assertIsInstance(self.response[0].to_ndarray(), np.ndarray)
#
#

#
#

#
