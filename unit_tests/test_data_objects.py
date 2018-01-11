import unittest

from unit_tests.api_mock_responses import *

class TagMatchingTestCase(unittest.TestCase):

    def setUp(self):
        self.response = tag_matching_response
    def test_object(self):
        from cognite.data_objects import TagMatchingObject
        self.assertIsInstance(self.response, TagMatchingObject)

    def test_json(self):
        self.assertIsInstance(self.response.to_json(), list)

    def test_pandas(self):
        import pandas as pd
        self.assertIsInstance(self.response.to_pandas(), pd.DataFrame)

    def test_ndarray(self):
        import numpy as np
        self.assertIsInstance(self.response.to_ndarray(), np.ndarray)

class AssetsTestCase(unittest.TestCase):

    def setUp(self):
        self.response = assets_response

    def test_object(self):
        from cognite.data_objects import AssetSearchObject
        self.assertIsInstance(self.response, AssetSearchObject)

    def test_json(self):
        self.assertIsInstance(self.response.to_json(), list)

    def test_pandas(self):
        import pandas as pd
        self.assertIsInstance(self.response.to_pandas(), pd.DataFrame)

    def test_ndarray(self):
        import numpy as np
        self.assertIsInstance(self.response.to_ndarray(), np.ndarray)

class TimeseriesTestCase(unittest.TestCase):

    def setUp(self):
        self.response = timeseries_response
    def test_multi_tag(self):
        self.assertIsInstance(self.response, list)

    def test_object(self):
        from cognite.data_objects import DatapointsObject
        self.assertIsInstance(self.response[0], DatapointsObject)

    def test_multi_tag_element_json(self):
        self.assertIsInstance(self.response[0].to_json(), dict)

    def test_pandas(self):
        import pandas as pd
        self.assertIsInstance(self.response[0].to_pandas(), pd.DataFrame)

    def test_ndarray(self):
        import numpy as np
        self.assertIsInstance(self.response[0].to_ndarray(), np.ndarray)

class SimilaritySearchTestCase(unittest.TestCase):

    def setUp(self):
        self.response = similarity_search_response

    def test_object(self):
        from cognite.data_objects import SimilaritySearchObject
        self.assertIsInstance(self.response, SimilaritySearchObject)

    def test_json(self):
        self.assertIsInstance(self.response.to_json(), list)

    def test_pandas(self):
        import pandas as pd
        self.assertIsInstance(self.response.to_pandas(), pd.DataFrame)

    def test_ndarray(self):
        import numpy as np
        self.assertIsInstance(self.response.to_ndarray(), np.ndarray)

    def test_response_length(self):
        self.assertGreater(len(self.response.to_json()), 0)

def suites():
    suite1 = unittest.TestLoader().loadTestsFromTestCase(TagMatchingTestCase)
    suite2 = unittest.TestLoader().loadTestsFromTestCase(AssetsTestCase)
    suite4 = unittest.TestLoader().loadTestsFromTestCase(TimeseriesTestCase)
    suite3 = unittest.TestLoader().loadTestsFromTestCase(SimilaritySearchTestCase)
    return [suite1, suite2, suite3, suite4]
