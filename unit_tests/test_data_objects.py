import unittest
import cognite.tagmatching as tm
import cognite.assets as assets
import cognite.timeseries as timeseries
import cognite.similarity_search as ss

from cognite.config import configure_session

class TagMatchingTestCase(unittest.TestCase):

    def setUp(self):
        configure_session('m7SSQZ8ug72b1cUWCLMfc3uy9lkHBeyO', 'akerbp')
        self.response = tm.tag_matching(['18pi2317'])

    def test_object(self):
        from cognite._data_objects import TagMatchingObject
        self.assertIsInstance(self.response, TagMatchingObject)

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

    def tearDown(self):
        configure_session('', '')

class AssetsTestCase(unittest.TestCase):

    def setUp(self):
        configure_session('m7SSQZ8ug72b1cUWCLMfc3uy9lkHBeyO', 'akerbp')
        search_results = assets.searchAssets('xmas').to_pandas()
        first = search_results.ix[0]
        self.response = assets.getAssets(tagId=first.id)

    def test_object(self):
        from cognite._data_objects import AssetSearchObject
        self.assertIsInstance(self.response, AssetSearchObject)

    def test_json(self):
        self.assertIsInstance(self.response.to_json(), list)

    def test_pandas(self):
        import pandas as pd
        self.assertIsInstance(self.response.to_pandas(), pd.DataFrame)

    def test_ndarray(self):
        import numpy as np
        self.assertIsInstance(self.response.to_ndarray(), np.ndarray)

    def tearDown(self):
        configure_session('', '')

class TimeseriesTestCase(unittest.TestCase):

    def setUp(self):
        configure_session('m7SSQZ8ug72b1cUWCLMfc3uy9lkHBeyO', 'akerbp')
        self.response = timeseries.get_multi_tag_datapoints(['SKAP_18PI2117/Y/10sSAMP'],
                                                            aggregates=['avg'],
                                                            granularity='1h',
                                                            start='2w-ago')
    def test_multi_tag(self):
        self.assertIsInstance(self.response, list)

    def test_object(self):
        from cognite._data_objects import DatapointsObject
        self.assertIsInstance(self.response[0], DatapointsObject)

    def test_multi_tag_element_json(self):
        self.assertIsInstance(self.response[0].to_json(), dict)

    def test_pandas(self):
        import pandas as pd
        self.assertIsInstance(self.response[0].to_pandas(), pd.DataFrame)

    def test_ndarray(self):
        import numpy as np
        self.assertIsInstance(self.response[0].to_ndarray(), np.ndarray)

    def tearDown(self):
        configure_session('', '')

class SimilaritySearchTestCase(unittest.TestCase):

    def setUp(self):
        configure_session('m7SSQZ8ug72b1cUWCLMfc3uy9lkHBeyO', 'akerbp')
        ss_input_tag = 'SKAP_18PI2117/Y/10sSAMP'
        ss_query_tag = 'SKAP_18PI2317/Y/10sSAMP'
        self.response = ss.search(input_tags=[ss_input_tag],
                        query_tags=[ss_query_tag],
                        input_interval=(1360969200000, 1360969300000),
                        query_interval=(1360969200000, 1360991300000),
                        modes=["pattern"])

    def test_object(self):
        from cognite._data_objects import SimilaritySearchObject
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

    def tearDown(self):
        configure_session('', '')

def suites():
    suite1 = unittest.TestLoader().loadTestsFromTestCase(TagMatchingTestCase)
    suite2 = unittest.TestLoader().loadTestsFromTestCase(AssetsTestCase)
    suite4 = unittest.TestLoader().loadTestsFromTestCase(TimeseriesTestCase)
    suite3 = unittest.TestLoader().loadTestsFromTestCase(SimilaritySearchTestCase)
    return [suite1, suite2, suite3, suite4]
