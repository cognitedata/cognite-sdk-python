import unittest
from cognite.preprocessing import *

MOCK_API_KEY = "AN_API_KEY"
MOCK_PROJECT = 'A_PROJECT'

class MergeDataframesTestCase(unittest.TestCase):

    def test_blabla(self):
        pass

def suites():
    suite1 = unittest.TestLoader().loadTestsFromTestCase(MergeDataframesTestCase)

    # return [suite1, suite2, suite3, suite4, suite5]
