import unittest
import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from unit_tests.test_data_objects import suites as do_suites
from unit_tests.test_config import suites as c_suites
from unit_tests.test_preprocessing import suites as pre_suites
from unit_tests.test_utils import suites as utils_suites

def get_all_test_suites():
    data_object_test_suites = do_suites()
    config_suites = c_suites()
    preprocessing_suites = pre_suites()
    suites = data_object_test_suites + \
             config_suites + \
             preprocessing_suites + \
             utils_suites()
    return suites

def run():
    suites = get_all_test_suites()
    alltests = unittest.TestSuite(suites)
    runner = unittest.TextTestRunner()
    return runner.run(alltests)


if __name__ == '__main__':
    result = run()
    print("{}/{} tests passed.".format(result.testsRun - len(result.errors), result.testsRun))
