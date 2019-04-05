import doctest
from unittest import TextTestRunner

import pytest

from cognite.client.api import assets, datapoints, events, files, login, time_series


def run_docstring_tests(module):
    runner = TextTestRunner()
    s = runner.run(doctest.DocTestSuite(module))
    assert 0 == len(s.failures)


@pytest.mark.usefixtures("mock_cognite_client")
class TestDocstringExamples:
    def test_time_series(self):
        run_docstring_tests(time_series)

    def test_assets(self):
        run_docstring_tests(assets)

    def test_datapoints(self):
        run_docstring_tests(datapoints)

    def test_events(self):
        run_docstring_tests(events)

    def test_files(self):
        run_docstring_tests(files)

    def test_login(self):
        run_docstring_tests(login)
