import doctest
from unittest import TextTestRunner

import pytest

from cognite.client._api import assets, datapoints, events, files, iam, login, raw, sequences, three_d, time_series


def run_docstring_tests(module):
    runner = TextTestRunner()
    s = runner.run(doctest.DocTestSuite(module))
    assert 0 == len(s.failures)


@pytest.mark.usefixtures("mock_cognite_client")
class TestDocstringExamples:
    def test_time_series(self):
        run_docstring_tests(time_series)

    def test_sequences(self):
        run_docstring_tests(sequences)

    def test_assets(self):
        run_docstring_tests(assets)

    @pytest.mark.dsl
    def test_datapoints(self):
        run_docstring_tests(datapoints)

    def test_events(self):
        run_docstring_tests(events)

    def test_files(self):
        run_docstring_tests(files)

    def test_login(self):
        run_docstring_tests(login)

    def test_raw(self):
        run_docstring_tests(raw)

    def test_3d(self):
        run_docstring_tests(three_d)

    def test_iam(self):
        run_docstring_tests(iam)
