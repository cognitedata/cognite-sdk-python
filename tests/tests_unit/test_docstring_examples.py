import doctest
from unittest import TextTestRunner
from unittest.mock import patch

import pytest

from cognite.client._api import (
    assets,
    data_sets,
    datapoints,
    entity_matching,
    events,
    files,
    iam,
    login,
    raw,
    relationships,
    sequences,
    three_d,
    time_series,
)
from cognite.client.testing import CogniteClientMock

# this fixes the issue with 'got MagicMock but expected Nothing in docstrings'
doctest.OutputChecker.__check_output = doctest.OutputChecker.check_output
doctest.OutputChecker.check_output = lambda self, want, got, optionflags: not want or self.__check_output(
    want, got, optionflags
)


def run_docstring_tests(module):
    runner = TextTestRunner()
    s = runner.run(doctest.DocTestSuite(module))
    assert 0 == len(s.failures)


@patch("cognite.client.CogniteClient", CogniteClientMock)
class TestDocstringExamples:
    def test_time_series(self):
        run_docstring_tests(time_series)

    def test_assets(self):
        run_docstring_tests(assets)

    @pytest.mark.dsl
    def test_datapoints(self):
        run_docstring_tests(datapoints)

    def test_data_sets(self):
        run_docstring_tests(data_sets)

    def test_events(self):
        run_docstring_tests(events)

    def test_files(self):
        run_docstring_tests(files)

    def test_login(self):
        run_docstring_tests(login)

    @pytest.mark.dsl
    def test_raw(self):
        run_docstring_tests(raw)

    def test_3d(self):
        run_docstring_tests(three_d)

    def test_iam(self):
        run_docstring_tests(iam)

    def test_sequences(self):
        run_docstring_tests(sequences)

    def test_relationships(self):
        run_docstring_tests(relationships)

    def test_entity_matching(self):
        run_docstring_tests(entity_matching)
