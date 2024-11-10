import doctest
from collections import defaultdict
from unittest import TextTestRunner
from unittest.mock import Mock, patch

import pytest

from cognite.client import _cognite_client, config, credentials
from cognite.client._api import (
    assets,
    data_sets,
    datapoints,
    datapoints_subscriptions,
    documents,
    entity_matching,
    events,
    files,
    iam,
    raw,
    relationships,
    sequences,
    three_d,
    time_series,
    units,
    workflows,
)
from cognite.client._api.data_modeling import containers, data_models, graphql, instances, spaces, views
from cognite.client._api.hosted_extractors import destinations, jobs, mappings, sources
from cognite.client._api.postgres_gateway import tables as postgres_gateway_tables
from cognite.client._api.postgres_gateway import users as postgres_gateway_users
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


@patch("os.environ", defaultdict(lambda: "value"))  # ensure env.var. lookups does not fail in doctests
def test_cognite_client():
    run_docstring_tests(_cognite_client)


@patch("cognite.client.credentials.ConfidentialClientApplication")
@patch("cognite.client.credentials.PublicClientApplication")
@patch("pathlib.Path.read_text", Mock(return_value="certificatecontents123"))
@patch("os.environ", defaultdict(lambda: "value"))  # ensure env.var. lookups does not fail in doctests
def test_credential_providers(mock_msal_app, mock_public_client):
    mock_msal_app().acquire_token_for_client.return_value = {
        "access_token": "azure_token",
        "expires_in": 1000,
    }
    mock_public_client().acquire_token_silent.return_value = {"access_token": "azure_token", "expires_in": 1000}
    run_docstring_tests(credentials)


@patch("cognite.client.CogniteClient", CogniteClientMock)
@patch("os.environ", defaultdict(lambda: "value"))
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

    def test_documents(self):
        run_docstring_tests(documents)

    @pytest.mark.dsl
    def test_raw(self):
        run_docstring_tests(raw)

    def test_3d(self):
        run_docstring_tests(three_d)

    def test_iam(self):
        run_docstring_tests(iam)

    @pytest.mark.dsl
    def test_sequences(self):
        run_docstring_tests(sequences)

    def test_relationships(self):
        run_docstring_tests(relationships)

    def test_entity_matching(self):
        run_docstring_tests(entity_matching)

    def test_data_modeling(self):
        run_docstring_tests(containers)
        run_docstring_tests(views)
        run_docstring_tests(instances)
        run_docstring_tests(data_models)
        run_docstring_tests(spaces)
        run_docstring_tests(graphql)

    def test_datapoint_subscriptions(self):
        run_docstring_tests(datapoints_subscriptions)

    def test_workflows(self):
        run_docstring_tests(workflows)

    def test_units(self):
        run_docstring_tests(units)

    def test_config(self):
        run_docstring_tests(config)

    def test_hosted_extractors(self):
        run_docstring_tests(mappings)
        run_docstring_tests(sources)
        run_docstring_tests(destinations)
        run_docstring_tests(jobs)

    def test_postgres_gateway(self):
        run_docstring_tests(postgres_gateway_users)
        run_docstring_tests(postgres_gateway_tables)
