import doctest
from collections import defaultdict
from typing import Any
from unittest import TextTestRunner
from unittest.mock import MagicMock, Mock, patch

import pytest

from cognite.client import _cognite_client, config, credentials
from cognite.client._api import (
    ai,
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
    simulators,
    three_d,
    time_series,
    units,
    workflows,
)
from cognite.client._api.data_modeling import containers, data_models, graphql, instances, spaces, statistics, views
from cognite.client._api.hosted_extractors import destinations, jobs, mappings, sources
from cognite.client._api.postgres_gateway import tables as postgres_gateway_tables
from cognite.client._api.postgres_gateway import users as postgres_gateway_users
from cognite.client.testing import CogniteClientMock

# this fixes the issue with 'got MagicMock but expected Nothing in docstrings'
doctest.OutputChecker.__check_output = doctest.OutputChecker.check_output  # type: ignore[attr-defined]
doctest.OutputChecker.check_output = lambda self, want, got, optionflags: not want or self.__check_output(  # type: ignore[attr-defined, method-assign]
    want, got, optionflags
)


def run_docstring_tests(module: Any) -> None:
    runner = TextTestRunner()
    s = runner.run(doctest.DocTestSuite(module))
    assert 0 == len(s.failures)


@patch("os.environ", defaultdict(lambda: "value"))  # ensure env.var. lookups does not fail in doctests
def test_cognite_client() -> None:
    run_docstring_tests(_cognite_client)


@patch("cognite.client.credentials.PublicClientApplication")
@patch("cognite.client.credentials.ConfidentialClientApplication")
@patch("pathlib.Path.read_text", Mock(return_value="certificatecontents123"))
@patch("os.environ", defaultdict(lambda: "value"))  # ensure env.var. lookups does not fail in doctests
def test_credential_providers(mock_confidential_client: MagicMock, mock_public_client: MagicMock) -> None:
    mock_confidential_client().acquire_token_for_client.return_value = {
        "access_token": "azure_token",
        "expires_in": 1000,
    }
    mock_public_client().acquire_token_silent.return_value = {"access_token": "azure_token", "expires_in": 1000}
    run_docstring_tests(credentials)


@patch("cognite.client.CogniteClient", CogniteClientMock)
@patch("os.environ", defaultdict(lambda: "value"))
class TestDocstringExamples:
    def test_time_series(self) -> None:
        run_docstring_tests(time_series)

    def test_assets(self) -> None:
        run_docstring_tests(assets)

    @pytest.mark.dsl
    def test_datapoints(self) -> None:
        run_docstring_tests(datapoints)

    def test_data_sets(self) -> None:
        run_docstring_tests(data_sets)

    def test_events(self) -> None:
        run_docstring_tests(events)

    def test_files(self) -> None:
        run_docstring_tests(files)

    def test_documents(self) -> None:
        run_docstring_tests(documents)

    @pytest.mark.dsl
    def test_raw(self) -> None:
        run_docstring_tests(raw)

    def test_3d(self) -> None:
        run_docstring_tests(three_d)

    def test_iam(self) -> None:
        run_docstring_tests(iam)

    @pytest.mark.dsl
    def test_sequences(self) -> None:
        run_docstring_tests(sequences)

    def test_relationships(self) -> None:
        run_docstring_tests(relationships)

    def test_entity_matching(self) -> None:
        run_docstring_tests(entity_matching)

    def test_data_modeling(self) -> None:
        run_docstring_tests(containers)
        run_docstring_tests(views)
        run_docstring_tests(instances)
        run_docstring_tests(data_models)
        run_docstring_tests(spaces)
        run_docstring_tests(graphql)
        run_docstring_tests(statistics)

    def test_datapoint_subscriptions(self) -> None:
        run_docstring_tests(datapoints_subscriptions)

    def test_workflows(self) -> None:
        run_docstring_tests(workflows)

    def test_units(self) -> None:
        run_docstring_tests(units)

    def test_config(self) -> None:
        run_docstring_tests(config)

    def test_hosted_extractors(self) -> None:
        run_docstring_tests(mappings)
        run_docstring_tests(sources)
        run_docstring_tests(destinations)
        run_docstring_tests(jobs)

    def test_postgres_gateway(self) -> None:
        run_docstring_tests(postgres_gateway_users)
        run_docstring_tests(postgres_gateway_tables)

    def test_ai(self) -> None:
        run_docstring_tests(ai)
        run_docstring_tests(ai.tools)
        run_docstring_tests(ai.tools.documents)

    def test_simulators(self) -> None:
        run_docstring_tests(simulators)
        run_docstring_tests(simulators.models)
        run_docstring_tests(simulators.models_revisions)
        run_docstring_tests(simulators.integrations)
        run_docstring_tests(simulators.routines)
        run_docstring_tests(simulators.routine_revisions)
        run_docstring_tests(simulators.logs)
        run_docstring_tests(simulators.runs)
