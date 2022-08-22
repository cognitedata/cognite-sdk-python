from unittest.mock import MagicMock

import pytest

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import APIKey
from cognite.client.testing import CogniteClientMock, monkeypatch_cognite_client


def test_mock_cognite_client():
    with monkeypatch_cognite_client() as c_mock:
        c = CogniteClient(ClientConfig(client_name="bla", project="bla", credentials=APIKey("bla")))
        assert isinstance(c_mock, MagicMock)
        assert c_mock == c

        api_pairs = [
            (c.time_series, c_mock.time_series),
            (c.raw, c_mock.raw),
            (c.assets, c_mock.assets),
            (c.datapoints, c_mock.datapoints),
            (c.events, c_mock.events),
            (c.files, c_mock.files),
            (c.iam, c_mock.iam),
            (c.login, c_mock.login),
            (c.three_d, c_mock.three_d),
        ]
        for api, mock_api in api_pairs:
            assert isinstance(mock_api, MagicMock)
            assert api == mock_api

            with pytest.raises(AttributeError):
                api.does_not_exist


def test_cognite_client_accepts_arguments_during_and_after_mock():
    with monkeypatch_cognite_client():
        CogniteClient(ClientConfig(client_name="bla", project="bla", credentials=APIKey("bla")))
    CogniteClient(ClientConfig(client_name="bla", project="bla", credentials=APIKey("bla")))


def test_client_mock_can_access_attributes_not_explicitly_defined_on_children():
    c_mock = CogniteClientMock()
    assert c_mock.config.max_workers
