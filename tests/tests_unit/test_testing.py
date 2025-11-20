from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cognite.client import AsyncCogniteClient, ClientConfig, CogniteClient
from cognite.client._api_client import APIClient
from cognite.client.credentials import Token
from cognite.client.testing import AsyncCogniteClientMock, CogniteClientMock, monkeypatch_cognite_client
from cognite.client.utils._auxiliary import all_concrete_subclasses
from tests.utils import all_mock_children, get_api_class_by_attribute


@pytest.fixture(scope="module")
def all_sync_client_mock_children() -> dict[str, tuple[type[APIClient], MagicMock]]:
    # This is a slooow call, so we reuse the results in this module
    return all_mock_children(CogniteClientMock())


@pytest.fixture(scope="module")
def all_async_client_mock_children() -> dict[str, tuple[type[APIClient], MagicMock]]:
    # This is a slooow call, so we reuse the results in this module
    return all_mock_children(AsyncCogniteClientMock())


@pytest.fixture(scope="module")
def mock_spec_cls_lookup(
    all_async_client_mock_children: dict[str, tuple[type[APIClient], MagicMock]],
) -> dict[str, type[APIClient]]:
    return {k: v for k, (v, _) in all_async_client_mock_children.items()}


@pytest.fixture(scope="module")
def mocked_api_lookup(
    all_async_client_mock_children: dict[str, tuple[type[APIClient], MagicMock]],
) -> dict[str, MagicMock]:
    return {k: v for k, (_, v) in all_async_client_mock_children.items()}


def test_ensure_all_apis_are_available_on_cognite_mock(mock_spec_cls_lookup: dict[str, type[APIClient]]) -> None:
    available = set(mock_spec_cls_lookup.values())
    expected = set(all_concrete_subclasses(APIClient))
    # Any new APIs that have not been added to CogniteClientMock?
    assert not expected.difference(available), f"Missing APIs: {expected.difference(available)}"
    # Any removed APIs that are still available on CogniteClientMock?
    assert not available.difference(expected), f"Removed APIs: {available.difference(expected)}"


def test_ensure_all_apis_use_equal_attr_paths_on_cognite_mock(mock_spec_cls_lookup: dict[str, type[APIClient]]) -> None:
    client = AsyncCogniteClient(ClientConfig(client_name="a", project="b", cluster="x", credentials="c"))  # type: ignore[arg-type]
    available_apis = set(get_api_class_by_attribute(client).items())
    mocked_apis = set(mock_spec_cls_lookup.items())

    missing_apis = available_apis.difference(mocked_apis)
    assert not missing_apis, f"Missing APIs: {missing_apis}"

    extra_apis = mocked_apis.difference(available_apis)
    assert not extra_apis, f"Extra APIs: {extra_apis}"


def test_ensure_both_cognite_client_mocks_are_in_sync(  # pun intended
    mock_spec_cls_lookup: dict[str, type[APIClient]],
    all_sync_client_mock_children: dict[str, tuple[type[APIClient], MagicMock]],
) -> None:
    sync_client_apis = {
        # A bit magical this, but how we translate async API class names to sync API class names is by prefixing with "Sync" and
        # replacing "3D" with "ThreeD". lol, worth it:
        k: v.__name__.replace("Sync", "").replace("3D", "ThreeD")
        for k, (v, _) in all_sync_client_mock_children.items()
    }
    async_client_apis = {k: v.__name__ for k, v in mock_spec_cls_lookup.items()}

    assert sync_client_apis == async_client_apis, "Sync and Async mock spec class lookups are not equal"


def test_ensure_all_apis_are_specced_on_cognite_mock(mocked_api_lookup: dict[str, MagicMock]) -> None:
    for dotted_path, mock_api in mocked_api_lookup.items():
        # All APIs should raise when trying to access a non-existing attribute:
        with pytest.raises(AttributeError):
            mock_api.does_not_exist
        # ...or set a non-existing attribute:
        with pytest.raises(AttributeError):
            mock_api.does_not_exist = 123

        # This will never trigger (above lines will), but this is actually what we ensure:
        assert mock_api._spec_set is True, f"API {dotted_path} does not have _spec_set=True"


def test_cognite_client_accepts_arguments_during_and_after_mock() -> None:
    # This test was here to ensure the old style ".__new__" override didn't fail after
    # reverting as that would break object.__new__ by passing more than the first arg.
    with monkeypatch_cognite_client():
        CogniteClient(ClientConfig(client_name="bla", project="bla", cluster="x", credentials=Token("bla")))
    CogniteClient(ClientConfig(client_name="bla", project="bla", cluster="x", credentials=Token("bla")))
