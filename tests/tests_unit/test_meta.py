import inspect
from pathlib import Path

import pytest

from cognite.client._api_client import APIClient
from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
    ExternalIDTransformerMixin,
    IdTransformerMixin,
    InternalIdTransformerMixin,
)
from cognite.client.data_classes.datapoints import DatapointsArrayList, DatapointsList
from cognite.client.data_classes.principals import PrincipalList
from cognite.client.utils._url import RETRYABLE_POST_ENDPOINT_REGEX_PATTERN
from tests.utils import all_concrete_subclasses, all_subclasses

ALL_FILEPATHS = Path("cognite/client/").rglob("*.py")


def test_assert_no_root_init_file() -> None:
    # We have an implicit namespace package under the namespace package directory: 'cognite'.

    # From: https://packaging.python.org/en/latest/guides/packaging-namespace-packages/#native-namespace-packages
    # "It is extremely important that every distribution that uses the namespace package omits the __init__.py
    # or uses a pkgutil-style __init__.py. If any distribution does not, it will cause the namespace logic to
    # fail and the other sub-packages will not be importable"
    assert not Path("cognite/__init__.py").exists()


@pytest.mark.parametrize("cls", [CogniteResource, CogniteResourceList])
def test_ensure_all_to_pandas_methods_use_snake_case(cls: type) -> None:
    err_msg = "Class: '{}' for method to_pandas does not default camel_case parameter to False."
    for sub_cls in all_subclasses(cls):
        if not (cls_method := getattr(sub_cls, "to_pandas", False)):
            continue
        if param := inspect.signature(cls_method).parameters.get("camel_case"):  # type: ignore[arg-type]
            assert param.default is False, err_msg.format(sub_cls.__name__)


@pytest.fixture(scope="session")
def apis_with_post_method_retry_set() -> set[str]:
    all_paths = set()
    for api in filter(None, RETRYABLE_POST_ENDPOINT_REGEX_PATTERN.pattern.split("^/")):
        base_path = api.split("/")[0]
        if base_path[0] == "]":
            continue
        elif base_path[0] == "(":
            all_paths.update(base_path[1:-1].split("|"))
        else:
            all_paths.add(base_path)
    return all_paths


@pytest.fixture(scope="session")
def apis_that_should_not_have_post_retry_rule() -> set[str]:
    return set(
        [
            "groups",  # ☑️
            "securitycategories",  # ☑️
            "templategroups",  # Won't do: deprecated API
        ]
    )


@pytest.mark.parametrize(
    "api",
    sorted(  # why sorted? xdist needs order to be consistent between test workers
        set(api._RESOURCE_PATH.split("/")[1] for api in all_subclasses(APIClient) if hasattr(api, "_RESOURCE_PATH"))
    ),
)
def test_all_base_api_paths_have_retry_or_specifically_no_set(
    api: str, apis_with_post_method_retry_set: set[str], apis_that_should_not_have_post_retry_rule: set[str]
) -> None:
    # So you've added a new API to the SDK, but suddenly this test is failing - what's the deal?!
    # Answer the following:
    # Does this new API have POST methods that should be retried automatically?
    # if yes -> look up 'RETRYABLE_POST_ENDPOINT_REGEX_PATTERN' and add a regex for the url path
    # if no  -> add the url base path to the "okey-without-list" above: 'apis_that_should_not_have_post_retry_rule'
    # ...but always(!): add tests to TestRetryableEndpoints!
    has_retry = api in apis_with_post_method_retry_set
    no_retry_needed = api in apis_that_should_not_have_post_retry_rule
    assert has_retry or no_retry_needed

    # If the below check fails, it means an API that has been specifically except from POST retries now have
    # been given a retry regex anyway. Please update 'apis_that_should_not_have_post_retry_rule' above!
    assert not (has_retry and no_retry_needed)


@pytest.mark.parametrize(
    "lst_cls",
    [
        list_cls
        # Principal list .as_ids() returns a list of strings and not integers,
        # so we skip the check for it.
        for list_cls in all_concrete_subclasses(CogniteResourceList, exclude={PrincipalList})
    ],
)
def test_ensure_identifier_mixins(lst_cls: type[CogniteResourceList]) -> None:
    # TODO: Data Modeling uses "as_ids()" even though existing classes use the same for "integer internal ids"
    if "data_modeling" in str(lst_cls):
        return
    elif lst_cls in {DatapointsList, DatapointsArrayList}:  # May contain duplicates
        return

    bases = lst_cls.__mro__
    sig = inspect.signature(lst_cls._RESOURCE).parameters

    missing_id = "id" in sig and not (InternalIdTransformerMixin in bases or IdTransformerMixin in bases)
    missing_external_id = "external_id" in sig and not (
        ExternalIDTransformerMixin in bases or IdTransformerMixin in bases
    )

    # TODO: Make an instance ID mixin class, for now, we just ignore:
    # missing_instance_id = "instance_id" in sig and ...

    if missing_id and missing_external_id:
        pytest.fail(f"List class: '{lst_cls.__name__}' should inherit from IdTransformerMixin (id+external_id)")
    elif missing_id:
        pytest.fail(f"List class: '{lst_cls.__name__}' should inherit from InternalIdTransformerMixin")
    elif missing_external_id:
        pytest.fail(f"List class: '{lst_cls.__name__}' should inherit from ExternalIDTransformerMixin")
