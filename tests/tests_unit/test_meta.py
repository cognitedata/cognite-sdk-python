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
from cognite.client.utils._url import NON_IDEMPOTENT_POST_ENDPOINT_REGEX_PATTERN
from tests.utils import all_concrete_subclasses, all_subclasses


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


@pytest.fixture(scope="session")
def apis_matching_non_idempotent_POST_regex() -> set[str]:
    regex = NON_IDEMPOTENT_POST_ENDPOINT_REGEX_PATTERN
    return {part.removeprefix("^/").removeprefix("(").split("/")[0] for part in regex.pattern.split("|")}


@pytest.mark.parametrize(
    "api",
    sorted(  # why sorted? xdist needs order to be consistent between test workers
        set(api._RESOURCE_PATH.split("/")[1] for api in all_subclasses(APIClient) if hasattr(api, "_RESOURCE_PATH"))
    ),
)
def test_POST_endpoint_idempotency_vs_retries(api: str, apis_matching_non_idempotent_POST_regex: set[str]) -> None:
    # So you've added a new API to the SDK, but suddenly this test is failing - what's the deal?!
    # Answer the following:
    # Is this new API fully idempotent, i.e. can all its POST endpoints be safely retried automatically?
    # if yes  -> add the url base path allow list below.
    # if no -> look up 'NON_IDEMPOTENT_POST_ENDPOINT_REGEX_PATTERN' and add a regex for the relevant url path(s)
    # ... but always(!): add tests to TestRetryableEndpoints!
    idempotent_api_allow_list = {
        "groups",
        "models",
        "principals",
        "securitycategories",
        "sessions",  # TODO: Review this with the sessions team
        "workflows",
        "units",
    }
    treated_as_idempotent = api not in apis_matching_non_idempotent_POST_regex
    is_whitelisted_as_idempotent = api in idempotent_api_allow_list

    if treated_as_idempotent and not is_whitelisted_as_idempotent:
        pytest.fail(
            f"API '{api}' is treated as a fully idempotent API, but it's not whitelisted as idempotent."
            "If all the POST endpoints of this API are idempotent, you can whitelist it. If not you'll need to match"
            "the endpoints in NON_IDEMPOTENT_POST_ENDPOINT_REGEX_PATTERN and add tests to TestRetryableEndpoints!"
        )
    if not treated_as_idempotent and is_whitelisted_as_idempotent:
        pytest.fail(
            f"API '{api}' matches the non-idempotent regex, but it's also whitelisted as idempotent. "
            "You'll need to either remove it from the whitelist or from "
            "NON_IDEMPOTENT_POST_ENDPOINT_REGEX_PATTERN."
        )
