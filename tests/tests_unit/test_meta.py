from __future__ import annotations

import inspect
from pathlib import Path

import pytest

from cognite.client._api_client import APIClient
from cognite.client._api.iam.groups import _GroupListAdapter
from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
    CogniteResourceListWithClientRef,
    ExternalIDTransformerMixin,
    IdTransformerMixin,
    InternalIdTransformerMixin,
    WriteableCogniteResourceList,
    WriteableCogniteResourceListWithClientRef,
    _RESOURCE_TO_LIST_CLASS,
)
from cognite.client.data_classes.annotation_types.primitives import VisionResource
from cognite.client.data_classes.contextualization import DiagramConvertItem, DiagramDetectItem
from cognite.client.data_classes.data_modeling.instances import (
    DataModelingInstancesList,
    EdgeListWithCursor,
    Instance,
    NodeListWithCursor,
    TypeInformation,
)
from cognite.client.data_classes.datapoints import Datapoint
from cognite.client.data_classes.datapoints_subscriptions import SubscriptionDatapoints
from cognite.client.data_classes.geospatial import FeatureListCore
from cognite.client.data_classes.raw import RowCore, RowListCore
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
        for list_cls in all_concrete_subclasses(
            CogniteResourceList, exclude={PrincipalList, CogniteResourceListWithClientRef}
        )
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
        "limits",
        "metering",
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


@pytest.fixture(scope="session")
def list_classes_without_resource() -> set[type]:
    return {
        # Abstract intermediates — _RESOURCE is defined by their concrete subclasses instead:
        CogniteResourceListWithClientRef,
        WriteableCogniteResourceList,
        WriteableCogniteResourceListWithClientRef,
        DataModelingInstancesList,
        FeatureListCore,
        RowListCore,
        # Cursor-bearing wrappers that inherit _RESOURCE from their parent:
        NodeListWithCursor,
        EdgeListWithCursor,
        # Internal adapter used only within the IAM groups API client:
        _GroupListAdapter,
    }


@pytest.mark.parametrize("list_cls", all_subclasses(CogniteResourceList))
def test_all_list_classes_define_resource(list_cls: type, list_classes_without_resource: set[type]) -> None:
    # We need to check __dict__ (not hasattr) to avoid picking up _RESOURCE via MRO inheritance:
    if "_RESOURCE" not in list_cls.__dict__:
        assert list_cls in list_classes_without_resource, (
            f"{list_cls.__name__} does not define _RESOURCE — add it, "
            f"or add it to list_classes_without_resource with a comment explaining why"
        )
        return

    resource_cls = list_cls._RESOURCE
    assert resource_cls in _RESOURCE_TO_LIST_CLASS, (
        f"{list_cls.__name__}._RESOURCE = {resource_cls.__name__} is not registered in _RESOURCE_TO_LIST_CLASS"
    )
    assert _RESOURCE_TO_LIST_CLASS[resource_cls] is list_cls, (
        f"{list_cls.__name__}._RESOURCE = {resource_cls.__name__}, "
        f"but _RESOURCE_TO_LIST_CLASS[{resource_cls.__name__}] = {_RESOURCE_TO_LIST_CLASS[resource_cls].__name__}"
    )


def test_standalone_to_pandas_allowlist() -> None:
    # Resource classes that define their own to_pandas without a registered list class to delegate to.
    # These are intentional exceptions — domain-specific data shapes where the standard
    # "delegate to list type" pattern doesn't apply. Adding a new class here requires justification.
    expected_standalone = {
        Datapoint,           # Time series datapoint — tabular layout, not a standard resource
        DiagramConvertItem,  # Embedded inside DiagramConvertResults, no standalone list type
        DiagramDetectItem,   # Embedded inside DiagramDetectResults, no standalone list type
        Instance,            # Abstract base; delegates at runtime via _RESOURCE_TO_LIST_CLASS[type(self)]
        RowCore,             # Raw table row — its to_pandas pivots columns, not a standard layout
        SubscriptionDatapoints,  # Datapoint subscription batch item, no standalone list type
        TypeInformation,     # DM type metadata embedded in query results, not a standard resource
        VisionResource,      # Abstract base for annotation geometry types (Point, Polygon, etc.)
    }
    import cognite.client.utils._auxiliary as aux

    actual_standalone = {
        cls
        for cls in aux.all_subclasses(CogniteResource)
        if cls.__module__.startswith("cognite.client")
        and "to_pandas" in cls.__dict__
        and cls not in _RESOURCE_TO_LIST_CLASS
    }
    unexpected = actual_standalone - expected_standalone
    assert not unexpected, (
        f"New resource class(es) with a standalone to_pandas found: "
        f"{sorted(c.__name__ for c in unexpected)}. "
        f"Either add a list class and register it, or add to the allowlist above with a comment."
    )


@pytest.mark.parametrize("resource_cls,list_cls", list(_RESOURCE_TO_LIST_CLASS.items()))
def test_registry_entries_are_consistent(resource_cls: type, list_cls: type) -> None:
    assert list_cls._RESOURCE is resource_cls, (
        f"_RESOURCE_TO_LIST_CLASS maps {resource_cls.__name__} → {list_cls.__name__}, "
        f"but {list_cls.__name__}._RESOURCE = {list_cls._RESOURCE}"
    )


def test_constants_are_importable() -> None:
    # Extractor utils using extractor_extensions/v1.py has a legit use case for needing the OMITTED singleton.
    # Thus this test is here to ensure we don't accidentally move it or break it.
    # Do not change this test without doing new major version release!!
    from cognite.client._constants import OMITTED, Omitted

    assert isinstance(OMITTED, Omitted)
