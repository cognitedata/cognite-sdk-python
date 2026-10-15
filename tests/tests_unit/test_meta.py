from __future__ import annotations

import inspect
import re
import subprocess
import sys
from pathlib import Path

import pytest

from cognite.client._api.data_modeling.instances import _NodeOrEdgeApplyResultList
from cognite.client._api.iam.groups import _GroupListAdapter
from cognite.client._api_client import APIClient
from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
    CogniteResourceListWithClientRef,
    ExternalIDTransformerMixin,
    IdTransformerMixin,
    InternalIdTransformerMixin,
    WriteableCogniteResourceList,
    WriteableCogniteResourceListWithClientRef,
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
from cognite.client.data_classes.datapoints import Datapoint, DatapointsArrayList, DatapointsList
from cognite.client.data_classes.datapoints_subscriptions import SubscriptionDatapoints
from cognite.client.data_classes.geospatial import FeatureListCore
from cognite.client.data_classes.principals import PrincipalList
from cognite.client.data_classes.raw import RowCore, RowListCore
from cognite.client.utils._auxiliary import all_subclasses
from cognite.client.utils._url import NON_IDEMPOTENT_POST_ENDPOINT_REGEX_PATTERN
from tests.utils import all_concrete_subclasses, dict_without
from tests.utils import all_subclasses as all_non_test_subclasses


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
    for sub_cls in all_non_test_subclasses(cls):
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
        set(
            api._RESOURCE_PATH.split("/")[1]
            for api in all_non_test_subclasses(APIClient)
            if hasattr(api, "_RESOURCE_PATH")
        )
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


def test_dict_without() -> None:
    # Was previously a doctest on dict_without itself, but pytest's built-in --doctest-modules
    # collection of it was a source of rare, very confusing CI flakes. Moved here as a plain test.
    a = {"foo": "bar", "bar": "baz", "zip": "zap"}
    b = dict_without(a, {"foo", "bar"})
    assert b == {"zip": "zap"}

    b["foo"] = "not bar"
    # Should be unaffected: dict_without returns a copy
    assert a == {"foo": "bar", "bar": "baz", "zip": "zap"}


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
        # Internal DM adapter over both node/edge apply result types. Intentionally does not
        # use _RESOURCE because it validates mixed types in its own constructor.
        _NodeOrEdgeApplyResultList,
    }


@pytest.mark.parametrize("list_cls", sorted(all_non_test_subclasses(CogniteResourceList), key=str))
def test_all_list_classes_define_resource(list_cls: type, list_classes_without_resource: set[type]) -> None:
    # We need to check __dict__ (not hasattr) to avoid picking up _RESOURCE via MRO inheritance:
    if "_RESOURCE" not in list_cls.__dict__:
        assert list_cls in list_classes_without_resource, (
            f"{list_cls.__name__} does not define _RESOURCE — add it, "
            f"or add it to list_classes_without_resource with a comment explaining why"
        )
        return

    resource_cls = list_cls.__dict__["_RESOURCE"]
    assert resource_cls._LIST_CLASS is list_cls, (
        f"{list_cls.__name__}._RESOURCE = {resource_cls.__name__}, "
        f"but {resource_cls.__name__}._LIST_CLASS = {resource_cls._LIST_CLASS}"
    )


@pytest.mark.parametrize("list_cls", sorted(all_non_test_subclasses(CogniteResourceList), key=str))
def test_list_class_resource_back_reference(list_cls: type, list_classes_without_resource: set[type]) -> None:
    if list_cls in list_classes_without_resource or "_RESOURCE" not in list_cls.__dict__:
        return
    resource_cls = list_cls.__dict__["_RESOURCE"]
    assert resource_cls._LIST_CLASS is list_cls, (
        f"{resource_cls.__name__}._LIST_CLASS should be {list_cls.__name__}, got {resource_cls._LIST_CLASS}"
    )


def test_standalone_to_pandas_allowlist() -> None:
    # Resource classes that define their own to_pandas without a list class to delegate to.
    # These are intentional exceptions — domain-specific data shapes where the standard
    # "delegate to list type" pattern doesn't apply.
    #
    # NOTE: Adding a new class here requires justification on why list delegation is not appropriate:
    expected_standalone = {
        Datapoint,  # Time series datapoint — tabular layout, not a standard resource
        DiagramConvertItem,  # Embedded inside DiagramConvertResults, no standalone list type
        DiagramDetectItem,  # Embedded inside DiagramDetectResults, no standalone list type
        Instance,  # Abstract base; delegates at runtime via type(self)._LIST_CLASS
        RowCore,  # Raw table row; its to_pandas pivots columns, not a standard layout
        SubscriptionDatapoints,  # Datapoint subscription batch item, no standalone list type
        TypeInformation,  # DM type metadata embedded in query results, not a standard resource
        VisionResource,  # Abstract base for annotation geometry types (Point, Polygon, etc.)
    }
    actual_standalone = {
        cls
        for cls in all_subclasses(CogniteResource)  # type: ignore[type-abstract]
        if cls.__module__.startswith("cognite.client") and "to_pandas" in cls.__dict__ and cls._LIST_CLASS is None
    }
    unexpected = actual_standalone - expected_standalone
    assert not unexpected, (
        f"New resource class(es) with a standalone to_pandas found: "
        f"{sorted(c.__name__ for c in unexpected)}. "
        f"Either add a list class (with _RESOURCE so binding happens automatically), "
        f"or add to the allowlist above with a comment."
    )


def test_constants_are_importable() -> None:
    # Extractor utils using extractor_extensions/v1.py has a legit use case for needing the OMITTED singleton.
    # Thus this test is here to ensure we don't accidentally move it or break it.
    # Do not change this test without doing new major version release!!
    from cognite.client._constants import OMITTED, Omitted

    assert isinstance(OMITTED, Omitted)


SECRET_ISH_ATTRIBUTE_PATTERN = re.compile(r"secret|password|passphrase|nonce", re.IGNORECASE)


@pytest.fixture
def attributes_exempt_from_sensitive_marking() -> set[tuple[str, str]]:
    # (class name, attr) pairs whose name matches SECRET_ISH_ATTRIBUTE_PATTERN but that do not
    # themselves hold "credential material", so they should stay visible in debug logs and str() etc.
    #
    # NOTE: Adding an entry here requires justification on why the value is safe to log:
    return {
        # These four hold a NonceCredentials object, and it lists its own 'nonce' field, so the
        # secret is taken care of one level down:
        ("Transformation", "source_nonce"),
        ("Transformation", "destination_nonce"),
        ("TransformationWrite", "source_nonce"),
        ("TransformationWrite", "destination_nonce"),
    }


@pytest.mark.parametrize("cls", sorted(all_non_test_subclasses(CogniteResource), key=str))
def test_credential_attributes_are_marked_sensitive(
    cls: type[CogniteResource], attributes_exempt_from_sensitive_marking: set[tuple[str, str]]
) -> None:
    # Attempt to catch the next credential field that gets added without being listed in _SENSITIVE_FIELDS
    marked = cls._SENSITIVE_FIELDS
    for name in inspect.signature(cls.__init__).parameters:
        if name in {"self", "args", "kwargs"} or not SECRET_ISH_ATTRIBUTE_PATTERN.search(name):
            continue
        assert name in marked or (cls.__name__, name) in attributes_exempt_from_sensitive_marking, (
            f"{cls.__name__}.{name} looks like it holds a credential but is missing from "
            f"{cls.__name__}._SENSITIVE_FIELDS — add it there so it gets redacted logs. "
            f"If not a credential, add ({cls.__name__!r}, {name!r}) to the attributes_exempt_from_sensitive_marking"
            "allowlist with a comment explaining why the value is safe to log."
        )


FIND_MISSING_SENSITIVE_FIELDS_SCRIPT = """
import importlib, pkgutil, sys

import cognite.client.data_classes as data_classes  # all the registry itself imports
from cognite.client.utils._redaction import sensitive_fields

known = sensitive_fields()
visible = known.anywhere | {key for keys in known.by_discriminator.values() for key in keys}

for module_info in pkgutil.walk_packages(data_classes.__path__, f"{data_classes.__name__}."):
    importlib.import_module(module_info.name)

missing = {
    f"{obj.__module__}.{obj.__name__}.{field}"
    for name, module in list(sys.modules.items())
    if name.startswith("cognite.client.data_classes")
    for obj in vars(module).values()
    if isinstance(obj, type) and obj.__dict__.get("_SENSITIVE_FIELDS")
    for field in obj.__dict__["_SENSITIVE_FIELDS"]
    if field not in visible
}
if missing:
    print("\\n".join(sorted(missing)))
    raise SystemExit(1)
"""


def test_every_declaration_is_visible_from_the_package_root() -> None:
    """The registry only imports the 'cognite.client.data_classes' package, so every declared sensitive
    field must be reachable.

    A credential field declared in a module that the package root does not pull in would be missing from
    the registry, and so never redacted. This test guards against that. It has to run in a subprocess as
    by the time pytest 'gets here', everything is imported already, which hides the very thing we check.
    """
    result = subprocess.run(
        [sys.executable, "-c", FIND_MISSING_SENSITIVE_FIELDS_SCRIPT], capture_output=True, text=True
    )
    assert result.returncode == 0, (
        "These credential fields are not reachable from 'import cognite.client.data_classes', so "
        f"they would never be redacted:\n{result.stdout}{result.stderr}\n"
        "Re-export the module from the package root, or add imports directly inside sensitive_fields()."
    )
