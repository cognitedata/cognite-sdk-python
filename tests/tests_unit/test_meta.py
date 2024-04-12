import inspect
from pathlib import Path

import pytest
import requests

from cognite.client._api_client import APIClient
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from tests.utils import all_subclasses

ALL_FILEPATHS = Path("cognite/client/").rglob("*.py")


def test_assert_no_root_init_file():
    # We have an implicit namespace package under the namespace package directory: 'cognite'.

    # From: https://packaging.python.org/en/latest/guides/packaging-namespace-packages/#native-namespace-packages
    # "It is extremely important that every distribution that uses the namespace package omits the __init__.py
    # or uses a pkgutil-style __init__.py. If any distribution does not, it will cause the namespace logic to
    # fail and the other sub-packages will not be importable"
    assert not Path("cognite/__init__.py").exists()


def test_ensure_all_files_use_future_annots():
    def keep(path):
        skip_list = [
            "_pb2.py",  # Auto-generated, dislikes changes ;)
        ]
        return all(skip not in str(path.as_posix()) for skip in skip_list)

    err_msg = "File: '{}' is missing 'from __future__ import annotations' at line=0"
    for filepath in filter(keep, ALL_FILEPATHS):
        with filepath.open("r") as file:
            # We just read the first line from each file:
            assert file.readline() == "from __future__ import annotations\n", err_msg.format(filepath)


@pytest.mark.parametrize("cls", [CogniteResource, CogniteResourceList])
def test_ensure_all_to_pandas_methods_use_snake_case(cls):
    err_msg = "Class: '{}' for method to_pandas does not default camel_case parameter to False."
    for sub_cls in all_subclasses(cls):
        if not (cls_method := getattr(sub_cls, "to_pandas", False)):
            continue
        if param := inspect.signature(cls_method).parameters.get("camel_case"):
            assert param.default is False, err_msg.format(sub_cls.__name__)


@pytest.fixture(scope="session")
def apis_with_post_method_retry_set():
    all_paths = set()
    (single_regex,) = APIClient._RETRYABLE_POST_ENDPOINT_REGEX_PATTERNS
    for api in filter(None, single_regex.split("^/")):
        base_path = api.split("/")[0]
        if base_path[0] == "]":
            continue
        elif base_path[0] == "(":
            all_paths.update(base_path[1:-1].split("|"))
        else:
            all_paths.add(base_path)
    return all_paths


@pytest.fixture(scope="session")
def apis_that_should_not_have_post_retry_rule():
    return set(
        [
            "groups",  # ☑️
            "securitycategories",  # ☑️
            "templategroups",  # Won't do: deprecated API
            "workflows",  # TODO later: Beta -> GA
        ]
    )


@pytest.mark.parametrize(
    "api",
    sorted(  # why sorted? xdist needs order to be consistent between test workers
        set(api._RESOURCE_PATH.split("/")[1] for api in all_subclasses(APIClient) if hasattr(api, "_RESOURCE_PATH"))
    ),
)
def test_all_base_api_paths_have_retry_or_specifically_no_set(
    api, apis_with_post_method_retry_set, apis_that_should_not_have_post_retry_rule
):
    # So you've added a new API to the SDK, but suddenly this test is failing - what's the deal?!
    # Answer the following:
    # Does this new API have POST methods that should be retried automatically?
    # if yes -> look up 'APIClient._RETRYABLE_POST_ENDPOINT_REGEX_PATTERNS' and add a regex for the url path
    # if no  -> add the url base path to the "okey-without-list" above: 'apis_that_should_not_have_post_retry_rule'
    # ...but always(!): add tests to TestRetryableEndpoints!
    has_retry = api in apis_with_post_method_retry_set
    no_retry_needed = api in apis_that_should_not_have_post_retry_rule
    assert has_retry or no_retry_needed

    # If the below check fails, it means an API that has been specifically except from POST retries now have
    # been given a retry regex anyway. Please update 'apis_that_should_not_have_post_retry_rule' above!
    assert not (has_retry and no_retry_needed)


@pytest.mark.xfail  # updated .proto-files not merged yet (StringDatapoint missing status)
def test_verify_proto_files(tmpdir):
    proto_url = "https://raw.githubusercontent.com/cognitedata/protobuf-files/master/v1/timeseries"

    remote_file1 = requests.get(f"{proto_url}/data_points.proto").text.splitlines()
    remote_file2 = requests.get(f"{proto_url}/data_point_list_response.proto").text.splitlines()

    assert remote_file1 == Path("cognite/client/_proto/data_points.proto").read_text().splitlines()
    assert remote_file2 == Path("cognite/client/_proto/data_point_list_response.proto").read_text().splitlines()
