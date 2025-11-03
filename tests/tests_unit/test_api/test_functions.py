from __future__ import annotations

import ast
import io
import json
import os
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import AsyncMock, MagicMock, patch
from zipfile import ZipFile

import pytest
from _pytest._py.path import LocalPath
from _pytest.monkeypatch import MonkeyPatch
from pytest_httpx import HTTPXMock

from cognite.client import ClientConfig, CogniteClient
from cognite.client._api.functions import (
    _extract_requirements_from_doc_string,
    _extract_requirements_from_file,
    _get_fn_docstring_requirements,
    _validate_and_parse_requirements,
    _validate_function_handle,
    get_handle_function_node,
    validate_function_folder,
)
from cognite.client.credentials import OAuthClientCredentials, Token
from cognite.client.data_classes import (
    ClientCredentials,
    FileMetadata,
    Function,
    FunctionCall,
    FunctionCallList,
    FunctionCallLog,
    FunctionList,
    FunctionSchedule,
    FunctionSchedulesList,
    FunctionScheduleWrite,
    FunctionsLimits,
)
from cognite.client.data_classes.functions import FunctionsStatus
from cognite.client.exceptions import CogniteAPIError
from tests.tests_unit.conftest import DefaultResourceGenerator
from tests.utils import get_or_raise, get_url, jsgz_load

if TYPE_CHECKING:
    from pytest_httpx import HTTPXMock

    from cognite.client import AsyncCogniteClient, CogniteClient

FUNCTION_ID = 1234
CALL_ID = 5678

EXAMPLE_FUNCTION = {
    "id": FUNCTION_ID,
    "name": "myfunction",
    "externalId": f"func-no-{FUNCTION_ID}",
    "description": "my fabulous function",
    "owner": "ola.normann@cognite.com",
    "status": "Ready",
    "fileId": 1234,
    "functionPath": "handler.py",
    "createdTime": 1585662507939,
    "secrets": {"key1": "***", "key2": "***"},
    "envVars": {"env1": "foo", "env2": "bar"},
    "cpu": 0.25,
    "memory": 1,
    "runtime": "py311",
    "runtimeVersion": "Python 3.11.11",
    "lastCalled": 1585925306822,
}

# Example function for creation scenarios - newly created functions haven't been called yet
EXAMPLE_FUNCTION_CREATED = {
    "id": FUNCTION_ID,
    "name": "myfunction",
    "externalId": f"func-no-{FUNCTION_ID}",
    "description": "my fabulous function",
    "owner": "ola.normann@cognite.com",
    "status": "Ready",
    "fileId": 1234,
    "functionPath": "handler.py",
    "createdTime": 1585662507939,
    "secrets": {"key1": "***", "key2": "***"},
    "envVars": {"env1": "foo", "env2": "bar"},
    "cpu": 0.25,
    "memory": 1,
    "runtime": "py311",
    "runtimeVersion": "Python 3.11.11",
    # lastCalled field is omitted when None (not included in API response)
}
CALL_RUNNING = {
    "id": CALL_ID,
    "startTime": 1585925306822,
    "endTime": 1585925310822,
    "status": "Running",
    "functionId": FUNCTION_ID,
}
CALL_COMPLETED = {
    "id": CALL_ID,
    "startTime": 1585925306822,
    "endTime": 1585925310822,
    "status": "Completed",
    "functionId": FUNCTION_ID,
}
CALL_FAILED = {
    "id": CALL_ID,
    "startTime": 1585925306822,
    "endTime": 1585925310822,
    "status": "Failed",
    "functionId": FUNCTION_ID,
    "error": {"message": "some message", "trace": "some stack trace"},
}
CALL_TIMEOUT = {
    "id": CALL_ID,
    "startTime": 1585925306822,
    "endTime": 1585925310822,
    "status": "Timeout",
    "functionId": FUNCTION_ID,
}
CALL_SCHEDULED = {
    "id": CALL_ID,
    "startTime": 1585925306822,
    "endTime": 1585925310822,
    "scheduledTime": 1585925306000,
    "status": "Completed",
    "scheduleId": 6789,
    "functionId": FUNCTION_ID,
}


@pytest.fixture
def mock_functions_filter_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
    response_body = {"items": [EXAMPLE_FUNCTION]}

    url = get_url(async_client.functions, "/functions/list")
    httpx_mock.add_response(method="POST", url=url, status_code=200, json=response_body)

    return httpx_mock


@pytest.fixture
def mock_functions_retrieve_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
    response_body = {"items": [EXAMPLE_FUNCTION]}

    url = get_url(async_client.functions, "/functions/byids")
    httpx_mock.add_response(method="POST", url=url, status_code=200, json=response_body)

    return httpx_mock


@pytest.fixture
def mock_functions_create_response(httpx_mock: HTTPXMock, async_client: AsyncCogniteClient) -> HTTPXMock:
    files_response_body = {
        "name": "myfunction",
        "id": FUNCTION_ID,
        "uploaded": True,
        "createdTime": 1585662507939,
        "lastUpdatedTime": 1585662507939,
        "uploadUrl": "https://upload.here",
    }

    files_url = get_url(async_client.files, "/files?overwrite=false")
    files_byids_url = get_url(async_client.files, "/files/byids")
    functions_url = get_url(async_client.functions, "/functions")

    httpx_mock.add_response(method="POST", url=files_url, status_code=201, json=files_response_body, is_optional=True)
    httpx_mock.add_response(method="PUT", url="https://upload.here", status_code=201, is_optional=True)
    httpx_mock.add_response(
        method="POST", url=files_byids_url, status_code=201, json={"items": [files_response_body]}, is_optional=True
    )
    # Use EXAMPLE_FUNCTION_CREATED for creation scenarios - newly created functions haven't been called yet
    httpx_mock.add_response(
        method="POST", url=functions_url, status_code=201, json={"items": [EXAMPLE_FUNCTION_CREATED]}, is_optional=True
    )
    return httpx_mock


@pytest.fixture
def mock_file_not_uploaded(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
    files_response_body = {
        "name": "myfunction",
        "id": FUNCTION_ID,
        "uploaded": False,
        "createdTime": 1585662507939,
        "lastUpdatedTime": 1585662507939,
        "uploadUrl": "https://upload.here",
    }

    files_byids_url = get_url(async_client.files, "/files/byids")

    httpx_mock.add_response(method="POST", url=files_byids_url, status_code=201, json={"items": [files_response_body]})
    return httpx_mock


@pytest.fixture
def mock_functions_delete_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
    url = get_url(async_client.functions, "/functions/delete")
    httpx_mock.add_response(method="POST", url=url, status_code=200, json={})

    return httpx_mock


@pytest.fixture
def mock_functions_call_responses(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
    url = get_url(async_client.functions, f"/functions/{FUNCTION_ID}/call")
    httpx_mock.add_response(method="POST", url=url, status_code=201, json=CALL_RUNNING)

    url = get_url(async_client.functions, f"/functions/{FUNCTION_ID}/calls/byids")
    httpx_mock.add_response(method="POST", url=url, status_code=200, json={"items": [CALL_COMPLETED]})

    return httpx_mock


@pytest.fixture
def mock_sessions_bad_request_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
    url = get_url(async_client.functions) + "/sessions"
    httpx_mock.add_response(method="POST", url=url, status_code=403)
    return httpx_mock


@pytest.fixture
def mock_functions_call_by_external_id_responses(
    mock_functions_retrieve_response: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
    httpx_mock = mock_functions_retrieve_response

    url = get_url(async_client.functions, f"/functions/{FUNCTION_ID}/call")
    httpx_mock.add_response(method="POST", url=url, status_code=201, json=CALL_RUNNING)

    url = get_url(async_client.functions, f"/functions/{FUNCTION_ID}/calls/byids")
    httpx_mock.add_response(method="POST", url=url, status_code=200, json={"items": [CALL_COMPLETED]})

    return httpx_mock


@pytest.fixture
def mock_functions_call_failed_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
    url = get_url(async_client.functions, f"/functions/{FUNCTION_ID}/call")
    httpx_mock.add_response(method="POST", url=url, status_code=201, json=CALL_FAILED)

    return httpx_mock


@pytest.fixture
def mock_functions_call_timeout_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
    url = get_url(async_client.functions, f"/functions/{FUNCTION_ID}/call")
    httpx_mock.add_response(method="POST", url=url, status_code=201, json=CALL_TIMEOUT)

    return httpx_mock


@pytest.fixture
def function_handle() -> Callable:
    def handle(data: Any, client: Any, secrets: Any) -> None:
        pass

    return handle


@pytest.fixture
def function_handle_with_reqs() -> Callable:
    def handle(data: Any, client: Any, secrets: Any) -> None:
        """
        [requirements]
        pandas
        [/requirements]
        """

    return handle


@pytest.fixture
def function_handle_illegal_name() -> Callable:
    def func(data: Any, client: Any, secrets: Any) -> None:
        pass

    return func


@pytest.fixture
def function_handle_illegal_argument() -> Callable:
    def handle(client: Any, input: Any) -> None:
        pass

    return handle


@pytest.fixture
def function_handle_as_variable() -> Any:
    """Fixture for handle as variable assignment (callable) with valid arguments."""

    def inner_handle(data: Any, client: Any, secrets: Any) -> dict[str, str]:
        return {"result": "success"}

    # This simulates handle = some_callable pattern
    # Set the __name__ to 'handle' to simulate variable assignment
    inner_handle.__name__ = "handle"
    return inner_handle


@pytest.fixture
def mock_function_calls_filter_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
    response_body = {"items": [CALL_COMPLETED, CALL_SCHEDULED]}
    url = get_url(async_client.functions, f"/functions/{FUNCTION_ID}/calls/list")
    httpx_mock.add_response(method="POST", url=url, status_code=200, json=response_body)

    return httpx_mock


@pytest.fixture
def cognite_client_with_client_credentials_flow(httpx_mock: HTTPXMock) -> CogniteClient:
    # We allow the mock to pass isinstance checks
    (credentials := MagicMock()).__class__ = OAuthClientCredentials  # type: ignore[assignment]

    credentials.token_url = ("https://bla",)
    credentials.client_id = ("bla",)
    credentials.client_secret = ("bla",)
    credentials.scopes = ["bla"]
    credentials.authorization_header.return_value = ("Authorization", "Bearer bla")

    return CogniteClient(ClientConfig(client_name="any", project="dummy", credentials=credentials))


@pytest.fixture
def cognite_client_with_token() -> CogniteClient:
    return CogniteClient(ClientConfig(client_name="any", project="dummy", credentials=Token("aabbccddeeffgg")))


@pytest.fixture
def mock_function_calls_filter_response_with_limit(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
    response_body = {"items": [CALL_COMPLETED, CALL_SCHEDULED]}
    url = get_url(async_client.functions, f"/functions/{FUNCTION_ID}/calls/list")
    httpx_mock.add_response(method="POST", url=url, status_code=200, json=response_body)

    return httpx_mock


@pytest.fixture
def mock_functions_limit_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> dict[str, Any]:
    response_body = {
        "timeoutMinutes": 10,
        "cpuCores": {"min": 0.1, "max": 0.6, "default": 0.25},
        "memoryGb": {"min": 0.1, "max": 2.5, "default": 1.0},
        "responseSizeMb": 1,
        "runtimes": ["py310", "py311", "py312"],
    }
    url = get_url(async_client.functions, "/functions/limits")
    httpx_mock.add_response(method="GET", url=url, status_code=200, json=response_body)

    return response_body


@pytest.fixture
def mock_functions_status_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> dict[str, Any]:
    response_body = {"status": "IN PROGRESS"}
    url = get_url(async_client.functions, "/functions/status")
    httpx_mock.add_response(method="POST", url=url, status_code=200, json=response_body)
    httpx_mock.add_response(method="GET", url=url, status_code=200, json=response_body)

    return response_body


@pytest.fixture
def mock_file_create_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
    response_body = {
        "externalId": "string",
        "name": "string",
        "source": "string",
        "mimeType": "string",
        "metadata": {},
        "assetIds": [1],
        "labels": [{"externalId": "WELL LOG"}],
        "id": 1,
        "uploaded": False,
        "uploadedTime": 0,
        "createdTime": 0,
        "lastUpdatedTime": 0,
        "uploadUrl": "https://upload.here",
    }
    httpx_mock.add_response(
        method="POST",
        url=get_url(async_client.files, "/files?overwrite=false"),
        status_code=200,
        json=response_body,
        is_optional=True,
    )
    httpx_mock.add_response(
        method="PUT", url="https://upload.here", status_code=200, json=response_body, is_optional=True
    )
    return httpx_mock


class TestFunctionsAPI:
    @pytest.mark.parametrize(
        "function_folder, function_path, exception",
        [
            (".", "handler.py", None),
            ("function_code", "./handler.py", None),
            ("bad_function_code", "handler.py", FileNotFoundError),
            ("bad_function_code2", "handler.py", TypeError),
        ],
    )
    def test_validate_folder(self, function_folder: str, function_path: str, exception: type[Exception] | None) -> None:
        folder = os.path.join(os.path.dirname(__file__), "function_test_resources", function_folder)
        if exception is None:
            validate_function_folder(folder, function_path, skip_folder_validation=True)
        else:
            with pytest.raises(exception):
                validate_function_folder(folder, function_path, skip_folder_validation=True)

    @pytest.mark.parametrize(
        "function_folder, function_path, exception",
        [
            ("./good_absolute_import/", "my_functions/handler.py", None),
            ("bad_absolute_import", "extra_root_folder/my_functions/handler.py", ModuleNotFoundError),
            ("relative_imports", "my_functions/good_relative_import.py", None),
            ("relative_imports", "bad_relative_import.py", ImportError),
        ],
    )
    def test_imports_in_validate_folder(
        self, function_folder: str, function_path: str, exception: type[Exception] | None
    ) -> None:
        folder = os.path.join(os.path.dirname(__file__), "function_test_resources", function_folder)
        if exception is None:
            validate_function_folder(folder, function_path, skip_folder_validation=False)
        else:
            with pytest.raises(exception):
                validate_function_folder(folder, function_path, skip_folder_validation=False)

    @pytest.mark.parametrize(
        "function_folder, function_name",
        [
            ("function_code", "function_w_no_requirements"),
            ("function_code_with_requirements", "function_w_requirements"),
        ],
    )
    @pytest.mark.usefixtures("mock_file_create_response")
    async def test_zip_and_upload_folder(
        self, function_folder: str, function_name: str, async_client: AsyncCogniteClient
    ) -> None:
        folder = os.path.join(os.path.dirname(__file__), "function_test_resources", function_folder)
        await async_client.functions._zip_and_upload_folder(folder, function_name)

    @patch("cognite.client._api.functions.MAX_RETRIES", 1)
    def test_create_function_with_file_not_uploaded(
        self, mock_file_not_uploaded: HTTPXMock, cognite_client: CogniteClient
    ) -> None:
        with pytest.raises(RuntimeError):
            cognite_client.functions.create(name="myfunction", file_id=123)

    def test_create_with_path(self, mock_functions_create_response: HTTPXMock, cognite_client: CogniteClient) -> None:
        folder = os.path.join(os.path.dirname(__file__), "function_test_resources", "function_code")
        res = cognite_client.functions.create(name="myfunction", folder=folder, function_path="handler.py")

        assert isinstance(res, Function)
        assert EXAMPLE_FUNCTION_CREATED == res.dump(camel_case=True)

    def test_create_with_file_id(
        self, mock_functions_create_response: HTTPXMock, cognite_client: CogniteClient
    ) -> None:
        res = cognite_client.functions.create(name="myfunction", file_id=1234)

        assert isinstance(res, Function)
        assert EXAMPLE_FUNCTION_CREATED == res.dump(camel_case=True)

    def test_create_with_function_handle(
        self, mock_functions_create_response: HTTPXMock, function_handle: Any, cognite_client: CogniteClient
    ) -> None:
        res = cognite_client.functions.create(name="myfunction", function_handle=function_handle)

        assert isinstance(res, Function)
        assert EXAMPLE_FUNCTION_CREATED == res.dump(camel_case=True)

    def test_create_with_function_handle_with_illegal_name_raises(
        self, function_handle_illegal_name: Any, cognite_client: CogniteClient
    ) -> None:
        with pytest.raises(TypeError):
            cognite_client.functions.create(name="myfunction", function_handle=function_handle_illegal_name)

    def test_create_with_function_handle_with_illegal_argument_raises(
        self, function_handle_illegal_argument: Any, cognite_client: CogniteClient
    ) -> None:
        with pytest.raises(TypeError):
            cognite_client.functions.create(name="myfunction", function_handle=function_handle_illegal_argument)

    def test_create_with_function_handle_as_variable_accepts(
        self, mock_functions_create_response: Any, function_handle_as_variable: Any, cognite_client: CogniteClient
    ) -> None:
        """Test that handle as variable assignment (callable) is accepted."""
        res = cognite_client.functions.create(name="myfunction", function_handle=function_handle_as_variable)

        assert isinstance(res, Function)
        assert EXAMPLE_FUNCTION_CREATED == res.dump(camel_case=True)

    def test_create_with_function_handle_assignment_from_folder(
        self, mock_functions_create_response: Any, cognite_client: CogniteClient
    ) -> None:
        """Test that handle as variable assignment works when loading from folder."""
        folder = os.path.join(os.path.dirname(__file__), "function_test_resources", "function_with_handle_assignment")
        res = cognite_client.functions.create(name="myfunction", folder=folder, function_path="handler.py")

        assert isinstance(res, Function)
        assert EXAMPLE_FUNCTION_CREATED == res.dump(camel_case=True)

    def test_create_with_function_handle_assignment_invalid_args_from_folder_accepts(
        self, mock_functions_create_response: Any, cognite_client: CogniteClient
    ) -> None:
        """Test that handle as variable assignment with invalid args is accepted (no arg validation)."""
        # This folder contains a handle assignment where the underlying function has invalid arguments
        # This should NOT raise TypeError because callable variables skip argument validation at create time
        folder = os.path.join(
            os.path.dirname(__file__), "function_test_resources", "function_with_handle_assignment_invalid_args"
        )
        res = cognite_client.functions.create(name="myfunction", folder=folder, function_path="handler.py")

        assert isinstance(res, Function)
        assert EXAMPLE_FUNCTION_CREATED == res.dump(camel_case=True)

    def test_create_with_function_handle_annotated_assignment_from_folder(
        self, mock_functions_create_response: Any, cognite_client: CogniteClient
    ) -> None:
        """Test that handle as annotated variable assignment works when loading from folder."""
        folder = os.path.join(
            os.path.dirname(__file__), "function_test_resources", "function_with_annotated_handle_assignment"
        )
        res = cognite_client.functions.create(name="myfunction", folder=folder, function_path="handler.py")

        assert isinstance(res, Function)
        assert EXAMPLE_FUNCTION_CREATED == res.dump(camel_case=True)

    def test_validate_function_handle_with_complex_assignment_target_raises(self) -> None:
        # Create AST node for: obj.handle = some_callable
        code = "obj.handle = lambda: None"
        tree = ast.parse(code)
        assign_node = tree.body[0]
        with pytest.raises(TypeError, match="Assignment target must be a simple name"):
            _validate_function_handle(assign_node)  # type: ignore[arg-type]

    def test_validate_function_handle_with_complex_annotated_assignment_target_raises(self) -> None:
        # Create AST node for: obj.handle: Callable = some_callable
        code = "obj.handle: Callable = lambda: None"
        tree = ast.parse(code)
        ann_assign_node = tree.body[0]
        with pytest.raises(TypeError, match="Assignment target must be a simple name"):
            _validate_function_handle(ann_assign_node)  # type: ignore[arg-type]

    def test_create_with_handle_function_and_file_id_raises(
        self, mock_functions_create_response: HTTPXMock, function_handle: Any, cognite_client: CogniteClient
    ) -> None:
        with pytest.raises(TypeError):
            cognite_client.functions.create(name="myfunction", function_handle=function_handle, file_id=1234)

    def test_create_with_path_and_file_id_raises(
        self, mock_functions_create_response: HTTPXMock, cognite_client: CogniteClient
    ) -> None:
        with pytest.raises(TypeError):
            cognite_client.functions.create(
                name="myfunction", folder="some/folder", file_id=1234, function_path="handler.py"
            )

    def test_create_with_cpu_and_memory(
        self, mock_functions_create_response: HTTPXMock, cognite_client: CogniteClient
    ) -> None:
        res = cognite_client.functions.create(name="myfunction", file_id=1234, cpu=0.2, memory=1.0)

        assert isinstance(res, Function)
        assert EXAMPLE_FUNCTION_CREATED == res.dump(camel_case=True)

    def test_create_with_cpu_not_float_raises(
        self, mock_functions_create_response: HTTPXMock, cognite_client: CogniteClient
    ) -> None:
        with pytest.raises(TypeError):
            cognite_client.functions.create(name="myfunction", file_id=1234, cpu="0.2")  # type: ignore[arg-type]

    def test_create_with_memory_not_float_raises(
        self, mock_functions_create_response: HTTPXMock, cognite_client: CogniteClient
    ) -> None:
        with pytest.raises(TypeError):
            cognite_client.functions.create(name="myfunction", file_id=1234, memory="0.5")  # type: ignore[arg-type]

    def test_create_upload_with_data_set_id(
        self, mock_functions_create_response: HTTPXMock, cognite_client: CogniteClient, function_handle: Callable
    ) -> None:
        cognite_client.files = MagicMock(spec=cognite_client.files)

        def mock_upload_bytes(*args: Any, **kwargs: Any) -> FileMetadata:
            assert kwargs.get("data_set_id") == 999
            return DefaultResourceGenerator.file_metadata(id=FUNCTION_ID)

        cognite_client.files.upload_bytes.side_effect = mock_upload_bytes

        cognite_client.functions.create(name="myfunction", function_handle=function_handle, data_set_id=999)

    def test_delete_single_id(self, mock_functions_delete_response: HTTPXMock, cognite_client: CogniteClient) -> None:
        _ = cognite_client.functions.delete(id=1)
        assert {"items": [{"id": 1}]} == jsgz_load(mock_functions_delete_response.get_requests()[0].content)

    def test_delete_single_external_id(
        self, mock_functions_delete_response: HTTPXMock, cognite_client: CogniteClient
    ) -> None:
        _ = cognite_client.functions.delete(external_id="func1")
        assert {"items": [{"externalId": "func1"}]} == jsgz_load(
            mock_functions_delete_response.get_requests()[0].content
        )

    def test_delete_multiple_id_and_multiple_external_id(
        self, mock_functions_delete_response: HTTPXMock, cognite_client: CogniteClient
    ) -> None:
        _ = cognite_client.functions.delete(id=[1, 2, 3], external_id=["func1", "func2"])
        assert {
            "items": [{"id": 1}, {"id": 2}, {"id": 3}, {"externalId": "func1"}, {"externalId": "func2"}]
        } == jsgz_load(mock_functions_delete_response.get_requests()[0].content)

    def test_list(self, mock_functions_filter_response: HTTPXMock, cognite_client: CogniteClient) -> None:
        res = cognite_client.functions.list()

        assert isinstance(res, FunctionList)
        assert [EXAMPLE_FUNCTION] == res.dump(camel_case=True)

    def test_list_with_limits(self, mock_functions_filter_response: HTTPXMock, cognite_client: CogniteClient) -> None:
        res = cognite_client.functions.list(limit=1)
        assert isinstance(res, FunctionList)
        assert len(res) == 1

    def test_retrieve_by_id(self, mock_functions_retrieve_response: HTTPXMock, cognite_client: CogniteClient) -> None:
        res = cognite_client.functions.retrieve(id=1)
        assert isinstance(res, Function)
        assert EXAMPLE_FUNCTION == res.dump(camel_case=True)

    def test_retrieve_by_external_id(
        self, mock_functions_retrieve_response: HTTPXMock, cognite_client: CogniteClient
    ) -> None:
        res = cognite_client.functions.retrieve(external_id="func1")
        assert isinstance(res, Function)
        assert EXAMPLE_FUNCTION == res.dump(camel_case=True)

    def test_retrieve_by_id_and_external_id_raises(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(ValueError):
            cognite_client.functions.retrieve(id=1, external_id="func1")

    def test_retrieve_multiple_by_ids(
        self, mock_functions_retrieve_response: HTTPXMock, cognite_client: CogniteClient
    ) -> None:
        res = cognite_client.functions.retrieve_multiple(ids=[1])
        assert isinstance(res, FunctionList)
        assert [EXAMPLE_FUNCTION] == res.dump(camel_case=True)

    def test_retrieve_multiple_by_external_ids(
        self, mock_functions_retrieve_response: HTTPXMock, cognite_client: CogniteClient
    ) -> None:
        res = cognite_client.functions.retrieve_multiple(external_ids=["func1"])
        assert isinstance(res, FunctionList)
        assert [EXAMPLE_FUNCTION] == res.dump(camel_case=True)

    def test_retrieve_multiple_by_ids_and_external_ids(
        self, mock_functions_retrieve_response: HTTPXMock, cognite_client: CogniteClient
    ) -> None:
        res = cognite_client.functions.retrieve_multiple(ids=[1], external_ids=["func1"])
        assert isinstance(res, FunctionList)
        assert [EXAMPLE_FUNCTION] == res.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_sessions_bad_request_response")
    def test_function_call_with_failing_client_credentials_flow(
        self, cognite_client_with_client_credentials_flow: CogniteClient
    ) -> None:
        with pytest.raises(CogniteAPIError) as excinfo:
            cognite_client_with_client_credentials_flow.functions.call(id=FUNCTION_ID)
        assert excinfo.value.code == 403

    @pytest.mark.usefixtures("mock_sessions_bad_request_response")
    def test_function_call_with_failing_token_exchange_flow(self, cognite_client_with_token: CogniteClient) -> None:
        with pytest.raises(CogniteAPIError) as excinfo:
            cognite_client_with_token.functions.call(id=FUNCTION_ID)
        assert excinfo.value.code == 403

    def test_functions_limits_endpoint(
        self, mock_functions_limit_response: dict[str, Any], cognite_client: CogniteClient
    ) -> None:
        res = cognite_client.functions.limits()
        assert isinstance(res, FunctionsLimits)
        assert mock_functions_limit_response == res.dump(camel_case=True)

    def test_functions_status_endpoint(
        self, mock_functions_status_response: dict[str, Any], cognite_client: CogniteClient
    ) -> None:
        res = cognite_client.functions.activate()
        assert isinstance(res, FunctionsStatus)
        assert mock_functions_status_response == res.dump(camel_case=True)

        res = cognite_client.functions.status()
        assert isinstance(res, FunctionsStatus)
        assert mock_functions_status_response == res.dump(camel_case=True)


class TestRequirementsParser:
    """Test extraction of requirements.txt from docstring in handle-function"""

    def test_validate_requirements(self) -> None:
        parsed = _validate_and_parse_requirements(["asyncio==3.4.3", "numpy==1.23.0", "pandas==1.4.3"])
        assert parsed == ["asyncio==3.4.3", "numpy==1.23.0", "pandas==1.4.3"]

    def test_validate_requirements_error(self) -> None:
        reqs = [["asyncio=3.4.3"], ["num py==1.23.0"], ["pandas==1.4.3 python_version=='3.8'"]]
        for req in reqs:
            with pytest.raises(Exception):
                _validate_and_parse_requirements(req)

    def test_get_requirements_handle(self) -> None:
        def fn() -> None:
            """
            [requirements]
            asyncio
            [/requirements]
            """
            return None

        assert _get_fn_docstring_requirements(fn) == ["asyncio"]

    def test_get_requirements_handle_error(self) -> None:
        def fn() -> None:
            return None

        assert _get_fn_docstring_requirements(fn) == []

    def test_get_requirements_handle_no_docstr(self) -> None:
        def fn() -> None:
            """
            [requirements]
            asyncio=3.4.3
            [/requirements]
            """
            return None

        with pytest.raises(Exception):
            _get_fn_docstring_requirements(fn)

    def test_get_requirements_handle_no_reqs(self) -> None:
        def fn() -> None:
            """
            [requirements]
            [/requirements]
            """
            return None

        assert _get_fn_docstring_requirements(fn) == []

    def test_extract_requirements_from_file(self, tmpdir: LocalPath) -> None:
        req = "somepackage == 3.8.1"
        file = os.path.join(tmpdir, "requirements.txt")
        with open(file, "w+") as f:
            f.writelines("\n".join(["# this should not be included", "     " + req]))
        reqs = _extract_requirements_from_file(file_name=file)
        assert type(reqs) is list
        assert len(reqs) == 1
        assert req in reqs

    def test_extract_requirements_from_doc_string(self) -> None:
        req_mock = '[requirements]\nSomePackage==3.4.3, >3.4.1; python_version=="3.7"\nSomePackage==21.4.0; python_version=="3.7" and python_full_version<"3.0.0" or python_full_version>="3.5.0" and python_version>="3.7"\nSomePackage==2022.6.15; python_version>="3.8" and python_version<"4"\ncSomePackage==1.15.0; python_version>="3.6"\n[/requirements]\n'
        doc_string_mock = "this should not be included\n" + req_mock + "neither should this\n"
        expected = req_mock.splitlines()[1:-1]
        assert _extract_requirements_from_doc_string(doc_string_mock) == expected

    def test_extract_requirements_from_doc_string_empty(self) -> None:
        doc_string = "[requirements]\n[/requirements]\n"
        assert _extract_requirements_from_doc_string(doc_string) == []

    def test_extract_requirements_from_doc_string_no_defined(self, async_client: AsyncCogniteClient) -> None:
        doc_string = "no requirements here"
        assert _extract_requirements_from_doc_string(doc_string) is None


@pytest.fixture
def mock_function_calls_retrieve_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
    response_body = CALL_COMPLETED
    url = get_url(async_client.functions, f"/functions/{FUNCTION_ID}/calls/byids")
    httpx_mock.add_response(method="POST", url=url, status_code=200, json={"items": [response_body]})

    return httpx_mock


@pytest.fixture
def mock_function_call_response_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> dict[str, Any]:
    response_body = {"callId": CALL_ID, "functionId": 1234, "response": {"key": "value"}}
    url = get_url(async_client.functions, f"/functions/{FUNCTION_ID}/calls/{CALL_ID}/response")
    httpx_mock.add_response(method="GET", url=url, status_code=200, json=response_body, is_optional=True)

    return response_body


@pytest.fixture
def mock_function_call_logs_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> dict[str, Any]:
    response_body = {
        "items": [
            {"timestamp": 1585925306822, "message": "message 1"},
            {"timestamp": 1585925310822, "message": "message 2"},
        ]
    }
    url = get_url(async_client.functions, f"/functions/{FUNCTION_ID}/calls/{CALL_ID}/logs")
    httpx_mock.add_response(method="GET", url=url, status_code=200, json=response_body, is_optional=True)

    return response_body


SCHEDULE_WITH_FUNCTION_EXTERNAL_ID = {
    "createdTime": 123,
    "cronExpression": "*/5 * * * *",
    "description": "Hi",
    "functionExternalId": "my-func",
    "id": 8012683333564363,
    "name": "my-schedule",
    "when": "Every 5 minutes",
    "sessionId": 12345,
}

SCHEDULE_WITH_FUNCTION_ID_AND_SESSION = {
    "createdTime": 456,
    "cronExpression": "*/5 * * * *",
    "description": "Hi",
    "functionId": FUNCTION_ID,
    "id": 8012683333564363,
    "name": "my-schedule",
    "when": "Every 5 minutes",
    "sessionId": 12345,
}


@pytest.fixture
def mock_filter_function_schedules_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
    url = get_url(async_client.functions, "/functions/schedules/list")
    httpx_mock.add_response(
        method="POST", url=url, status_code=200, json={"items": [SCHEDULE_WITH_FUNCTION_EXTERNAL_ID]}
    )

    return httpx_mock


@pytest.fixture
def mock_function_schedules_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
    url = get_url(async_client.functions, "/functions/schedules")
    httpx_mock.add_response(
        method="GET", url=url, status_code=200, json={"items": [SCHEDULE_WITH_FUNCTION_EXTERNAL_ID]}, is_optional=True
    )
    httpx_mock.add_response(
        method="POST", url=url, status_code=200, json={"items": [SCHEDULE_WITH_FUNCTION_EXTERNAL_ID]}
    )
    return httpx_mock


@pytest.fixture
def mock_function_schedules_response_with_xid(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
    # Creating a new schedule first needs a session (to pass the nonce):
    httpx_mock.add_response(
        method="POST",
        url=get_url(async_client.functions, "/sessions"),
        status_code=200,
        json={"items": [{"nonce": "very noncy", "id": 123, "status": "mocky"}]},
    )

    schedule_url = get_url(async_client.functions, "/functions/schedules")
    httpx_mock.add_response(
        method="POST", url=schedule_url, status_code=200, json={"items": [SCHEDULE_WITH_FUNCTION_EXTERNAL_ID]}
    )

    retrieve_url = get_url(async_client.functions, "/functions/byids")
    httpx_mock.add_response(method="POST", url=retrieve_url, status_code=200, json={"items": [EXAMPLE_FUNCTION]})

    return httpx_mock


@pytest.fixture
def mock_function_schedules_retrieve_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
    url = get_url(async_client.functions, "/functions/schedules/byids")
    httpx_mock.add_response(
        method="POST", url=url, status_code=200, json={"items": [SCHEDULE_WITH_FUNCTION_EXTERNAL_ID]}
    )
    return httpx_mock


@pytest.fixture
def mock_function_schedules_delete_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
    url = get_url(async_client.functions, "/functions/schedules/delete")
    httpx_mock.add_response(method="POST", url=url, status_code=200, json={})
    return httpx_mock


@pytest.fixture
def mock_schedule_get_data_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
    schedule_id = SCHEDULE_WITH_FUNCTION_ID_AND_SESSION["id"]
    url = get_url(async_client.functions, f"/functions/schedules/{schedule_id}/input_data")
    httpx_mock.add_response(method="GET", url=url, status_code=200, json={"id": schedule_id, "data": {"value": 2}})
    return httpx_mock


@pytest.fixture
def mock_schedule_no_data_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
    schedule_id = SCHEDULE_WITH_FUNCTION_ID_AND_SESSION["id"]
    url = get_url(async_client.functions, f"/functions/schedules/{schedule_id}/input_data")
    httpx_mock.add_response(method="GET", url=url, status_code=200, json={"id": schedule_id})
    return httpx_mock


class TestFunctionSchedulesAPI:
    def test_retrieve_schedules(
        self, mock_function_schedules_retrieve_response: HTTPXMock, cognite_client: CogniteClient
    ) -> None:
        res = cognite_client.functions.schedules.retrieve(id=cast(int, SCHEDULE_WITH_FUNCTION_EXTERNAL_ID["id"]))
        assert isinstance(res, FunctionSchedule)
        assert SCHEDULE_WITH_FUNCTION_EXTERNAL_ID == res.dump(camel_case=True)

    def test_list_schedules(
        self, mock_filter_function_schedules_response: HTTPXMock, cognite_client: CogniteClient
    ) -> None:
        res = cognite_client.functions.schedules.list()
        assert isinstance(res, FunctionSchedulesList)
        assert [SCHEDULE_WITH_FUNCTION_EXTERNAL_ID] == res.dump(camel_case=True)

    def test_list_schedules_with_limit(
        self, mock_filter_function_schedules_response: HTTPXMock, cognite_client: CogniteClient
    ) -> None:
        res = cognite_client.functions.schedules.list(limit=1)

        assert isinstance(res, FunctionSchedulesList)
        assert len(res) == 1

    def test_list_schedules_with_function_id_and_function_external_id_raises(
        self, cognite_client: CogniteClient
    ) -> None:
        with pytest.raises(ValueError) as excinfo:
            cognite_client.functions.schedules.list(function_id=123, function_external_id="my-func")
        assert (
            "Both 'function_id' and 'function_external_id' were supplied, pass exactly one or neither."
            == excinfo.value.args[0]
        )

    def test_create_schedules_with_function_id_and_function_external_id_raises(
        self, cognite_client: CogniteClient
    ) -> None:
        with pytest.raises(ValueError) as excinfo:
            cognite_client.functions.schedules.create(
                function_id=123,
                function_external_id="my-func",
                name="my-schedule",
                cron_expression="*/5 * * * *",
            )
        assert "Exactly one of function_id and function_external_id must be specified" == excinfo.value.args[0]

    @pytest.mark.usefixtures("mock_function_schedules_response_with_xid")
    def test_create_schedules_with_function_external_id(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient
    ) -> None:
        with patch.object(
            async_client.functions.schedules, "_post", wraps=async_client.functions.schedules._post
        ) as post_mock:
            res = cognite_client.functions.schedules.create(
                name="my-schedule",
                function_external_id="my-func",
                cron_expression="*/5 * * * *",
                description="Hi",
            )

        call_args = post_mock.call_args[0][1]["items"][0]
        assert "functionId" in call_args
        assert "functionExternalId" not in call_args
        assert isinstance(res, FunctionSchedule)

    def test_create_schedules_with_data(
        self, mock_function_schedules_response: HTTPXMock, cognite_client: CogniteClient, monkeypatch: MonkeyPatch
    ) -> None:
        # @patch seems to conflict with httpx_mock, so we use monkeypatch instead:
        async def mock_nonce(*args: Any, **kwargs: Any) -> str:
            return "very noncy"

        monkeypatch.setattr(
            "cognite.client._api.functions.schedules.create_session_and_return_nonce",
            mock_nonce,
        )
        res = cognite_client.functions.schedules.create(
            name="my-schedule",
            function_id=123,
            cron_expression="*/5 * * * *",
            description="Hi",
            data={"value": 2},
        )
        assert isinstance(res, FunctionSchedule)
        assert SCHEDULE_WITH_FUNCTION_EXTERNAL_ID == res.dump(camel_case=True)

    @pytest.mark.usefixtures("disable_gzip")
    @pytest.mark.parametrize(
        "nonce, client_credentials, expected_call_count, expected_client_id, expected_nonce",
        [
            pytest.param(
                "nonce_direct",
                ClientCredentials(
                    client_id="should-not-be-used",
                    client_secret="should-not-be-used",
                ),
                1,
                None,
                "nonce_direct",
                id="First priority nonce",
            ),
            pytest.param(
                None,
                ClientCredentials("should-be-used", "should-be-used"),
                2,
                "should-be-used",
                "credentials_nonce",
                id="Second priority client credentials",
            ),
            pytest.param(None, None, 2, "client_id", "credentials_nonce", id="Third priority THIS client credentials"),
        ],
    )
    def test_create_schedule_nonce_prioritization(
        self,
        nonce: str | None,
        client_credentials: ClientCredentials | None,
        expected_call_count: int,
        expected_client_id: str | None,
        expected_nonce: str,
        httpx_mock: HTTPXMock,
    ) -> None:
        credentials = MagicMock(spec=OAuthClientCredentials)
        credentials.client_id = "client_id"
        credentials.client_secret = "client_secret"
        credentials.authorization_header.return_value = "Bearer token", "42"

        config = ClientConfig(client_name="test_client", project="dummy_project", credentials=credentials)
        cognite_client = CogniteClient(config)
        base_url = f"{config.base_url}/api/v1/projects/{config.project}"
        schedule = FunctionScheduleWrite(
            name="my-schedule",
            cron_expression="*/5 * * * *",
            function_id=123,
            nonce=nonce,
        )
        if expected_call_count > 1:
            httpx_mock.add_response(
                method="POST",
                url=f"{base_url}/sessions",
                status_code=200,
                json={"items": [{"id": 456, "nonce": "credentials_nonce", "status": "READY"}]},
            )
        httpx_mock.add_response(
            method="POST",
            url=f"{base_url}/functions/schedules",
            status_code=200,
            json={
                "items": [
                    {
                        "id": 123,
                        "createdTime": 0,
                        "lastUpdatedTime": 0,
                        "when": "Next Monday",
                        "sessionId": 123,
                        **schedule.dump(),
                    }
                ]
            },
        )
        result = cognite_client.functions.schedules.create(schedule, client_credentials=client_credentials)
        assert len(httpx_mock.get_requests()) == expected_call_count
        if expected_call_count > 1:
            first_body = json.loads(httpx_mock.get_requests()[0].content)
            assert first_body["items"][0]["clientId"] == expected_client_id

        body = json.loads(httpx_mock.get_requests()[-1].content)
        assert body["items"][0]["nonce"] == expected_nonce

        assert isinstance(result, FunctionSchedule)

    def test_delete_schedules(
        self, mock_function_schedules_delete_response: HTTPXMock, cognite_client: CogniteClient
    ) -> None:
        res = cognite_client.functions.schedules.delete(id=8012683333564363)
        assert res is None

    def test_schedule_get_data__function_has_data(
        self, mock_schedule_get_data_response: HTTPXMock, cognite_client: CogniteClient
    ) -> None:
        res = cognite_client.functions.schedules.get_input_data(id=8012683333564363)

        assert isinstance(res, dict)
        assert {"value": 2} == res

    def test_schedule_get_data__function_is_missing_data(
        self, mock_schedule_no_data_response: HTTPXMock, cognite_client: CogniteClient
    ) -> None:
        res = cognite_client.functions.schedules.get_input_data(id=8012683333564363)
        assert res is None


class TestFunctionCallsAPI:
    def test_list_calls_and_filter(
        self,
        mock_function_calls_filter_response: HTTPXMock,
        mock_functions_retrieve_response: HTTPXMock,
        cognite_client: CogniteClient,
    ) -> None:
        fn = get_or_raise(cognite_client.functions.retrieve(id=FUNCTION_ID))
        res = fn.list_calls(
            status="Completed",
            schedule_id=123,
            start_time={"min": 1585925306822, "max": 1585925306823},
            end_time={"min": 1585925310822, "max": 1585925310823},
            limit=-1,
        )

        assert isinstance(res, FunctionCallList)
        assert [CALL_COMPLETED, CALL_SCHEDULED] == res.dump(camel_case=True)

    def test_list_calls_by_function_id(
        self, mock_function_calls_filter_response: HTTPXMock, cognite_client: CogniteClient
    ) -> None:
        res = cognite_client.functions.calls.list(
            function_id=FUNCTION_ID,
            status="Completed",
            schedule_id=123,
            start_time={"min": 1585925306822, "max": 1585925306823},
            end_time={"min": 1585925306822, "max": 1585925306823},
            limit=-1,
        )
        assert isinstance(res, FunctionCallList)
        assert [CALL_COMPLETED, CALL_SCHEDULED] == res.dump(camel_case=True)

    def test_list_calls_by_function_id_with_limits(
        self, mock_function_calls_filter_response_with_limit: HTTPXMock, cognite_client: CogniteClient
    ) -> None:
        res = cognite_client.functions.calls.list(function_id=FUNCTION_ID, limit=2)
        assert isinstance(res, FunctionCallList)
        assert len(res) == 2

    @pytest.mark.usefixtures("mock_functions_retrieve_response")
    def test_list_calls_by_function_external_id(
        self, mock_function_calls_filter_response: HTTPXMock, cognite_client: CogniteClient
    ) -> None:
        res = cognite_client.functions.calls.list(function_external_id=f"func-no-{FUNCTION_ID}")
        assert isinstance(res, FunctionCallList)
        assert [CALL_COMPLETED, CALL_SCHEDULED] == res.dump(camel_case=True)

    def test_list_calls_with_function_id_and_function_external_id_raises(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(ValueError) as excinfo:
            cognite_client.functions.calls.list(function_id=123, function_external_id="my-function")
        assert "Exactly one of function_id and function_external_id must be specified" == excinfo.value.args[0]

    def test_retrieve_call_by_function_id(
        self, mock_function_calls_retrieve_response: HTTPXMock, cognite_client: CogniteClient
    ) -> None:
        res = cognite_client.functions.calls.retrieve(call_id=CALL_ID, function_id=FUNCTION_ID)
        assert isinstance(res, FunctionCall)
        assert CALL_COMPLETED == res.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_functions_retrieve_response")
    def test_retrieve_call_by_function_external_id(
        self, mock_function_calls_retrieve_response: HTTPXMock, cognite_client: CogniteClient
    ) -> None:
        res = cognite_client.functions.calls.retrieve(call_id=CALL_ID, function_external_id=f"func-no-{FUNCTION_ID}")
        assert isinstance(res, FunctionCall)
        assert CALL_COMPLETED == res.dump(camel_case=True)

    def test_retrieve_call_with_function_id_and_function_external_id_raises(
        self, cognite_client: CogniteClient
    ) -> None:
        with pytest.raises(ValueError) as excinfo:
            cognite_client.functions.calls.retrieve(
                call_id=CALL_ID, function_id=FUNCTION_ID, function_external_id=f"func-no-{FUNCTION_ID}"
            )
        assert "Exactly one of function_id and function_external_id must be specified" == excinfo.value.args[0]

    def test_function_call_logs_by_function_id(
        self, mock_function_call_logs_response: dict[str, Any], cognite_client: CogniteClient
    ) -> None:
        res = cognite_client.functions.calls.get_logs(call_id=CALL_ID, function_id=FUNCTION_ID)
        assert isinstance(res, FunctionCallLog)
        assert mock_function_call_logs_response["items"] == res.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_functions_retrieve_response")
    def test_function_call_logs_by_function_external_id(
        self, mock_function_call_logs_response: dict[str, Any], cognite_client: CogniteClient
    ) -> None:
        res = cognite_client.functions.calls.get_logs(call_id=CALL_ID, function_external_id=f"func-no-{FUNCTION_ID}")
        assert isinstance(res, FunctionCallLog)
        assert mock_function_call_logs_response["items"] == res.dump(camel_case=True)

    def test_function_call_logs_by_function_id_and_function_external_id_raises(
        self, mock_function_call_logs_response: dict[str, Any], cognite_client: CogniteClient
    ) -> None:
        with pytest.raises(ValueError) as excinfo:
            cognite_client.functions.calls.get_logs(
                call_id=CALL_ID, function_id=FUNCTION_ID, function_external_id=f"func-no-{FUNCTION_ID}"
            )
        assert "Exactly one of function_id and function_external_id must be specified" == excinfo.value.args[0]

    @pytest.mark.usefixtures("mock_function_calls_retrieve_response")
    def test_get_logs_on_retrieved_call_object(
        self, mock_function_call_logs_response: dict[str, Any], cognite_client: CogniteClient
    ) -> None:
        call = get_or_raise(cognite_client.functions.calls.retrieve(call_id=CALL_ID, function_id=FUNCTION_ID))
        logs = call.get_logs()
        assert isinstance(logs, FunctionCallLog)
        assert mock_function_call_logs_response["items"] == logs.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_function_calls_filter_response")
    def test_get_logs_on_listed_call_object(
        self, mock_function_call_logs_response: dict[str, Any], cognite_client: CogniteClient
    ) -> None:
        calls = cognite_client.functions.calls.list(function_id=FUNCTION_ID)
        logs = calls[0].get_logs()
        assert isinstance(logs, FunctionCallLog)
        assert mock_function_call_logs_response["items"] == logs.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_functions_call_responses")
    def test_get_logs_on_created_call_object(
        self, mock_function_call_logs_response: dict[str, Any], cognite_client: CogniteClient
    ) -> None:
        call = cognite_client.functions.call(id=FUNCTION_ID, nonce="or how to skip 'get nonce' hehe")
        logs = call.get_logs()
        assert isinstance(logs, FunctionCallLog)
        assert mock_function_call_logs_response["items"] == logs.dump(camel_case=True)

    def test_function_call_response_by_function_id(
        self, mock_function_call_response_response: dict[str, Any], cognite_client: CogniteClient
    ) -> None:
        res = cognite_client.functions.calls.get_response(call_id=CALL_ID, function_id=FUNCTION_ID)
        assert isinstance(res, dict)
        assert mock_function_call_response_response["response"] == res

    def test_function_call_response_by_function_external_id(
        self, mock_function_call_response_response: dict[str, Any], cognite_client: CogniteClient
    ) -> None:
        res = cognite_client.functions.calls.get_response(call_id=CALL_ID, function_id=FUNCTION_ID)
        assert isinstance(res, dict)
        assert mock_function_call_response_response["response"] == res

    def test_function_call_response_by_function_id_and_function_external_id_raises(
        self, cognite_client: CogniteClient
    ) -> None:
        with pytest.raises(ValueError) as excinfo:
            cognite_client.functions.calls.get_response(
                call_id=CALL_ID, function_id=FUNCTION_ID, function_external_id=f"func-no-{FUNCTION_ID}"
            )
        assert "Exactly one of function_id and function_external_id must be specified" == excinfo.value.args[0]

    @pytest.mark.usefixtures("mock_function_calls_retrieve_response")
    def test_get_response_on_retrieved_call_object(
        self, mock_function_call_response_response: dict[str, Any], cognite_client: CogniteClient
    ) -> None:
        call = get_or_raise(cognite_client.functions.calls.retrieve(call_id=CALL_ID, function_id=FUNCTION_ID))
        response = call.get_response()
        assert isinstance(response, dict)
        assert mock_function_call_response_response["response"] == response


@pytest.mark.parametrize(
    "xid, overwrite",
    (
        (None, False),
        ("xid", True),
        ("xid", True),
    ),
)
async def test__zip_and_upload_handle__call_signature(
    async_client: AsyncCogniteClient, xid: str | None, overwrite: bool, function_handle: Callable
) -> None:
    mock = MagicMock()
    mock.files.upload_bytes = AsyncMock(return_value=DefaultResourceGenerator.file_metadata(id=123))
    async_client.functions._cognite_client = mock

    file_id = await async_client.functions._zip_and_upload_handle(function_handle, name="name", external_id=xid)
    assert file_id == 123

    mock.files.upload_bytes.assert_called_once()
    call = mock.files.upload_bytes.call_args
    assert len(call.args) == 1 and type(call.args[0]) is bytes
    assert call.kwargs == {"name": "name.zip", "external_id": xid, "overwrite": overwrite, "data_set_id": None}


@pytest.mark.parametrize(
    "xid, overwrite",
    (
        (None, False),
        ("xid", True),
        ("xid", True),
    ),
)
async def test__zip_and_upload_handle__zip_file_content(
    async_client: AsyncCogniteClient, xid: str | None, overwrite: bool, function_handle_with_reqs: Callable
) -> None:
    async def validate_file_upload_call(*args: Any, **kwargs: Any) -> FileMetadata:
        assert len(args) == 1 and type(args[0]) is bytes
        assert kwargs == {"name": "name.zip", "external_id": xid, "overwrite": overwrite, "data_set_id": None}

        with io.BytesIO(args[0]) as wrapped_binary, ZipFile(wrapped_binary, "r") as zip_file:
            assert zip_file.testzip() is None
            assert zip_file.namelist() == ["handler.py", "requirements.txt"]
            with zip_file.open("handler.py", "r") as py_file:
                expected_lines = [
                    "def handle(data: Any, client: Any, secrets: Any) -> None:",
                    '    """',
                    "    [requirements]",
                    "    pandas",
                    "    [/requirements]",
                    '    """',
                ]
                # We use splitlines to ignore line ending differences between OSs:
                assert py_file.read().decode("utf-8").splitlines() == expected_lines
        return DefaultResourceGenerator.file_metadata(id=123)

    mock = MagicMock()
    mock.files.upload_bytes = validate_file_upload_call
    async_client.functions._cognite_client = mock

    file_id = await async_client.functions._zip_and_upload_handle(
        function_handle_with_reqs, name="name", external_id=xid
    )
    assert file_id == 123


@pytest.mark.parametrize(
    "xid, overwrite",
    (
        (None, False),
        ("xid", True),
        ("xid", True),
    ),
)
async def test__zip_and_upload_folder__call_signature(
    async_client: AsyncCogniteClient, xid: str | None, overwrite: bool
) -> None:
    mock = MagicMock()
    mock.files.upload_bytes = AsyncMock(return_value=DefaultResourceGenerator.file_metadata(id=123))
    async_client.functions._cognite_client = mock

    folder = Path(__file__).parent / "function_test_resources" / "good_absolute_import"
    file_id = await async_client.functions._zip_and_upload_folder(folder, name="name", external_id=xid)
    assert file_id == 123

    mock.files.upload_bytes.assert_called_once()
    call = mock.files.upload_bytes.call_args
    assert len(call.args) == 1 and type(call.args[0]) is bytes
    assert call.kwargs == {"name": "name.zip", "external_id": xid, "overwrite": overwrite, "data_set_id": None}


@pytest.mark.parametrize(
    "xid, overwrite",
    (
        (None, False),
        ("xid", True),
        ("xid", True),
    ),
)
async def test__zip_and_upload_folder__zip_file_content(
    async_client: AsyncCogniteClient, xid: str | None, overwrite: bool
) -> None:
    async def validate_file_upload_call(*args: Any, **kwargs: Any) -> FileMetadata:
        assert len(args) == 1 and type(args[0]) is bytes
        assert kwargs == {"name": "name.zip", "external_id": xid, "overwrite": overwrite, "data_set_id": None}

        with io.BytesIO(args[0]) as wrapped_binary, ZipFile(wrapped_binary, "r") as zip_file:
            assert zip_file.testzip() is None
            expected_names = {"./", "shared/", "shared/util.py", "my_functions/", "my_functions/handler.py"}
            assert set(zip_file.namelist()) >= expected_names
            with zip_file.open("my_functions/handler.py", "r") as py_file:
                expected_lines = [
                    "from shared.util import shared_func  # type: ignore[import-not-found]",
                    "",
                    "",
                    "def handle() -> int:",
                    "    return shared_func()",
                ]
                # We use splitlines to ignore line ending differences between OSs:
                assert py_file.read().decode("utf-8").splitlines() == expected_lines
        return DefaultResourceGenerator.file_metadata(id=123, data_set_id=None)

    mock = MagicMock()
    mock.files.upload_bytes = validate_file_upload_call
    async_client.functions._cognite_client = mock

    folder = Path(__file__).parent / "function_test_resources" / "good_absolute_import"
    file_id = await async_client.functions._zip_and_upload_folder(folder, name="name", external_id=xid)
    assert file_id == 123


class TestGetHandleFunctionNode:
    """Test cases for get_handle_function_node function."""

    def test_single_handle_function(self) -> None:
        """Test that a single handle function is returned correctly."""
        file_content = '''
def handle(data, client, secrets):
    """Handle function for processing data."""
    return data
'''
        result = get_handle_function_node(file_content)

        assert result is not None
        assert isinstance(result, ast.FunctionDef)
        assert result.name == "handle"
        assert len(result.args.args) == 3  # data, client, secrets

    def test_multiple_handle_functions_returns_last(self) -> None:
        """Test that when there are multiple handle functions, the last one is returned."""
        file_content = '''
def handle(data, client, secrets):
    """First handle function."""
    return data

def other_function():
    """Some other function."""
    pass

def handle(data, client, secrets):
    """Second handle function - this should be returned."""
    return data * 2
'''
        result = get_handle_function_node(file_content)

        assert result is not None
        assert isinstance(result, ast.FunctionDef)
        assert result.name == "handle"
        # Check that it's the second handle function by looking at the docstring
        docstring = ast.get_docstring(result)
        assert docstring is not None and "Second handle function" in docstring

    def test_no_handle_function_returns_none(self) -> None:
        """Test that None is returned when no handle function exists."""
        file_content = '''
def other_function():
    """Some other function."""
    pass

def another_function():
    """Another function."""
    pass
'''
        result = get_handle_function_node(file_content)
        assert result is None

    def test_empty_file_returns_none(self) -> None:
        """Test that None is returned for an empty file."""
        file_content = ""
        result = get_handle_function_node(file_content)
        assert result is None

    def test_handle_function_with_imports_and_comments(self) -> None:
        """Test that handle function is found even with imports and comments."""
        file_content = '''
import os
from pathlib import Path

# This is a comment
def some_other_function():
    pass

def handle(data, client, secrets):
    """Handle function with proper signature."""
    return {"result": data}

# Another comment
'''
        result = get_handle_function_node(file_content)

        assert result is not None
        assert isinstance(result, ast.FunctionDef)
        assert result.name == "handle"
        docstring = ast.get_docstring(result)
        assert docstring is not None and "Handle function with proper signature" in docstring

    def test_handle_function_inside_class_ignored(self) -> None:
        """Test that handle functions inside classes are ignored (only top-level functions)."""
        file_content = '''
def handle(data, client, secrets):
    """Top-level handle function - should be returned."""
    return data

class MyClass:
    def handle(self, data, client, secrets):
        """Handle function inside class - should be ignored."""
        return data
'''
        result = get_handle_function_node(file_content)

        assert result is not None
        assert isinstance(result, ast.FunctionDef)
        assert result.name == "handle"
        docstring = ast.get_docstring(result)
        assert docstring is not None and "Top-level handle function" in docstring

    def test_handle_function_inside_other_function_ignored(self) -> None:
        """Test that handle functions inside other functions are ignored."""
        file_content = '''
def outer_function():
    def handle(data, client, secrets):
        """Handle function inside another function - should be ignored."""
        return data

def handle(data, client, secrets):
    """Top-level handle function - should be returned."""
    return data
'''
        result = get_handle_function_node(file_content)

        assert result is not None
        assert isinstance(result, ast.FunctionDef)
        assert result.name == "handle"
        docstring = ast.get_docstring(result)
        assert docstring is not None and "Top-level handle function" in docstring
