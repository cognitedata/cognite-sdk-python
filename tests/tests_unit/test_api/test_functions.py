import io
import operator as op
import os
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch
from zipfile import ZipFile

import pytest
from requests import PreparedRequest

from cognite.client import ClientConfig, CogniteClient
from cognite.client._api.functions import (
    _extract_requirements_from_doc_string,
    _extract_requirements_from_file,
    _get_fn_docstring_requirements,
    _validate_and_parse_requirements,
    validate_function_folder,
)
from cognite.client.credentials import OAuthClientCredentials, Token
from cognite.client.data_classes import (
    FileMetadata,
    Function,
    FunctionCall,
    FunctionCallList,
    FunctionCallLog,
    FunctionList,
    FunctionSchedule,
    FunctionSchedulesList,
    FunctionsLimits,
)
from cognite.client.data_classes.functions import FunctionsStatus
from cognite.client.exceptions import CogniteAPIError
from tests.utils import jsgz_load


def full_url(client, resource_path, api="functions"):
    # getattr does not support nested dots, i.e. `iam.sessions`:
    return op.attrgetter(api)(client)._get_base_url_with_base_path() + resource_path


def post_body_matcher(params):
    """Used for verifying post-bodies to mocked endpoints. See the `match`-argument in `rsps.add()`"""

    def match(request_body):
        if request_body is None:
            return params is None, None
        else:
            if isinstance(request_body, PreparedRequest):
                decompressed_body = jsgz_load(request_body.body)
            else:
                decompressed_body = jsgz_load(request_body)
            sorted_params = sorted(params.items())
            sorted_body = sorted(decompressed_body.items())

            res = sorted_params == sorted_body
            return res, None

    return match


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
    "runtime": "py38",
    "runtimeVersion": "Python 3.8.13",
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
def mock_sessions_with_client_credentials(rsps, cognite_client_with_client_credentials_flow):
    url = full_url(cognite_client_with_client_credentials_flow, "/sessions", api="iam.sessions")

    creds = cognite_client_with_client_credentials_flow.config.credentials
    assert isinstance(creds, OAuthClientCredentials)

    rsps.add(
        rsps.POST,
        url=url,
        status=200,
        json={"items": [{"nonce": "aabbccdd", "id": 123, "status": "mocky"}]},
        match=[post_body_matcher({"items": [{"clientId": creds.client_id, "clientSecret": creds.client_secret}]})],
    )

    return rsps


@pytest.fixture
def mock_sessions_with_token_exchange(rsps, cognite_client):
    url = full_url(cognite_client, "/sessions")

    rsps.add(
        rsps.POST,
        url=url,
        status=200,
        json={"items": [{"nonce": "aabbccdd", "id": 123, "status": "mocky"}]},
        match=[post_body_matcher({"items": [{"tokenExchange": True}]})],
    )

    return rsps


@pytest.fixture
def mock_functions_filter_response(rsps, cognite_client):
    response_body = {"items": [EXAMPLE_FUNCTION]}

    url = full_url(cognite_client, "/functions/list")
    rsps.add(rsps.POST, url, status=200, json=response_body)

    yield rsps


@pytest.fixture
def mock_functions_retrieve_response(rsps, cognite_client):
    response_body = {"items": [EXAMPLE_FUNCTION]}

    url = full_url(cognite_client, "/functions/byids")
    rsps.add(rsps.POST, url, status=200, json=response_body)

    yield rsps


@pytest.fixture
def mock_functions_create_response(rsps, cognite_client):
    files_response_body = {
        "name": "myfunction",
        "id": FUNCTION_ID,
        "uploaded": True,
        "createdTime": 1585662507939,
        "lastUpdatedTime": 1585662507939,
        "uploadUrl": "https://upload.here",
    }

    rsps.assert_all_requests_are_fired = False

    files_url = full_url(cognite_client, "/files", api="files")
    files_byids_url = full_url(cognite_client, "/files/byids", api="files")

    rsps.add(rsps.POST, files_url, status=201, json=files_response_body)
    rsps.add(rsps.PUT, "https://upload.here", status=201)
    rsps.add(rsps.POST, files_byids_url, status=201, json={"items": [files_response_body]})
    functions_url = full_url(cognite_client, "/functions")
    rsps.add(rsps.POST, functions_url, status=201, json={"items": [EXAMPLE_FUNCTION]})

    yield rsps


@pytest.fixture
def mock_file_not_uploaded(rsps, cognite_client):
    files_response_body = {
        "name": "myfunction",
        "id": FUNCTION_ID,
        "uploaded": False,
        "createdTime": 1585662507939,
        "lastUpdatedTime": 1585662507939,
        "uploadUrl": "https://upload.here",
    }

    files_byids_url = full_url(cognite_client, "/files/byids", api="files")

    rsps.add(rsps.POST, files_byids_url, status=201, json={"items": [files_response_body]})
    yield rsps


@pytest.fixture
def mock_functions_delete_response(rsps, cognite_client):
    url = full_url(cognite_client, "/functions/delete")
    rsps.add(rsps.POST, url, status=200, json={})

    yield rsps


@pytest.fixture
def mock_functions_call_responses(rsps, cognite_client):
    url = full_url(cognite_client, f"/functions/{FUNCTION_ID}/call")
    rsps.add(rsps.POST, url, status=201, json=CALL_RUNNING)

    url = full_url(cognite_client, f"/functions/{FUNCTION_ID}/calls/byids")
    rsps.add(rsps.POST, url, status=200, json={"items": [CALL_COMPLETED]})

    yield rsps


@pytest.fixture
def mock_sessions_bad_request_response(rsps, cognite_client):
    url = full_url(cognite_client, "/sessions")

    rsps.add(rsps.POST, url, status=403)

    yield rsps


@pytest.fixture
def mock_functions_call_by_external_id_responses(mock_functions_retrieve_response, cognite_client):
    rsps = mock_functions_retrieve_response

    url = full_url(cognite_client, f"/functions/{FUNCTION_ID}/call")
    rsps.add(rsps.POST, url, status=201, json=CALL_RUNNING)

    url = full_url(cognite_client, f"/functions/{FUNCTION_ID}/calls/byids")
    rsps.add(rsps.POST, url, status=200, json={"items": [CALL_COMPLETED]})

    yield rsps


@pytest.fixture
def mock_functions_call_failed_response(rsps, cognite_client):
    url = full_url(cognite_client, f"/functions/{FUNCTION_ID}/call")
    rsps.add(rsps.POST, url, status=201, json=CALL_FAILED)

    yield rsps


@pytest.fixture
def mock_functions_call_timeout_response(rsps, cognite_client):
    url = full_url(cognite_client, f"/functions/{FUNCTION_ID}/call")
    rsps.add(rsps.POST, url, status=201, json=CALL_TIMEOUT)

    yield rsps


@pytest.fixture
def function_handle():
    def handle(data, client, secrets):
        pass

    return handle


@pytest.fixture
def function_handle_with_reqs():
    def handle(data, client, secrets):
        """
        [requirements]
        pandas
        [/requirements]
        """

    return handle


@pytest.fixture
def function_handle_illegal_name():
    def func(data, client, secrets):
        pass

    return func


@pytest.fixture
def function_handle_illegal_argument():
    def handle(client, input):
        pass

    return handle


@pytest.fixture
def mock_function_calls_filter_response(rsps, cognite_client):
    response_body = {"items": [CALL_COMPLETED, CALL_SCHEDULED]}
    url = full_url(cognite_client, f"/functions/{FUNCTION_ID}/calls/list")
    rsps.add(rsps.POST, url, status=200, json=response_body)

    yield rsps


@pytest.fixture
def cognite_client_with_client_credentials_flow(rsps):
    rsps.add(
        rsps.POST,
        "https://bla",
        status=200,
        json={"access_token": "abc", "expires_at": datetime.now().timestamp() + 1000},
    )
    return CogniteClient(
        ClientConfig(
            client_name="any",
            project="dummy",
            credentials=OAuthClientCredentials(
                token_url="https://bla", client_id="bla", client_secret="bla", scopes=["bla"]
            ),
        )
    )


@pytest.fixture
def cognite_client_with_token():
    return CogniteClient(ClientConfig(client_name="any", project="dummy", credentials=Token("aabbccddeeffgg")))


@pytest.fixture
def mock_function_calls_filter_response_with_limit(rsps, cognite_client):
    response_body = {"items": [CALL_COMPLETED, CALL_SCHEDULED]}
    url = full_url(cognite_client, f"/functions/{FUNCTION_ID}/calls/list")
    rsps.add(rsps.POST, url, status=200, json=response_body)

    yield rsps


@pytest.fixture
def mock_functions_limit_response(rsps, cognite_client):
    response_body = {
        "timeoutMinutes": 10,
        "cpuCores": {"min": 0.1, "max": 0.6, "default": 0.25},
        "memoryGb": {"min": 0.1, "max": 2.5, "default": 1.0},
        "responseSizeMb": 1,
        "runtimes": ["py38", "py39", "py310"],
    }
    url = full_url(cognite_client, "/functions/limits")
    rsps.add(rsps.GET, url, status=200, json=response_body)

    yield rsps


@pytest.fixture
def mock_functions_status_response(rsps, cognite_client):
    response_body = {"status": "IN PROGRESS"}
    url = full_url(cognite_client, "/functions/status")
    rsps.add(rsps.POST, url, status=200, json=response_body)
    rsps.add(rsps.GET, url, status=200, json=response_body)

    yield rsps


@pytest.fixture
def mock_file_create_response(rsps, cognite_client):
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
    rsps.add(rsps.POST, full_url(cognite_client, "/files", api="files"), status=200, json=response_body)
    rsps.add(rsps.PUT, "https://upload.here", status=200, json=response_body)
    yield rsps


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
    def test_validate_folder(self, function_folder, function_path, exception):
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
    def test_imports_in_validate_folder(self, function_folder, function_path, exception):
        folder = os.path.join(os.path.dirname(__file__), "function_test_resources", function_folder)
        if exception is None:
            validate_function_folder(folder, function_path, skip_folder_validation=False)
        else:
            with pytest.raises(exception):
                validate_function_folder(folder, function_path, skip_folder_validation=False)

    @pytest.mark.parametrize(
        "function_folder, function_name, exception",
        [
            ("function_code", "function_w_no_requirements", None),
            ("function_code_with_requirements", "function_w_requirements", None),
        ],
    )
    @pytest.mark.usefixtures("mock_file_create_response")
    def test_zip_and_upload_folder(self, function_folder, function_name, exception, cognite_client):
        folder = os.path.join(os.path.dirname(__file__), "function_test_resources", function_folder)
        cognite_client.functions._zip_and_upload_folder(folder, function_name)

    @patch("cognite.client._api.functions.MAX_RETRIES", 1)
    def test_create_function_with_file_not_uploaded(self, mock_file_not_uploaded, cognite_client):
        with pytest.raises(IOError):
            cognite_client.functions.create(name="myfunction", file_id=123)

    def test_create_with_path(self, mock_functions_create_response, cognite_client):
        folder = os.path.join(os.path.dirname(__file__), "function_test_resources", "function_code")
        res = cognite_client.functions.create(name="myfunction", folder=folder, function_path="handler.py")

        assert isinstance(res, Function)
        assert mock_functions_create_response.calls[3].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_with_file_id(self, mock_functions_create_response, cognite_client):
        res = cognite_client.functions.create(name="myfunction", file_id=1234)

        assert isinstance(res, Function)
        assert mock_functions_create_response.calls[1].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_with_function_handle(self, mock_functions_create_response, function_handle, cognite_client):
        res = cognite_client.functions.create(name="myfunction", function_handle=function_handle)

        assert isinstance(res, Function)
        assert mock_functions_create_response.calls[3].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_with_function_handle_with_illegal_name_raises(self, function_handle_illegal_name, cognite_client):
        with pytest.raises(TypeError):
            cognite_client.functions.create(name="myfunction", function_handle=function_handle_illegal_name)

    def test_create_with_function_handle_with_illegal_argument_raises(
        self, function_handle_illegal_argument, cognite_client
    ):
        with pytest.raises(TypeError):
            cognite_client.functions.create(name="myfunction", function_handle=function_handle_illegal_argument)

    def test_create_with_handle_function_and_file_id_raises(
        self, mock_functions_create_response, function_handle, cognite_client
    ):
        with pytest.raises(TypeError):
            cognite_client.functions.create(name="myfunction", function_handle=function_handle, file_id=1234)

    def test_create_with_path_and_file_id_raises(self, mock_functions_create_response, cognite_client):
        with pytest.raises(TypeError):
            cognite_client.functions.create(
                name="myfunction", folder="some/folder", file_id=1234, function_path="handler.py"
            )

    def test_create_with_cpu_and_memory(self, mock_functions_create_response, cognite_client):
        res = cognite_client.functions.create(name="myfunction", file_id=1234, cpu=0.2, memory=1)

        assert isinstance(res, Function)
        assert mock_functions_create_response.calls[1].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_with_cpu_not_float_raises(self, mock_functions_create_response, cognite_client):
        with pytest.raises(TypeError):
            cognite_client.functions.create(name="myfunction", file_id=1234, cpu="0.2")

    def test_create_with_memory_not_float_raises(self, mock_functions_create_response, cognite_client):
        with pytest.raises(TypeError):
            cognite_client.functions.create(name="myfunction", file_id=1234, memory="0.5")

    def test_create_upload_with_data_set_id(self, mock_functions_create_response, cognite_client, function_handle):
        cognite_client.files = MagicMock(spec=cognite_client.files)

        def mock_upload_bytes(*args, **kwargs):
            assert kwargs.get("data_set_id") == 999
            return FileMetadata(id=FUNCTION_ID, data_set_id=kwargs.get("data_set_id"))

        cognite_client.files.upload_bytes.side_effect = mock_upload_bytes

        cognite_client.functions.create(name="myfunction", function_handle=function_handle, data_set_id=999)

    def test_delete_single_id(self, mock_functions_delete_response, cognite_client):
        _ = cognite_client.functions.delete(id=1)
        assert {"items": [{"id": 1}]} == jsgz_load(mock_functions_delete_response.calls[0].request.body)

    def test_delete_single_external_id(self, mock_functions_delete_response, cognite_client):
        _ = cognite_client.functions.delete(external_id="func1")
        assert {"items": [{"externalId": "func1"}]} == jsgz_load(mock_functions_delete_response.calls[0].request.body)

    def test_delete_multiple_id_and_multiple_external_id(self, mock_functions_delete_response, cognite_client):
        _ = cognite_client.functions.delete(id=[1, 2, 3], external_id=["func1", "func2"])
        assert {
            "items": [{"id": 1}, {"id": 2}, {"id": 3}, {"externalId": "func1"}, {"externalId": "func2"}]
        } == jsgz_load(mock_functions_delete_response.calls[0].request.body)

    def test_list(self, mock_functions_filter_response, cognite_client):
        res = cognite_client.functions.list()

        assert isinstance(res, FunctionList)
        assert mock_functions_filter_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_list_with_limits(self, mock_functions_filter_response, cognite_client):
        res = cognite_client.functions.list(limit=1)
        assert isinstance(res, FunctionList)
        assert len(res) == 1

    def test_retrieve_by_id(self, mock_functions_retrieve_response, cognite_client):
        res = cognite_client.functions.retrieve(id=1)
        assert isinstance(res, Function)
        assert mock_functions_retrieve_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_retrieve_by_external_id(self, mock_functions_retrieve_response, cognite_client):
        res = cognite_client.functions.retrieve(external_id="func1")
        assert isinstance(res, Function)
        assert mock_functions_retrieve_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_retrieve_by_id_and_external_id_raises(self, cognite_client):
        with pytest.raises(ValueError):
            cognite_client.functions.retrieve(id=1, external_id="func1")

    def test_retrieve_multiple_by_ids(self, mock_functions_retrieve_response, cognite_client):
        res = cognite_client.functions.retrieve_multiple(ids=[1])
        assert isinstance(res, FunctionList)
        assert mock_functions_retrieve_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_retrieve_multiple_by_external_ids(self, mock_functions_retrieve_response, cognite_client):
        res = cognite_client.functions.retrieve_multiple(external_ids=["func1"])
        assert isinstance(res, FunctionList)
        assert mock_functions_retrieve_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_retrieve_multiple_by_ids_and_external_ids(self, mock_functions_retrieve_response, cognite_client):
        res = cognite_client.functions.retrieve_multiple(ids=[1], external_ids=["func1"])
        assert isinstance(res, FunctionList)
        assert mock_functions_retrieve_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_sessions_with_client_credentials")
    def test_function_call_from_oidc_client_credentials_flow(
        self, mock_functions_call_responses, cognite_client_with_client_credentials_flow
    ):
        res = cognite_client_with_client_credentials_flow.functions.call(id=FUNCTION_ID)

        assert isinstance(res, FunctionCall)
        assert mock_functions_call_responses.calls[-1].response.json()["items"][0] == res.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_sessions_with_client_credentials")
    def test_function_call_by_external_id_from_oidc_client_credentials_flow(
        self, mock_functions_call_by_external_id_responses, cognite_client_with_client_credentials_flow
    ):
        res = cognite_client_with_client_credentials_flow.functions.call(external_id=f"func-no-{FUNCTION_ID}")

        assert isinstance(res, FunctionCall)
        expected = mock_functions_call_by_external_id_responses.calls[-1].response.json()["items"][0]
        assert expected == res.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_sessions_bad_request_response")
    def test_function_call_with_failing_client_credentials_flow(self, cognite_client_with_client_credentials_flow):
        with pytest.raises(CogniteAPIError) as excinfo:
            cognite_client_with_client_credentials_flow.functions.call(id=FUNCTION_ID)
        assert excinfo.value.code == 403

    @pytest.mark.usefixtures("mock_sessions_with_client_credentials")
    def test_function_call_timeout_from_oidc_client_credentials_flow(
        self, mock_functions_call_timeout_response, cognite_client_with_client_credentials_flow
    ):
        res = cognite_client_with_client_credentials_flow.functions.call(id=FUNCTION_ID)
        assert isinstance(res, FunctionCall)
        assert mock_functions_call_timeout_response.calls[-1].response.json() == res.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_sessions_with_token_exchange")
    def test_function_call_from_oidc_token_exchange_flow(
        self, mock_functions_call_responses, cognite_client_with_token
    ):
        res = cognite_client_with_token.functions.call(id=FUNCTION_ID)

        assert isinstance(res, FunctionCall)
        assert mock_functions_call_responses.calls[2].response.json()["items"][0] == res.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_sessions_with_token_exchange")
    def test_function_call_by_external_id_from_oidc_token_exchange_flow(
        self, mock_functions_call_by_external_id_responses, cognite_client_with_token
    ):
        res = cognite_client_with_token.functions.call(external_id=f"func-no-{FUNCTION_ID}")

        assert isinstance(res, FunctionCall)
        assert mock_functions_call_by_external_id_responses.calls[3].response.json()["items"][0] == res.dump(
            camel_case=True
        )

    @pytest.mark.usefixtures("mock_sessions_bad_request_response")
    def test_function_call_with_failing_token_exchange_flow(self, cognite_client_with_token):
        with pytest.raises(CogniteAPIError) as excinfo:
            cognite_client_with_token.functions.call(id=FUNCTION_ID)
        assert excinfo.value.code == 403

    @pytest.mark.usefixtures("mock_sessions_with_token_exchange")
    def test_function_call_timeout_from_from_oidc_token_exchange_flow(
        self, mock_functions_call_timeout_response, cognite_client_with_token
    ):
        res = cognite_client_with_token.functions.call(id=FUNCTION_ID)
        assert isinstance(res, FunctionCall)
        assert mock_functions_call_timeout_response.calls[1].response.json() == res.dump(camel_case=True)

    def test_functions_limits_endpoint(self, mock_functions_limit_response, cognite_client):
        res = cognite_client.functions.limits()
        assert isinstance(res, FunctionsLimits)
        assert mock_functions_limit_response.calls[0].response.json() == res.dump(camel_case=True)

    def test_functions_status_endpoint(self, mock_functions_status_response, cognite_client):
        res = cognite_client.functions.activate()
        assert isinstance(res, FunctionsStatus)
        assert mock_functions_status_response.calls[0].response.json() == res.dump(camel_case=True)

        res = cognite_client.functions.status()
        assert isinstance(res, FunctionsStatus)
        assert mock_functions_status_response.calls[1].response.json() == res.dump(camel_case=True)


class TestRequirementsParser:
    """Test extraction of requirements.txt from docstring in handle-function"""

    def test_validate_requirements(self):
        parsed = _validate_and_parse_requirements(["asyncio==3.4.3", "numpy==1.23.0", "pandas==1.4.3"])
        assert parsed == ["asyncio==3.4.3", "numpy==1.23.0", "pandas==1.4.3"]

    def test_validate_requirements_error(self):
        reqs = [["asyncio=3.4.3"], ["num py==1.23.0"], ["pandas==1.4.3 python_version=='3.8'"]]
        for req in reqs:
            with pytest.raises(Exception):
                _validate_and_parse_requirements(req)

    def test_get_requirements_handle(self):
        def fn():
            """
            [requirements]
            asyncio
            [/requirements]
            """
            return None

        assert _get_fn_docstring_requirements(fn) == ["asyncio"]

    def test_get_requirements_handle_error(self):
        def fn():
            return None

        assert _get_fn_docstring_requirements(fn) == []

    def test_get_requirements_handle_no_docstr(self):
        def fn():
            """
            [requirements]
            asyncio=3.4.3
            [/requirements]
            """
            return None

        with pytest.raises(Exception):
            _get_fn_docstring_requirements(fn)

    def test_get_requirements_handle_no_reqs(self):
        def fn():
            """
            [requirements]
            [/requirements]
            """
            return None

        assert _get_fn_docstring_requirements(fn) == []

    def test_extract_requirements_from_file(self, tmpdir):
        req = "somepackage == 3.8.1"
        file = os.path.join(tmpdir, "requirements.txt")
        with open(file, "w+") as f:
            f.writelines("\n".join(["# this should not be included", "     " + req]))
        reqs = _extract_requirements_from_file(file_name=file)
        assert type(reqs) is list  # noqa E721
        assert len(reqs) == 1
        assert req in reqs

    def test_extract_requirements_from_doc_string(self):
        req_mock = '[requirements]\nSomePackage==3.4.3, >3.4.1; python_version=="3.7"\nSomePackage==21.4.0; python_version=="3.7" and python_full_version<"3.0.0" or python_full_version>="3.5.0" and python_version>="3.7"\nSomePackage==2022.6.15; python_version>="3.8" and python_version<"4"\ncSomePackage==1.15.0; python_version>="3.6"\n[/requirements]\n'
        doc_string_mock = "this should not be included\n" + req_mock + "neither should this\n"
        expected = req_mock.splitlines()[1:-1]
        assert _extract_requirements_from_doc_string(doc_string_mock) == expected

    def test_extract_requirements_from_doc_string_empty(self):
        doc_string = "[requirements]\n[/requirements]\n"
        assert _extract_requirements_from_doc_string(doc_string) == []

    def test_extract_requirements_from_doc_string_no_defined(self):
        doc_string = "no requirements here"
        assert _extract_requirements_from_doc_string(doc_string) is None


@pytest.fixture
def mock_function_calls_retrieve_response(rsps, cognite_client):
    response_body = CALL_COMPLETED
    url = full_url(cognite_client, f"/functions/{FUNCTION_ID}/calls/byids")
    rsps.add(rsps.POST, url, status=200, json={"items": [response_body]})

    yield rsps


@pytest.fixture
def mock_function_call_response_response(rsps, cognite_client):
    response_body = {"callId": CALL_ID, "functionId": 1234, "response": {"key": "value"}}
    url = full_url(cognite_client, f"/functions/{FUNCTION_ID}/calls/{CALL_ID}/response")
    rsps.assert_all_requests_are_fired = False
    rsps.add(rsps.GET, url, status=200, json=response_body)

    yield rsps


@pytest.fixture
def mock_function_call_logs_response(rsps, cognite_client):
    response_body = {
        "items": [
            {"timestamp": 1585925306822, "message": "message 1"},
            {"timestamp": 1585925310822, "message": "message 2"},
        ]
    }
    url = full_url(cognite_client, f"/functions/{FUNCTION_ID}/calls/{CALL_ID}/logs")
    rsps.assert_all_requests_are_fired = False
    rsps.add(rsps.GET, url, status=200, json=response_body)

    yield rsps


SCHEDULE_WITH_FUNCTION_EXTERNAL_ID = {
    "createdTime": 123,
    "cronExpression": "*/5 * * * *",
    "description": "Hi",
    "functionExternalId": "my-func",
    "id": 8012683333564363,
    "name": "my-schedule",
    "when": "Every 5 minutes",
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
def mock_filter_function_schedules_response(rsps, cognite_client):
    url = full_url(cognite_client, "/functions/schedules/list")
    rsps.add(rsps.POST, url, status=200, json={"items": [SCHEDULE_WITH_FUNCTION_EXTERNAL_ID]})

    yield rsps


@pytest.fixture
def mock_function_schedules_response(rsps, cognite_client):
    # Creating a new schedule first needs a session (to pass the nonce):
    rsps.add(
        rsps.POST,
        full_url(cognite_client, "/sessions"),
        status=200,
        json={"items": [{"nonce": "very noncy", "id": 123, "status": "mocky"}]},
    )

    url = full_url(cognite_client, "/functions/schedules")
    rsps.assert_all_requests_are_fired = False
    rsps.add(rsps.GET, url, status=200, json={"items": [SCHEDULE_WITH_FUNCTION_EXTERNAL_ID]})
    rsps.add(rsps.POST, url, status=200, json={"items": [SCHEDULE_WITH_FUNCTION_EXTERNAL_ID]})

    yield rsps


@pytest.fixture
def mock_function_schedules_response_xid_not_valid_with_oidc(rsps, cognite_client):
    # Creating a new schedule first needs a session (to pass the nonce):
    rsps.add(
        rsps.POST,
        full_url(cognite_client, "/sessions"),
        status=200,
        json={"items": [{"nonce": "very noncy", "id": 123, "status": "mocky"}]},
    )
    rsps.add(
        rsps.POST,
        full_url(cognite_client, "/functions/schedules"),
        status=400,
        json={
            "error": {
                "message": "When creating a schedule with OIDC-tokens, you must use 'function_id' and not 'function_external_id'",
                "code": 400,
            }
        },
    )
    yield rsps


@pytest.fixture
def mock_function_schedules_response_oidc_client_credentials(rsps, cognite_client):
    session_url = full_url(cognite_client, "/sessions")

    rsps.add(
        rsps.POST,
        session_url,
        status=200,
        json={"items": [{"nonce": "aaabbb", "id": 123, "status": "mocky"}]},
        match=[post_body_matcher({"items": [{"clientId": "aabbccdd", "clientSecret": "xxyyzz"}]})],
    )

    url = full_url(cognite_client, "/functions/schedules")
    rsps.add(rsps.POST, url, status=200, json={"items": [SCHEDULE_WITH_FUNCTION_ID_AND_SESSION]})
    yield rsps


@pytest.fixture
def mock_function_schedules_retrieve_response(rsps, cognite_client):
    url = full_url(cognite_client, "/functions/schedules/byids")
    rsps.add(rsps.POST, url, status=200, json={"items": [SCHEDULE_WITH_FUNCTION_EXTERNAL_ID]})

    yield rsps


@pytest.fixture
def mock_function_schedules_delete_response(rsps, cognite_client):
    url = full_url(cognite_client, "/functions/schedules/delete")
    rsps.add(rsps.POST, url, status=200, json={})

    yield rsps


@pytest.fixture
def mock_schedule_get_data_response(rsps, cognite_client):
    schedule_id = SCHEDULE_WITH_FUNCTION_ID_AND_SESSION["id"]
    url = full_url(cognite_client, f"/functions/schedules/{schedule_id}/input_data")
    rsps.add(rsps.GET, url, status=200, json={"id": schedule_id, "data": {"value": 2}})

    yield rsps


@pytest.fixture
def mock_schedule_no_data_response(rsps, cognite_client):
    schedule_id = SCHEDULE_WITH_FUNCTION_ID_AND_SESSION["id"]
    url = full_url(cognite_client, f"/functions/schedules/{schedule_id}/input_data")
    rsps.add(rsps.GET, url, status=200, json={"id": schedule_id})

    yield rsps


class TestFunctionSchedulesAPI:
    def test_retrieve_schedules(self, mock_function_schedules_retrieve_response, cognite_client):
        res = cognite_client.functions.schedules.retrieve(id=SCHEDULE_WITH_FUNCTION_EXTERNAL_ID["id"])
        assert isinstance(res, FunctionSchedule)
        expected = mock_function_schedules_retrieve_response.calls[0].response.json()["items"][0]
        assert expected == res.dump(camel_case=True)

    def test_list_schedules(self, mock_filter_function_schedules_response, cognite_client):
        res = cognite_client.functions.schedules.list()
        assert isinstance(res, FunctionSchedulesList)
        expected = mock_filter_function_schedules_response.calls[0].response.json()["items"]
        assert expected == res.dump(camel_case=True)

    def test_list_schedules_with_limit(self, mock_filter_function_schedules_response, cognite_client):
        res = cognite_client.functions.schedules.list(limit=1)

        assert isinstance(res, FunctionSchedulesList)
        assert len(res) == 1

    def test_list_schedules_with_function_id_and_function_external_id_raises(self, cognite_client):
        with pytest.raises(ValueError) as excinfo:
            cognite_client.functions.schedules.list(function_id=123, function_external_id="my-func")
        assert (
            "Both 'function_id' and 'function_external_id' were supplied, pass exactly one or neither."
            == excinfo.value.args[0]
        )

    def test_create_schedules_with_function_id_and_client_credentials(
        self, mock_function_schedules_response_oidc_client_credentials, cognite_client
    ):
        res = cognite_client.functions.schedules.create(
            name="my-schedule",
            function_id=123,
            cron_expression="*/5 * * * *",
            description="Hi",
            client_credentials={"client_id": "aabbccdd", "client_secret": "xxyyzz"},
        )

        assert isinstance(res, FunctionSchedule)
        expected = mock_function_schedules_response_oidc_client_credentials.calls[1].response.json()["items"][0]
        assert expected == res.dump(camel_case=True)

    def test_create_schedules_with_data(self, mock_function_schedules_response, cognite_client):
        res = cognite_client.functions.schedules.create(
            name="my-schedule",
            function_id=123,
            cron_expression="*/5 * * * *",
            description="Hi",
            data={"value": 2},
        )
        assert isinstance(res, FunctionSchedule)
        expected = mock_function_schedules_response.calls[1].response.json()["items"][0]
        assert expected == res.dump(camel_case=True)

    def test_delete_schedules(self, mock_function_schedules_delete_response, cognite_client):
        res = cognite_client.functions.schedules.delete(id=8012683333564363)
        assert res is None

    def test_schedule_get_data__function_has_data(self, mock_schedule_get_data_response, cognite_client):
        res = cognite_client.functions.schedules.get_input_data(id=8012683333564363)

        assert isinstance(res, dict)
        expected = mock_schedule_get_data_response.calls[0].response.json()["data"]
        assert res == expected

    def test_schedule_get_data__function_is_missing_data(self, mock_schedule_no_data_response, cognite_client):
        res = cognite_client.functions.schedules.get_input_data(id=8012683333564363)
        assert res is None


class TestFunctionCallsAPI:
    def test_list_calls_and_filter(
        self, mock_function_calls_filter_response, mock_functions_retrieve_response, cognite_client
    ):
        filter_kwargs = {
            "status": "Completed",
            "schedule_id": 123,
            "start_time": {"min": 1585925306822, "max": 1585925306823},
            "end_time": {"min": 1585925310822, "max": 1585925310823},
        }
        res = cognite_client.functions.retrieve(id=FUNCTION_ID).list_calls(**filter_kwargs, limit=-1)

        assert isinstance(res, FunctionCallList)
        assert mock_function_calls_filter_response.calls[1].response.json()["items"] == res.dump(camel_case=True)

    def test_list_calls_by_function_id(self, mock_function_calls_filter_response, cognite_client):
        filter_kwargs = {
            "status": "Completed",
            "schedule_id": 123,
            "start_time": {"min": 1585925306822, "max": 1585925306823},
            "end_time": {"min": 1585925310822, "max": 1585925310823},
        }
        res = cognite_client.functions.calls.list(function_id=FUNCTION_ID, **filter_kwargs, limit=-1)
        assert isinstance(res, FunctionCallList)
        assert mock_function_calls_filter_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_list_calls_by_function_id_with_limits(
        self, mock_function_calls_filter_response_with_limit, cognite_client
    ):
        res = cognite_client.functions.calls.list(function_id=FUNCTION_ID, limit=2)
        assert isinstance(res, FunctionCallList)
        assert len(res) == 2

    @pytest.mark.usefixtures("mock_functions_retrieve_response")
    def test_list_calls_by_function_external_id(self, mock_function_calls_filter_response, cognite_client):
        res = cognite_client.functions.calls.list(function_external_id=f"func-no-{FUNCTION_ID}")
        assert isinstance(res, FunctionCallList)
        assert mock_function_calls_filter_response.calls[1].response.json()["items"] == res.dump(camel_case=True)

    def test_list_calls_with_function_id_and_function_external_id_raises(self, cognite_client):
        with pytest.raises(ValueError) as excinfo:
            cognite_client.functions.calls.list(function_id=123, function_external_id="my-function")
        assert "Exactly one of function_id and function_external_id must be specified" == excinfo.value.args[0]

    def test_retrieve_call_by_function_id(self, mock_function_calls_retrieve_response, cognite_client):
        res = cognite_client.functions.calls.retrieve(call_id=CALL_ID, function_id=FUNCTION_ID)
        assert isinstance(res, FunctionCall)
        assert mock_function_calls_retrieve_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_functions_retrieve_response")
    def test_retrieve_call_by_function_external_id(self, mock_function_calls_retrieve_response, cognite_client):
        res = cognite_client.functions.calls.retrieve(call_id=CALL_ID, function_external_id=f"func-no-{FUNCTION_ID}")
        assert isinstance(res, FunctionCall)
        assert mock_function_calls_retrieve_response.calls[1].response.json()["items"][0] == res.dump(camel_case=True)

    def test_retrieve_call_with_function_id_and_function_external_id_raises(self, cognite_client):
        with pytest.raises(ValueError) as excinfo:
            cognite_client.functions.calls.retrieve(
                call_id=CALL_ID, function_id=FUNCTION_ID, function_external_id=f"func-no-{FUNCTION_ID}"
            )
        assert "Exactly one of function_id and function_external_id must be specified" == excinfo.value.args[0]

    def test_function_call_logs_by_function_id(self, mock_function_call_logs_response, cognite_client):
        res = cognite_client.functions.calls.get_logs(call_id=CALL_ID, function_id=FUNCTION_ID)
        assert isinstance(res, FunctionCallLog)
        assert mock_function_call_logs_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_functions_retrieve_response")
    def test_function_call_logs_by_function_external_id(self, mock_function_call_logs_response, cognite_client):
        res = cognite_client.functions.calls.get_logs(call_id=CALL_ID, function_external_id=f"func-no-{FUNCTION_ID}")
        assert isinstance(res, FunctionCallLog)
        assert mock_function_call_logs_response.calls[1].response.json()["items"] == res.dump(camel_case=True)

    def test_function_call_logs_by_function_id_and_function_external_id_raises(
        self, mock_function_call_logs_response, cognite_client
    ):
        with pytest.raises(ValueError) as excinfo:
            cognite_client.functions.calls.get_logs(
                call_id=CALL_ID, function_id=FUNCTION_ID, function_external_id=f"func-no-{FUNCTION_ID}"
            )
        assert "Exactly one of function_id and function_external_id must be specified" == excinfo.value.args[0]

    @pytest.mark.usefixtures("mock_function_calls_retrieve_response")
    def test_get_logs_on_retrieved_call_object(self, mock_function_call_logs_response, cognite_client):
        call = cognite_client.functions.calls.retrieve(call_id=CALL_ID, function_id=FUNCTION_ID)
        logs = call.get_logs()
        assert isinstance(logs, FunctionCallLog)
        assert mock_function_call_logs_response.calls[1].response.json()["items"] == logs.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_function_calls_filter_response")
    def test_get_logs_on_listed_call_object(self, mock_function_call_logs_response, cognite_client):
        calls = cognite_client.functions.calls.list(function_id=FUNCTION_ID)
        call = calls[0]
        logs = call.get_logs()
        assert isinstance(logs, FunctionCallLog)
        assert mock_function_call_logs_response.calls[1].response.json()["items"] == logs.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_sessions_with_client_credentials")
    @pytest.mark.usefixtures("mock_functions_call_responses")
    def test_get_logs_on_created_call_object(
        self, mock_function_call_logs_response, cognite_client_with_client_credentials_flow
    ):
        call = cognite_client_with_client_credentials_flow.functions.call(id=FUNCTION_ID)
        logs = call.get_logs()
        assert isinstance(logs, FunctionCallLog)
        assert mock_function_call_logs_response.calls[-1].response.json()["items"] == logs.dump(camel_case=True)

    def test_function_call_response_by_function_id(self, mock_function_call_response_response, cognite_client):
        res = cognite_client.functions.calls.get_response(call_id=CALL_ID, function_id=FUNCTION_ID)
        assert isinstance(res, dict)
        assert mock_function_call_response_response.calls[0].response.json()["response"] == res

    @pytest.mark.usefixtures("mock_functions_retrieve_response")
    def test_function_call_response_by_function_external_id(self, mock_function_call_response_response, cognite_client):
        res = cognite_client.functions.calls.get_response(call_id=CALL_ID, function_id=FUNCTION_ID)
        assert isinstance(res, dict)
        assert mock_function_call_response_response.calls[0].response.json()["response"] == res

    def test_function_call_response_by_function_id_and_function_external_id_raises(self, cognite_client):
        with pytest.raises(ValueError) as excinfo:
            cognite_client.functions.calls.get_response(
                call_id=CALL_ID, function_id=FUNCTION_ID, function_external_id=f"func-no-{FUNCTION_ID}"
            )
        assert "Exactly one of function_id and function_external_id must be specified" == excinfo.value.args[0]

    @pytest.mark.usefixtures("mock_function_calls_retrieve_response")
    def test_get_response_on_retrieved_call_object(self, mock_function_call_response_response, cognite_client):
        call = cognite_client.functions.calls.retrieve(call_id=CALL_ID, function_id=FUNCTION_ID)
        response = call.get_response()
        assert isinstance(response, dict)
        assert mock_function_call_response_response.calls[1].response.json()["response"] == response


@pytest.fixture
def fns_api_with_mock_client(cognite_client):
    cognite_client.functions._cognite_client = MagicMock()
    return cognite_client.functions


@pytest.mark.parametrize(
    "xid, overwrite",
    (
        (None, False),
        ("xid", True),
        ("xid", True),
    ),
)
def test__zip_and_upload_handle__call_signature(fns_api_with_mock_client, xid, overwrite, function_handle):
    mock = fns_api_with_mock_client._cognite_client
    mock.files.upload_bytes.return_value = FileMetadata(id=123)
    file_id = fns_api_with_mock_client._zip_and_upload_handle(function_handle, name="name", external_id=xid)
    assert file_id == 123

    mock.files.upload_bytes.assert_called_once()
    call = mock.files.upload_bytes.call_args
    assert len(call.args) == 1 and type(call.args[0]) is bytes  # noqa: E721
    assert call.kwargs == {"name": "name.zip", "external_id": xid, "overwrite": overwrite, "data_set_id": None}


@pytest.mark.parametrize(
    "xid, overwrite",
    (
        (None, False),
        ("xid", True),
        ("xid", True),
    ),
)
def test__zip_and_upload_handle__zip_file_content(fns_api_with_mock_client, xid, overwrite, function_handle_with_reqs):
    def validate_file_upload_call(*args, **kwargs):
        assert len(args) == 1 and type(args[0]) is bytes  # noqa: E721
        assert kwargs == {"name": "name.zip", "external_id": xid, "overwrite": overwrite, "data_set_id": None}

        with io.BytesIO(args[0]) as wrapped_binary, ZipFile(wrapped_binary, "r") as zip_file:
            assert zip_file.testzip() is None
            assert zip_file.namelist() == ["handler.py", "requirements.txt"]
            with zip_file.open("handler.py", "r") as py_file:
                expected_lines = [
                    "def handle(data, client, secrets):",
                    '    """',
                    "    [requirements]",
                    "    pandas",
                    "    [/requirements]",
                    '    """',
                ]
                # We use splitlines to ignore line ending differences between OSs:
                assert py_file.read().decode("utf-8").splitlines() == expected_lines
        return FileMetadata(id=123)

    mock = fns_api_with_mock_client._cognite_client
    mock.files.upload_bytes = validate_file_upload_call

    file_id = fns_api_with_mock_client._zip_and_upload_handle(function_handle_with_reqs, name="name", external_id=xid)
    assert file_id == 123


@pytest.mark.parametrize(
    "xid, overwrite",
    (
        (None, False),
        ("xid", True),
        ("xid", True),
    ),
)
def test__zip_and_upload_folder__call_signature(fns_api_with_mock_client, xid, overwrite):
    mock = fns_api_with_mock_client._cognite_client
    mock.files.upload_bytes.return_value = FileMetadata(id=123, data_set_id=None)

    folder = Path(__file__).parent / "function_test_resources" / "good_absolute_import"
    file_id = fns_api_with_mock_client._zip_and_upload_folder(folder, name="name", external_id=xid)
    assert file_id == 123

    mock.files.upload_bytes.assert_called_once()
    call = mock.files.upload_bytes.call_args
    assert len(call.args) == 1 and type(call.args[0]) is bytes  # noqa: E721
    assert call.kwargs == {"name": "name.zip", "external_id": xid, "overwrite": overwrite, "data_set_id": None}


@pytest.mark.parametrize(
    "xid, overwrite",
    (
        (None, False),
        ("xid", True),
        ("xid", True),
    ),
)
def test__zip_and_upload_folder__zip_file_content(fns_api_with_mock_client, xid, overwrite):
    def validate_file_upload_call(*args, **kwargs):
        assert len(args) == 1 and type(args[0]) is bytes  # noqa: E721
        assert kwargs == {"name": "name.zip", "external_id": xid, "overwrite": overwrite, "data_set_id": None}

        with io.BytesIO(args[0]) as wrapped_binary, ZipFile(wrapped_binary, "r") as zip_file:
            assert zip_file.testzip() is None
            expected_names = {"./", "shared/", "shared/util.py", "my_functions/", "my_functions/handler.py"}
            assert set(zip_file.namelist()) >= expected_names
            with zip_file.open("my_functions/handler.py", "r") as py_file:
                expected_lines = [
                    "from shared.util import shared_func",
                    "",
                    "",
                    "def handle():",
                    "    return shared_func()",
                ]
                # We use splitlines to ignore line ending differences between OSs:
                assert py_file.read().decode("utf-8").splitlines() == expected_lines
        return FileMetadata(id=123, data_set_id=None)

    mock = fns_api_with_mock_client._cognite_client
    mock.files.upload_bytes = validate_file_upload_call

    folder = Path(__file__).parent / "function_test_resources" / "good_absolute_import"
    file_id = fns_api_with_mock_client._zip_and_upload_folder(folder, name="name", external_id=xid)
    assert file_id == 123
