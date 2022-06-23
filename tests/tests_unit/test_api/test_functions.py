import gzip
import json
import os
from unittest.mock import patch

import pytest
from cognite.client.exceptions import CogniteAPIError

from cognite.client import CogniteClient
from cognite.client._api.functions import _using_client_credential_flow, validate_function_folder
from cognite.client.data_classes import (
    Function,
    FunctionCall,
    FunctionCallList,
    FunctionCallLog,
    FunctionList,
    FunctionSchedule,
    FunctionSchedulesList,
    FunctionsLimits,
)
from tests.utils import jsgz_load


def post_body_matcher(params):
    """
    Used for verifying post-bodies to mocked endpoints. See the `match`-argument in `rsps.add()`.
    """

    def match(request_body):
        if request_body is None:
            return params is None
        else:
            decompressed_body = json.loads(gzip.decompress(request_body))
            sorted_params = sorted(params.items())
            sorted_body = sorted(decompressed_body.items())

            res = sorted_params == sorted_body
            return res

    return match


COGNITE_CLIENT = CogniteClient()
FUNCTIONS_API = COGNITE_CLIENT.functions
FUNCTION_CALLS_API = FUNCTIONS_API.calls
FUNCTION_SCHEDULES_API = FUNCTIONS_API.schedules
FILES_API = COGNITE_CLIENT.files

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
    "apiKey": "***",
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
def mock_sessions_with_client_credentials(rsps):
    url = FUNCTIONS_API._get_base_url_with_base_path() + "/sessions"
    url = url.replace("playground", "v1")
    rsps.add(
        rsps.POST,
        url=url,
        status=200,
        json={"items": [{"nonce": "aabbccdd"}]},
        match=[
            post_body_matcher(
                {
                    "items": [
                        {
                            "clientId": os.environ.get("COGNITE_CLIENT_ID"),
                            "clientSecret": os.environ.get("COGNITE_CLIENT_SECRET"),
                        }
                    ]
                }
            )
        ],
    )

    return rsps


@pytest.fixture
def mock_sessions_with_token_exchange(rsps):
    url = FUNCTIONS_API._get_base_url_with_base_path() + "/sessions"
    url = url.replace("playground", "v1")
    rsps.add(
        rsps.POST,
        url=url,
        status=200,
        json={"items": [{"nonce": "aabbccdd"}]},
        match=[post_body_matcher({"items": [{"tokenExchange": True}]})],
    )

    return rsps


@pytest.fixture
def mock_functions_filter_response(rsps):
    response_body = {"items": [EXAMPLE_FUNCTION]}

    url = FUNCTIONS_API._get_base_url_with_base_path() + "/functions/list"
    rsps.add(rsps.POST, url, status=200, json=response_body)

    yield rsps


@pytest.fixture
def mock_functions_retrieve_response(rsps):
    response_body = {"items": [EXAMPLE_FUNCTION]}

    url = FUNCTIONS_API._get_base_url_with_base_path() + "/functions/byids"
    rsps.add(rsps.POST, url, status=200, json=response_body)

    yield rsps


@pytest.fixture
def mock_functions_create_response(rsps):
    files_response_body = {
        "name": "myfunction",
        "id": FUNCTION_ID,
        "uploaded": True,
        "createdTime": 1585662507939,
        "lastUpdatedTime": 1585662507939,
        "uploadUrl": "https://upload.here",
    }

    rsps.assert_all_requests_are_fired = False

    files_url = FILES_API._get_base_url_with_base_path() + "/files"
    files_byids_url = FILES_API._get_base_url_with_base_path() + "/files/byids"

    rsps.add(rsps.POST, files_url, status=201, json=files_response_body)
    rsps.add(rsps.PUT, "https://upload.here", status=201)
    rsps.add(rsps.POST, files_byids_url, status=201, json={"items": [files_response_body]})
    functions_url = FUNCTIONS_API._get_base_url_with_base_path() + "/functions"
    rsps.add(rsps.POST, functions_url, status=201, json={"items": [EXAMPLE_FUNCTION]})

    yield rsps


@pytest.fixture
def mock_file_not_uploaded(rsps):
    files_response_body = {
        "name": "myfunction",
        "id": FUNCTION_ID,
        "uploaded": False,
        "createdTime": 1585662507939,
        "lastUpdatedTime": 1585662507939,
        "uploadUrl": "https://upload.here",
    }

    files_byids_url = FILES_API._get_base_url_with_base_path() + "/files/byids"

    rsps.add(rsps.POST, files_byids_url, status=201, json={"items": [files_response_body]})
    yield rsps


@pytest.fixture
def mock_functions_delete_response(rsps):
    url = FUNCTIONS_API._get_base_url_with_base_path() + "/functions/delete"
    rsps.add(rsps.POST, url, status=200, json={})

    yield rsps


@pytest.fixture
def mock_functions_call_responses(rsps):
    url = FUNCTIONS_API._get_base_url_with_base_path() + f"/functions/{FUNCTION_ID}/call"
    rsps.add(rsps.POST, url, status=201, json=CALL_RUNNING)

    url = FUNCTIONS_API._get_base_url_with_base_path() + f"/functions/{FUNCTION_ID}/calls/byids"
    rsps.add(rsps.POST, url, status=200, json={"items": [CALL_COMPLETED]})

    yield rsps


@pytest.fixture
def mock_sessions_bad_request_response(rsps):
    url = FUNCTIONS_API._get_base_url_with_base_path() + "/sessions"
    url = url.replace("playground", "v1")
    rsps.add(rsps.POST, url, status=400)

    yield rsps


@pytest.fixture
def mock_functions_call_by_external_id_responses(mock_functions_retrieve_response):
    rsps = mock_functions_retrieve_response

    url = FUNCTIONS_API._get_base_url_with_base_path() + f"/functions/{FUNCTION_ID}/call"
    rsps.add(rsps.POST, url, status=201, json=CALL_RUNNING)

    url = FUNCTIONS_API._get_base_url_with_base_path() + f"/functions/{FUNCTION_ID}/calls/byids"
    rsps.add(rsps.POST, url, status=200, json={"items": [CALL_COMPLETED]})

    yield rsps


@pytest.fixture
def mock_functions_call_failed_response(rsps):
    url = FUNCTIONS_API._get_base_url_with_base_path() + f"/functions/{FUNCTION_ID}/call"
    rsps.add(rsps.POST, url, status=201, json=CALL_FAILED)

    yield rsps


@pytest.fixture
def mock_functions_call_timeout_response(rsps):
    url = FUNCTIONS_API._get_base_url_with_base_path() + f"/functions/{FUNCTION_ID}/call"
    rsps.add(rsps.POST, url, status=201, json=CALL_TIMEOUT)

    yield rsps


@pytest.fixture
def function_handle():
    def handle(data, client, secrets):
        pass

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
def mock_function_calls_filter_response(rsps):
    response_body = {"items": [CALL_COMPLETED, CALL_SCHEDULED]}
    url = FUNCTIONS_API._get_base_url_with_base_path() + f"/functions/{FUNCTION_ID}/calls/list"
    rsps.add(rsps.POST, url, status=200, json=response_body)

    yield rsps


@pytest.fixture
def cognite_client_with_api_key():
    client = CogniteClient(
        api_key="caner_was_here_but_not_for_long_because_api_keys_will_be_removed",
        disable_pypi_version_check=True,
    )
    client.config.token_client_id = None  # Disables Client Credentials coming from the ENV

    return client


@pytest.fixture
def cognite_client_with_token():
    client = CogniteClient(
        token="aabbccddeeffgg",
        disable_pypi_version_check=True,
    )
    client.config.token_client_id = None  # Disables Client Credentials coming from the ENV

    return client


@pytest.fixture
def mock_function_calls_filter_response_with_limit(rsps):
    response_body = {"items": [CALL_COMPLETED, CALL_SCHEDULED]}
    url = FUNCTIONS_API._get_base_url_with_base_path() + f"/functions/{FUNCTION_ID}/calls/list"
    rsps.add(rsps.POST, url, status=200, json=response_body)

    yield rsps


@pytest.fixture
def mock_functions_limit_response(rsps):
    response_body = {
        "timeoutMinutes": 10,
        "cpuCores": {"min": 0.1, "max": 0.6, "default": 0.25},
        "memoryGb": {"min": 0.1, "max": 2.5, "default": 1.0},
        "responseSizeMb": 1,
        "runtimes": ["py37", "py38", "py39"],
    }
    url = FUNCTIONS_API._get_base_url_with_base_path() + f"/functions/limits"
    rsps.add(rsps.GET, url, status=200, json=response_body)

    yield rsps


class TestFunctionsAPI:
    @pytest.mark.parametrize(
        "function_folder, function_path, exception",
        [
            (".", "handler.py", None),
            ("function_code", "./handler.py", None),
            ("bad_function_code", "handler.py", TypeError),
            ("bad_function_code2", "handler.py", TypeError),
            ("./good_absolute_import/", "my_functions/handler.py", None),
            ("bad_absolute_import", "extra_root_folder/my_functions/handler.py", ModuleNotFoundError),
            ("relative_imports", "my_functions/good_relative_import.py", None),
            ("relative_imports", "bad_relative_import.py", ImportError),
        ],
    )
    def test_validate_folder(self, function_folder, function_path, exception):
        folder = os.path.join(os.path.dirname(__file__), function_folder)
        if exception is None:
            validate_function_folder(folder, function_path)
        else:
            with pytest.raises(exception):
                validate_function_folder(folder, function_path)

    @patch("cognite.client._api.functions.MAX_RETRIES", 1)
    def test_create_function_with_file_not_uploaded(self, mock_file_not_uploaded):
        with pytest.raises(IOError):
            FUNCTIONS_API.create(name="myfunction", file_id=123)

    def test_create_with_path(self, mock_functions_create_response):
        folder = os.path.join(os.path.dirname(__file__), "function_code")
        res = FUNCTIONS_API.create(name="myfunction", folder=folder, function_path="handler.py")

        assert isinstance(res, Function)
        assert mock_functions_create_response.calls[3].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_with_file_id(self, mock_functions_create_response):
        res = FUNCTIONS_API.create(name="myfunction", file_id=1234)

        assert isinstance(res, Function)
        assert mock_functions_create_response.calls[1].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_with_function_handle(self, mock_functions_create_response, function_handle):
        res = FUNCTIONS_API.create(name="myfunction", function_handle=function_handle)

        assert isinstance(res, Function)
        assert mock_functions_create_response.calls[3].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_with_function_handle_with_illegal_name_raises(self, function_handle_illegal_name):
        with pytest.raises(TypeError):
            FUNCTIONS_API.create(name="myfunction", function_handle=function_handle_illegal_name)

    def test_create_with_function_handle_with_illegal_argument_raises(self, function_handle_illegal_argument):
        with pytest.raises(TypeError):
            FUNCTIONS_API.create(name="myfunction", function_handle=function_handle_illegal_argument)

    def test_create_with_handle_function_and_file_id_raises(self, mock_functions_create_response, function_handle):
        with pytest.raises(TypeError):
            FUNCTIONS_API.create(name="myfunction", function_handle=function_handle, file_id=1234)

    def test_create_with_path_and_file_id_raises(self, mock_functions_create_response):
        with pytest.raises(TypeError):
            FUNCTIONS_API.create(name="myfunction", folder="some/folder", file_id=1234, function_path="handler.py")

    def test_create_with_cpu_and_memory(self, mock_functions_create_response):
        res = FUNCTIONS_API.create(name="myfunction", file_id=1234, cpu=0.2, memory=1)

        assert isinstance(res, Function)
        assert mock_functions_create_response.calls[1].response.json()["items"][0] == res.dump(camel_case=True)

    def test_create_with_cpu_not_float_raises(self, mock_functions_create_response):
        with pytest.raises(TypeError):
            FUNCTIONS_API.create(name="myfunction", file_id=1234, cpu="0.2")

    def test_create_with_memory_not_float_raises(self, mock_functions_create_response):
        with pytest.raises(TypeError):
            FUNCTIONS_API.create(name="myfunction", file_id=1234, memory="0.5")

    def test_delete_single_id(self, mock_functions_delete_response):
        _ = FUNCTIONS_API.delete(id=1)
        assert {"items": [{"id": 1}]} == jsgz_load(mock_functions_delete_response.calls[0].request.body)

    def test_delete_single_external_id(self, mock_functions_delete_response):
        _ = FUNCTIONS_API.delete(external_id="func1")
        assert {"items": [{"externalId": "func1"}]} == jsgz_load(mock_functions_delete_response.calls[0].request.body)

    def test_delete_multiple_id_and_multiple_external_id(self, mock_functions_delete_response):
        _ = FUNCTIONS_API.delete(id=[1, 2, 3], external_id=["func1", "func2"])
        assert {
            "items": [{"id": 1}, {"id": 2}, {"id": 3}, {"externalId": "func1"}, {"externalId": "func2"}]
        } == jsgz_load(mock_functions_delete_response.calls[0].request.body)

    def test_list(self, mock_functions_filter_response):
        res = FUNCTIONS_API.list()

        assert isinstance(res, FunctionList)
        assert mock_functions_filter_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_list_with_limits(self, mock_functions_filter_response):

        res = FUNCTIONS_API.list(limit=1)
        assert isinstance(res, FunctionList)
        assert len(res) == 1

    def test_retrieve_by_id(self, mock_functions_retrieve_response):
        res = FUNCTIONS_API.retrieve(id=1)
        assert isinstance(res, Function)
        assert mock_functions_retrieve_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_retrieve_by_external_id(self, mock_functions_retrieve_response):
        res = FUNCTIONS_API.retrieve(external_id="func1")
        assert isinstance(res, Function)
        assert mock_functions_retrieve_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_retrieve_by_id_and_external_id_raises(self):
        with pytest.raises(AssertionError):
            FUNCTIONS_API.retrieve(id=1, external_id="func1")

    def test_retrieve_multiple_by_ids(self, mock_functions_retrieve_response):
        res = FUNCTIONS_API.retrieve_multiple(ids=[1])
        assert isinstance(res, FunctionList)
        assert mock_functions_retrieve_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_retrieve_multiple_by_external_ids(self, mock_functions_retrieve_response):
        res = FUNCTIONS_API.retrieve_multiple(external_ids=["func1"])
        assert isinstance(res, FunctionList)
        assert mock_functions_retrieve_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_retrieve_multiple_by_ids_and_external_ids(self, mock_functions_retrieve_response):
        res = FUNCTIONS_API.retrieve_multiple(ids=[1], external_ids=["func1"])
        assert isinstance(res, FunctionList)
        assert mock_functions_retrieve_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_function_call_from_api_key_flow(self, mock_functions_call_responses, cognite_client_with_api_key):
        res = cognite_client_with_api_key.functions.call(id=FUNCTION_ID)
        assert isinstance(res, FunctionCall)
        assert mock_functions_call_responses.calls[1].response.json()["items"][0] == res.dump(camel_case=True)

    def test_function_call_by_external_id_from_api_key_flow(
        self, mock_functions_call_by_external_id_responses, cognite_client_with_api_key
    ):
        res = cognite_client_with_api_key.functions.call(external_id=f"func-no-{FUNCTION_ID}")

        assert isinstance(res, FunctionCall)
        assert mock_functions_call_by_external_id_responses.calls[2].response.json()["items"][0] == res.dump(
            camel_case=True
        )

    def test_function_call_failed_from_api_key_flow(
        self, mock_functions_call_failed_response, cognite_client_with_api_key
    ):
        res = cognite_client_with_api_key.functions.call(id=FUNCTION_ID)
        assert isinstance(res, FunctionCall)
        assert mock_functions_call_failed_response.calls[0].response.json() == res.dump(camel_case=True)

    def test_function_call_timeout_from_api_key_flow(
        self, mock_functions_call_timeout_response, cognite_client_with_api_key
    ):
        res = cognite_client_with_api_key.functions.call(id=FUNCTION_ID)
        assert isinstance(res, FunctionCall)
        assert mock_functions_call_timeout_response.calls[0].response.json() == res.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_sessions_with_client_credentials")
    def test_function_call_from_oidc_client_credentials_flow(self, mock_functions_call_responses):
        assert _using_client_credential_flow(COGNITE_CLIENT)
        res = FUNCTIONS_API.call(id=FUNCTION_ID)

        assert isinstance(res, FunctionCall)
        assert mock_functions_call_responses.calls[2].response.json()["items"][0] == res.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_sessions_with_client_credentials")
    def test_function_call_by_external_id_from_oidc_client_credentials_flow(
        self, mock_functions_call_by_external_id_responses
    ):
        assert _using_client_credential_flow(COGNITE_CLIENT)
        res = FUNCTIONS_API.call(external_id=f"func-no-{FUNCTION_ID}")

        assert isinstance(res, FunctionCall)
        assert mock_functions_call_by_external_id_responses.calls[3].response.json()["items"][0] == res.dump(
            camel_case=True
        )

    @pytest.mark.usefixtures("mock_sessions_bad_request_response")
    def test_function_call_with_failing_client_credentials_flow(self):
        with pytest.raises(CogniteAPIError) as excinfo:
            assert _using_client_credential_flow(COGNITE_CLIENT)
            FUNCTIONS_API.call(id=FUNCTION_ID)
        assert "Failed to create session using client credentials flow." in str(excinfo.value)

    @pytest.mark.usefixtures("mock_sessions_with_client_credentials")
    def test_function_call_timeout_from_oidc_client_credentials_flow(self, mock_functions_call_timeout_response):
        assert _using_client_credential_flow(COGNITE_CLIENT)

        res = FUNCTIONS_API.call(id=FUNCTION_ID)
        assert isinstance(res, FunctionCall)
        assert mock_functions_call_timeout_response.calls[1].response.json() == res.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_sessions_with_token_exchange")
    def test_function_call_from_oidc_token_exchange_flow(
        self, mock_functions_call_responses, cognite_client_with_token
    ):
        assert not _using_client_credential_flow(cognite_client_with_token)
        res = cognite_client_with_token.functions.call(id=FUNCTION_ID)

        assert isinstance(res, FunctionCall)
        assert mock_functions_call_responses.calls[2].response.json()["items"][0] == res.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_sessions_with_token_exchange")
    def test_function_call_by_external_id_from_oidc_token_exchange_flow(
        self, mock_functions_call_by_external_id_responses, cognite_client_with_token
    ):
        assert not _using_client_credential_flow(cognite_client_with_token)

        res = cognite_client_with_token.functions.call(external_id=f"func-no-{FUNCTION_ID}")

        assert isinstance(res, FunctionCall)
        assert mock_functions_call_by_external_id_responses.calls[3].response.json()["items"][0] == res.dump(
            camel_case=True
        )

    @pytest.mark.usefixtures("mock_sessions_bad_request_response")
    def test_function_call_with_failing_token_exchange_flow(self, cognite_client_with_token):
        assert not _using_client_credential_flow(cognite_client_with_token)

        with pytest.raises(CogniteAPIError) as excinfo:
            assert not _using_client_credential_flow(cognite_client_with_token)
            cognite_client_with_token.functions.call(id=FUNCTION_ID)
        assert "Failed to create session using token exchange flow." in str(excinfo.value)

    @pytest.mark.usefixtures("mock_sessions_with_token_exchange")
    def test_function_call_timeout_from_from_oidc_token_exchange_flow(
        self, mock_functions_call_timeout_response, cognite_client_with_token
    ):
        assert not _using_client_credential_flow(cognite_client_with_token)

        res = cognite_client_with_token.functions.call(id=FUNCTION_ID)
        assert isinstance(res, FunctionCall)
        assert mock_functions_call_timeout_response.calls[1].response.json() == res.dump(camel_case=True)

    def test_functions_limits_endpoint(self, mock_functions_limit_response):
        res = FUNCTIONS_API.limits()
        assert isinstance(res, FunctionsLimits)
        assert mock_functions_limit_response.calls[0].response.json() == res.dump(camel_case=True)


@pytest.fixture
def mock_function_calls_retrieve_response(rsps):
    response_body = CALL_COMPLETED
    url = FUNCTIONS_API._get_base_url_with_base_path() + f"/functions/{FUNCTION_ID}/calls/byids"
    rsps.add(rsps.POST, url, status=200, json={"items": [response_body]})

    yield rsps


@pytest.fixture
def mock_function_call_response_response(rsps):
    response_body = {"callId": CALL_ID, "functionId": 1234, "response": {"key": "value"}}
    url = FUNCTIONS_API._get_base_url_with_base_path() + f"/functions/{FUNCTION_ID}/calls/{CALL_ID}/response"
    rsps.assert_all_requests_are_fired = False
    rsps.add(rsps.GET, url, status=200, json=response_body)

    yield rsps


@pytest.fixture
def mock_function_call_logs_response(rsps):
    response_body = {
        "items": [
            {"timestamp": 1585925306822, "message": "message 1"},
            {"timestamp": 1585925310822, "message": "message 2"},
        ]
    }
    url = FUNCTIONS_API._get_base_url_with_base_path() + f"/functions/{FUNCTION_ID}/calls/{CALL_ID}/logs"
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
def mock_filter_function_schedules_response(rsps):
    url = FUNCTIONS_API._get_base_url_with_base_path() + "/functions/schedules/list"
    rsps.add(rsps.POST, url, status=200, json={"items": [SCHEDULE_WITH_FUNCTION_EXTERNAL_ID]})

    yield rsps


@pytest.fixture
def mock_function_schedules_response(rsps):
    url = FUNCTIONS_API._get_base_url_with_base_path() + "/functions/schedules"
    rsps.assert_all_requests_are_fired = False
    rsps.add(rsps.GET, url, status=200, json={"items": [SCHEDULE_WITH_FUNCTION_EXTERNAL_ID]})
    rsps.add(rsps.POST, url, status=200, json={"items": [SCHEDULE_WITH_FUNCTION_EXTERNAL_ID]})

    yield rsps


@pytest.fixture
def mock_function_schedules_response_oidc_client_credentials(rsps):
    session_url = FUNCTIONS_API._get_base_url_with_base_path() + "/sessions"
    session_url = session_url.replace("playground", "v1")
    rsps.add(
        rsps.POST,
        session_url,
        status=200,
        json={"items": [{"nonce": "aaabbb"}]},
        match=[post_body_matcher({"items": [{"clientId": "aabbccdd", "clientSecret": "xxyyzz"}]})],
    )

    url = FUNCTIONS_API._get_base_url_with_base_path() + "/functions/schedules"
    rsps.add(rsps.POST, url, status=200, json={"items": [SCHEDULE_WITH_FUNCTION_ID_AND_SESSION]})
    yield rsps


@pytest.fixture
def mock_function_schedules_retrieve_response(rsps):
    url = FUNCTIONS_API._get_base_url_with_base_path() + "/functions/schedules/byids"
    rsps.add(rsps.POST, url, status=200, json={"items": [SCHEDULE_WITH_FUNCTION_EXTERNAL_ID]})

    yield rsps


@pytest.fixture
def mock_function_schedules_delete_response(rsps):
    url = FUNCTIONS_API._get_base_url_with_base_path() + "/functions/schedules/delete"
    rsps.add(rsps.POST, url, status=200, json={})

    yield rsps


@pytest.fixture
def mock_schedule_get_data_response(rsps):
    url = (
        FUNCTIONS_API._get_base_url_with_base_path()
        + f"/functions/schedules/{SCHEDULE_WITH_FUNCTION_ID_AND_SESSION['id']}/input_data"
    )
    rsps.add(rsps.GET, url, status=200, json={"id": SCHEDULE_WITH_FUNCTION_ID_AND_SESSION["id"], "data": {"value": 2}})

    yield rsps


class TestFunctionSchedulesAPI:
    def test_retrieve_schedules(self, mock_function_schedules_retrieve_response):
        res = FUNCTION_SCHEDULES_API.retrieve(id=SCHEDULE_WITH_FUNCTION_EXTERNAL_ID["id"])
        assert isinstance(res, FunctionSchedule)
        expected = mock_function_schedules_retrieve_response.calls[0].response.json()["items"][0]
        expected.pop("when")
        assert expected == res.dump(camel_case=True)

    def test_list_schedules(self, mock_filter_function_schedules_response):
        res = FUNCTION_SCHEDULES_API.list()
        assert isinstance(res, FunctionSchedulesList)
        expected = mock_filter_function_schedules_response.calls[0].response.json()["items"]
        expected[0].pop("when")
        assert expected == res.dump(camel_case=True)

    def test_list_schedules_with_limit(self, mock_filter_function_schedules_response):
        res = FUNCTION_SCHEDULES_API.list(limit=1)

        assert isinstance(res, FunctionSchedulesList)
        assert len(res) == 1

    def test_list_schedules_with_function_id_and_function_external_id_raises(self):
        with pytest.raises(AssertionError) as excinfo:
            FUNCTION_SCHEDULES_API.list(function_id=123, function_external_id="my-func")
        assert "Only function_id or function_external_id allowed when listing schedules." in str(excinfo.value)

    def test_create_schedules_with_function_external_id(self, mock_function_schedules_response):
        res = FUNCTION_SCHEDULES_API.create(
            name="my-schedule",
            function_external_id="user/hello-cognite/hello-cognite:latest",
            cron_expression="*/5 * * * *",
            description="Hi",
        )
        assert isinstance(res, FunctionSchedule)
        expected = mock_function_schedules_response.calls[0].response.json()["items"][0]
        expected.pop("when")
        assert expected == res.dump(camel_case=True)

    def test_create_schedules_with_function_id_and_client_credentials(
        self, mock_function_schedules_response_oidc_client_credentials
    ):
        res = FUNCTION_SCHEDULES_API.create(
            name="my-schedule",
            function_id=123,
            cron_expression="*/5 * * * *",
            description="Hi",
            client_credentials={"client_id": "aabbccdd", "client_secret": "xxyyzz"},
        )

        assert isinstance(res, FunctionSchedule)
        expected = mock_function_schedules_response_oidc_client_credentials.calls[1].response.json()["items"][0]
        expected.pop("when")
        assert expected == res.dump(camel_case=True)

    def test_create_schedules_with_function_external_id_and_client_credentials_raises(self):
        with pytest.raises(AssertionError) as excinfo:
            FUNCTION_SCHEDULES_API.create(
                name="my-schedule",
                function_external_id="user/hello-cognite/hello-cognite:latest",
                cron_expression="*/5 * * * *",
                description="Hi",
                client_credentials={"client_id": "aabbccdd", "client_secret": "xxyyzz"},
            )
        assert "function_id must be set when creating a schedule with client_credentials." in str(excinfo.value)

    def test_create_schedules_with_function_id_and_function_external_id_raises(self):
        with pytest.raises(AssertionError) as excinfo:
            FUNCTION_SCHEDULES_API.create(
                name="my-schedule",
                function_id=123,
                function_external_id="user/hello-cognite/hello-cognite:latest",
                cron_expression="*/5 * * * *",
                description="Hi",
                client_credentials={"client_id": "aabbccdd", "client_secret": "xxyyzz"},
            )
        assert "Exactly one of function_id and function_external_id must be specified" in str(excinfo.value)

    def test_create_schedules_with_data(self, mock_function_schedules_response):
        res = FUNCTION_SCHEDULES_API.create(
            name="my-schedule",
            function_external_id="user/hello-cognite/hello-cognite:latest",
            cron_expression="*/5 * * * *",
            description="Hi",
            data={"value": 2},
        )
        assert isinstance(res, FunctionSchedule)
        expected = mock_function_schedules_response.calls[0].response.json()["items"][0]
        expected.pop("when")
        assert expected == res.dump(camel_case=True)

    def test_delete_schedules(self, mock_function_schedules_delete_response):
        res = FUNCTION_SCHEDULES_API.delete(id=8012683333564363)
        assert res is None

    def test_schedule_get_data(self, mock_schedule_get_data_response):
        res = FUNCTION_SCHEDULES_API.get_input_data(id=8012683333564363)

        assert isinstance(res, dict)
        expected = mock_schedule_get_data_response.calls[0].response.json()["data"]
        assert res == expected


class TestFunctionCallsAPI:
    def test_list_calls_and_filter(self, mock_function_calls_filter_response, mock_functions_retrieve_response):
        filter_kwargs = {
            "status": "Completed",
            "schedule_id": 123,
            "start_time": {"min": 1585925306822, "max": 1585925306823},
            "end_time": {"min": 1585925310822, "max": 1585925310823},
        }
        res = FUNCTIONS_API.retrieve(id=FUNCTION_ID).list_calls(**filter_kwargs, limit=-1)

        assert isinstance(res, FunctionCallList)
        assert mock_function_calls_filter_response.calls[1].response.json()["items"] == res.dump(camel_case=True)

    def test_list_calls_by_function_id(self, mock_function_calls_filter_response):
        filter_kwargs = {
            "status": "Completed",
            "schedule_id": 123,
            "start_time": {"min": 1585925306822, "max": 1585925306823},
            "end_time": {"min": 1585925310822, "max": 1585925310823},
        }
        res = FUNCTION_CALLS_API.list(function_id=FUNCTION_ID, **filter_kwargs, limit=-1)
        assert isinstance(res, FunctionCallList)
        assert mock_function_calls_filter_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_list_calls_by_function_id_with_limits(self, mock_function_calls_filter_response_with_limit):
        res = FUNCTION_CALLS_API.list(function_id=FUNCTION_ID, limit=2)
        assert isinstance(res, FunctionCallList)
        assert len(res) == 2

    @pytest.mark.usefixtures("mock_functions_retrieve_response")
    def test_list_calls_by_function_external_id(self, mock_function_calls_filter_response):
        res = FUNCTION_CALLS_API.list(function_external_id=f"func-no-{FUNCTION_ID}")
        assert isinstance(res, FunctionCallList)
        assert mock_function_calls_filter_response.calls[1].response.json()["items"] == res.dump(camel_case=True)

    def test_list_calls_with_function_id_and_function_external_id_raises(self):
        with pytest.raises(AssertionError) as excinfo:
            FUNCTION_CALLS_API.list(function_id=123, function_external_id="my-function")
        assert "Exactly one of function_id and function_external_id must be specified" in str(excinfo.value)

    def test_retrieve_call_by_function_id(self, mock_function_calls_retrieve_response):
        res = FUNCTION_CALLS_API.retrieve(call_id=CALL_ID, function_id=FUNCTION_ID)
        assert isinstance(res, FunctionCall)
        assert mock_function_calls_retrieve_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_functions_retrieve_response")
    def test_retrieve_call_by_function_external_id(self, mock_function_calls_retrieve_response):
        res = FUNCTION_CALLS_API.retrieve(call_id=CALL_ID, function_external_id=f"func-no-{FUNCTION_ID}")
        assert isinstance(res, FunctionCall)
        assert mock_function_calls_retrieve_response.calls[1].response.json()["items"][0] == res.dump(camel_case=True)

    def test_retrieve_call_with_function_id_and_function_external_id_raises(self):
        with pytest.raises(AssertionError) as excinfo:
            FUNCTION_CALLS_API.retrieve(
                call_id=CALL_ID, function_id=FUNCTION_ID, function_external_id=f"func-no-{FUNCTION_ID}"
            )
        assert "Exactly one of function_id and function_external_id must be specified" in str(excinfo.value)

    def test_function_call_logs_by_function_id(self, mock_function_call_logs_response):
        res = FUNCTION_CALLS_API.get_logs(call_id=CALL_ID, function_id=FUNCTION_ID)
        assert isinstance(res, FunctionCallLog)
        assert mock_function_call_logs_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_functions_retrieve_response")
    def test_function_call_logs_by_function_external_id(self, mock_function_call_logs_response):
        res = FUNCTION_CALLS_API.get_logs(call_id=CALL_ID, function_external_id=f"func-no-{FUNCTION_ID}")
        assert isinstance(res, FunctionCallLog)
        assert mock_function_call_logs_response.calls[1].response.json()["items"] == res.dump(camel_case=True)

    def test_function_call_logs_by_function_id_and_function_external_id_raises(self, mock_function_call_logs_response):
        with pytest.raises(AssertionError) as excinfo:
            FUNCTION_CALLS_API.get_logs(
                call_id=CALL_ID, function_id=FUNCTION_ID, function_external_id=f"func-no-{FUNCTION_ID}"
            )
        assert "Exactly one of function_id and function_external_id must be specified" in str(excinfo.value)

    @pytest.mark.usefixtures("mock_function_calls_retrieve_response")
    def test_get_logs_on_retrieved_call_object(self, mock_function_call_logs_response):
        call = FUNCTION_CALLS_API.retrieve(call_id=CALL_ID, function_id=FUNCTION_ID)
        logs = call.get_logs()
        assert isinstance(logs, FunctionCallLog)
        assert mock_function_call_logs_response.calls[1].response.json()["items"] == logs.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_function_calls_filter_response")
    def test_get_logs_on_listed_call_object(self, mock_function_call_logs_response):
        calls = FUNCTION_CALLS_API.list(function_id=FUNCTION_ID)
        call = calls[0]
        logs = call.get_logs()
        assert isinstance(logs, FunctionCallLog)
        assert mock_function_call_logs_response.calls[1].response.json()["items"] == logs.dump(camel_case=True)

    @pytest.mark.usefixtures("mock_sessions_with_client_credentials")
    @pytest.mark.usefixtures("mock_functions_call_responses")
    def test_get_logs_on_created_call_object(self, mock_function_call_logs_response):
        call = FUNCTIONS_API.call(id=FUNCTION_ID)
        logs = call.get_logs()
        assert isinstance(logs, FunctionCallLog)
        assert mock_function_call_logs_response.calls[3].response.json()["items"] == logs.dump(camel_case=True)

    def test_function_call_response_by_function_id(self, mock_function_call_response_response):
        res = FUNCTION_CALLS_API.get_response(call_id=CALL_ID, function_id=FUNCTION_ID)
        assert isinstance(res, dict)
        assert mock_function_call_response_response.calls[0].response.json()["response"] == res

    @pytest.mark.usefixtures("mock_functions_retrieve_response")
    def test_function_call_response_by_function_external_id(self, mock_function_call_response_response):
        res = FUNCTION_CALLS_API.get_response(call_id=CALL_ID, function_id=FUNCTION_ID)
        assert isinstance(res, dict)
        assert mock_function_call_response_response.calls[0].response.json()["response"] == res

    def test_function_call_response_by_function_id_and_function_external_id_raises(self):
        with pytest.raises(AssertionError) as excinfo:
            FUNCTION_CALLS_API.get_response(
                call_id=CALL_ID, function_id=FUNCTION_ID, function_external_id=f"func-no-{FUNCTION_ID}"
            )
        assert "Exactly one of function_id and function_external_id must be specified" in str(excinfo.value)

    @pytest.mark.usefixtures("mock_function_calls_retrieve_response")
    def test_get_response_on_retrieved_call_object(self, mock_function_call_response_response):
        call = FUNCTION_CALLS_API.retrieve(call_id=CALL_ID, function_id=FUNCTION_ID)
        response = call.get_response()
        assert isinstance(response, dict)
        assert mock_function_call_response_response.calls[1].response.json()["response"] == response
