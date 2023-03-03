import importlib.util
import os
import re
import sys
import time
from inspect import getdoc, getsource
from numbers import Number
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client._constants import LIST_LIMIT_CEILING, LIST_LIMIT_DEFAULT
from cognite.client.credentials import OAuthClientCredentials, Token
from cognite.client.data_classes import (
    Function,
    FunctionCall,
    FunctionCallList,
    FunctionCallLog,
    FunctionFilter,
    FunctionList,
    FunctionSchedule,
    FunctionSchedulesFilter,
    FunctionSchedulesList,
    FunctionsLimits,
)
from cognite.client.data_classes.files import FileMetadata
from cognite.client.data_classes.functions import FunctionCallsFilter, FunctionsStatus
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._auxiliary import is_unlimited
from cognite.client.utils._identifier import IdentifierSequence

HANDLER_FILE_NAME = "handler.py"
MAX_RETRIES = 5
REQUIREMENTS_FILE_NAME = "requirements.txt"
REQUIREMENTS_REG = re.compile("(\\[\\/?requirements\\]){1}$", flags=re.M)
UNCOMMENTED_LINE_REG = re.compile("^[^\\#]]*.*")


def _get_function_internal_id(_cognite_client, identifier):
    id_object = identifier[0]
    id_dict = id_object.as_dict()
    if "id" in id_dict:
        return id_object.as_primitive()
    if "externalId" in id_dict:
        function = _cognite_client.functions.retrieve(external_id=id_object.as_primitive())
        if function:
            return function.id
    raise ValueError(f'Function with external ID "{id_object.as_primitive()}" is not found')


def _get_function_identifier(id, external_id):
    identifier = IdentifierSequence.load(ids=id, external_ids=external_id)
    if identifier.is_singleton():
        return identifier.as_singleton()
    raise AssertionError("Exactly one of function_id and function_external_id must be specified")


class FunctionsAPI(APIClient):
    _RESOURCE_PATH = "/functions"
    _LIST_CLASS = FunctionList

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls = FunctionCallsAPI(*args, **kwargs)
        self.schedules = FunctionSchedulesAPI(*args, **kwargs)
        self._cognite_client: CogniteClient = cast("CogniteClient", self._cognite_client)

    def create(
        self,
        name,
        folder=None,
        file_id=None,
        function_path=HANDLER_FILE_NAME,
        function_handle=None,
        external_id=None,
        description="",
        owner="",
        api_key=None,
        secrets=None,
        env_vars=None,
        cpu=None,
        memory=None,
        runtime=None,
        metadata=None,
        index_url=None,
        extra_index_urls=None,
    ):
        self._assert_exactly_one_of_folder_or_file_id_or_function_handle(folder, file_id, function_handle)
        if folder:
            validate_function_folder(folder, function_path)
            file_id = self._zip_and_upload_folder(folder, name, external_id)
        elif function_handle:
            _validate_function_handle(function_handle)
            file_id = self._zip_and_upload_handle(function_handle, name, external_id)
        utils._auxiliary.assert_type(cpu, "cpu", [Number], allow_none=True)
        utils._auxiliary.assert_type(memory, "memory", [Number], allow_none=True)
        sleep_time = 1.0
        for i in range(MAX_RETRIES):
            file = self._cognite_client.files.retrieve(id=file_id)
            if (file is None) or (not file.uploaded):
                time.sleep(sleep_time)
                sleep_time *= 2
            else:
                break
        else:
            raise OSError("Could not retrieve file from files API")
        url = "/functions"
        function = {
            "name": name,
            "description": description,
            "owner": owner,
            "fileId": file_id,
            "functionPath": function_path,
            "envVars": env_vars,
            "metadata": metadata,
        }
        if cpu:
            function["cpu"] = cpu
        if memory:
            function["memory"] = memory
        if runtime:
            function["runtime"] = runtime
        if external_id:
            function["externalId"] = external_id
        if api_key:
            function["apiKey"] = api_key
        if secrets:
            function["secrets"] = secrets
        if extra_index_urls:
            function["extraIndexUrls"] = extra_index_urls
        if index_url:
            function["indexUrl"] = index_url
        body = {"items": [function]}
        res = self._post(url, json=body)
        return Function._load(res.json()["items"][0], cognite_client=self._cognite_client)

    def delete(self, id=None, external_id=None):
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id, external_ids=external_id), wrap_ids=True)

    def list(
        self,
        name=None,
        owner=None,
        file_id=None,
        status=None,
        external_id_prefix=None,
        created_time=None,
        limit=LIST_LIMIT_DEFAULT,
    ):
        if is_unlimited(limit):
            limit = LIST_LIMIT_CEILING
        filter = FunctionFilter(
            name=name,
            owner=owner,
            file_id=file_id,
            status=status,
            external_id_prefix=external_id_prefix,
            created_time=created_time,
        ).dump(camel_case=True)
        res = self._post(url_path=f"{self._RESOURCE_PATH}/list", json={"filter": filter, "limit": limit})
        return self._LIST_CLASS._load(res.json()["items"], cognite_client=self._cognite_client)

    def retrieve(self, id=None, external_id=None):
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(identifiers=identifiers, resource_cls=Function, list_cls=FunctionList)

    def retrieve_multiple(self, ids=None, external_ids=None):
        utils._auxiliary.assert_type(ids, "id", [Sequence], allow_none=True)
        utils._auxiliary.assert_type(external_ids, "external_id", [Sequence], allow_none=True)
        return self._retrieve_multiple(
            identifiers=IdentifierSequence.load(ids=ids, external_ids=external_ids),
            resource_cls=Function,
            list_cls=FunctionList,
        )

    def call(self, id=None, external_id=None, data=None, wait=True):
        identifier = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        id = _get_function_internal_id(self._cognite_client, identifier)
        nonce = None
        if _using_client_credential_flow(self._cognite_client):
            nonce = _use_client_credentials(self._cognite_client, client_credentials=None)
        elif _using_token_exchange_flow(self._cognite_client):
            nonce = _use_token_exchange(self._cognite_client)
        if data is None:
            data = {}
        body = {"data": data, "nonce": nonce}
        url = f"/functions/{id}/call"
        res = self._post(url, json=body)
        function_call = FunctionCall._load(res.json(), cognite_client=self._cognite_client)
        if wait:
            function_call.wait()
        return function_call

    def limits(self):
        res = self._get("/functions/limits")
        return FunctionsLimits._load(res.json())

    def _zip_and_upload_folder(self, folder, name, external_id=None):
        name = name.replace("/", "-")
        current_dir = os.getcwd()
        os.chdir(folder)
        try:
            with TemporaryDirectory() as tmpdir:
                zip_path = os.path.join(tmpdir, "function.zip")
                with ZipFile(zip_path, "w") as zf:
                    for (root, dirs, files) in os.walk("."):
                        zf.write(root)
                        for filename in files:
                            zf.write(os.path.join(root, filename))
                overwrite = True if external_id else False
                file = cast(
                    FileMetadata,
                    self._cognite_client.files.upload(
                        zip_path, name=f"{name}.zip", external_id=external_id, overwrite=overwrite
                    ),
                )
            file_id = cast(int, file.id)
            return file_id
        finally:
            os.chdir(current_dir)

    def _zip_and_upload_handle(self, function_handle, name, external_id=None):
        name = name.replace("/", "-")
        docstr_requirements = _get_fn_docstring_requirements(function_handle)
        with TemporaryDirectory() as tmpdir:
            handle_path = os.path.join(tmpdir, HANDLER_FILE_NAME)
            with open(handle_path, "w") as f:
                source = getsource(function_handle)
                f.write(source)
            if docstr_requirements:
                requirements_path = os.path.join(tmpdir, REQUIREMENTS_FILE_NAME)
                with open(requirements_path, "w") as f:
                    for req in docstr_requirements:
                        f.write(
                            f"""{req}
"""
                        )
            zip_path = os.path.join(tmpdir, "function.zip")
            with ZipFile(zip_path, "w") as zf:
                zf.write(handle_path, arcname=HANDLER_FILE_NAME)
                if docstr_requirements:
                    zf.write(requirements_path, arcname=REQUIREMENTS_FILE_NAME)
            overwrite = True if external_id else False
            file = cast(
                FileMetadata,
                self._cognite_client.files.upload(
                    zip_path, name=f"{name}.zip", external_id=external_id, overwrite=overwrite
                ),
            )
            file_id = cast(int, file.id)
        return file_id

    @staticmethod
    def _assert_exactly_one_of_folder_or_file_id_or_function_handle(folder, file_id, function_handle):
        source_code_options = {"folder": folder, "file_id": file_id, "function_handle": function_handle}
        given_source_code_options = [key for key in source_code_options.keys() if source_code_options[key]]
        if len(given_source_code_options) < 1:
            raise TypeError("Exactly one of the arguments folder, file_id and handle is required, but none were given.")
        elif len(given_source_code_options) > 1:
            raise TypeError(
                (
                    "Exactly one of the arguments folder, file_id and handle is required, but "
                    + ", ".join(given_source_code_options)
                )
                + " were given."
            )

    def activate(self):
        res = self._post("/functions/status")
        return FunctionsStatus._load(res.json())

    def status(self):
        res = self._get("/functions/status")
        return FunctionsStatus._load(res.json())


def _use_client_credentials(cognite_client, client_credentials=None):
    if client_credentials:
        client_id = client_credentials["client_id"]
        client_secret = client_credentials["client_secret"]
    else:
        assert isinstance(cognite_client.config.credentials, OAuthClientCredentials)
        client_id = cognite_client.config.credentials.client_id
        client_secret = cognite_client.config.credentials.client_secret
    session_url = f"/api/v1/projects/{cognite_client.config.project}/sessions"
    payload = {"items": [{"clientId": client_id, "clientSecret": client_secret}]}
    try:
        res = cognite_client.post(session_url, json=payload)
        nonce = res.json()["items"][0]["nonce"]
        return nonce
    except CogniteAPIError as e:
        raise CogniteAPIError("Failed to create session using client credentials flow.", 403) from e


def _use_token_exchange(cognite_client):
    session_url = f"/api/v1/projects/{cognite_client.config.project}/sessions"
    payload = {"items": [{"tokenExchange": True}]}
    try:
        res = cognite_client.post(url=session_url, json=payload)
        nonce = res.json()["items"][0]["nonce"]
        return nonce
    except CogniteAPIError as e:
        raise CogniteAPIError("Failed to create session using token exchange flow.", 403) from e


def _using_token_exchange_flow(cognite_client):
    return isinstance(cognite_client.config.credentials, Token)


def _using_client_credential_flow(cognite_client):
    return isinstance(cognite_client.config.credentials, OAuthClientCredentials)


def convert_file_path_to_module_path(file_path):
    return ".".join(Path(file_path).with_suffix("").parts)


def validate_function_folder(root_path, function_path):
    file_extension = Path(function_path).suffix
    if file_extension != ".py":
        raise TypeError(f"{function_path} is not a valid value for function_path. File extension must be .py.")
    function_path_full = Path(root_path) / Path(function_path)
    if not function_path_full.is_file():
        raise FileNotFoundError(f"No file found at location '{function_path}' in '{root_path}'.")
    sys.path.insert(0, root_path)
    cached_handler_module = sys.modules.get("handler")
    if cached_handler_module:
        del sys.modules["handler"]
    module_path = convert_file_path_to_module_path(function_path)
    handler = importlib.import_module(module_path)
    if "handle" not in dir(handler):
        raise TypeError(f"{function_path} must contain a function named 'handle'.")
    _validate_function_handle(handler.handle)
    sys.path.remove(root_path)


def _validate_function_handle(function_handle):
    if not (function_handle.__code__.co_name == "handle"):
        raise TypeError("Function referenced by function_handle must be named handle.")
    if not set(function_handle.__code__.co_varnames[: function_handle.__code__.co_argcount]).issubset(
        {"data", "client", "secrets", "function_call_info"}
    ):
        raise TypeError(
            "Arguments to function referenced by function_handle must be a subset of (data, client, secrets, function_call_info)"
        )


def _extract_requirements_from_file(file_name):
    requirements: List[str] = []
    with open(file_name, "r+") as f:
        for line in f:
            line = line.strip()
            if UNCOMMENTED_LINE_REG.match(line):
                requirements.append(line)
    return requirements


def _extract_requirements_from_doc_string(docstr):
    (substr_start, substr_end) = (None, None)
    for match in REQUIREMENTS_REG.finditer(docstr):
        val = match.group()
        if val == "[requirements]":
            substr_start = match.end()
        elif val == "[/requirements]":
            substr_end = match.start()
    if substr_start and substr_end:
        return docstr[substr_start:substr_end].splitlines()[1:]
    return None


def _validate_and_parse_requirements(requirements):
    constructors = cast(Any, utils._auxiliary.local_import("pip._internal.req.constructors"))
    install_req_from_line = constructors.install_req_from_line
    parsed_reqs: List[str] = []
    for req in requirements:
        try:
            parsed = install_req_from_line(req)
        except Exception as e:
            raise ValueError(str(e))
        parsed_reqs.append(str(parsed).strip())
    return parsed_reqs


def _get_fn_docstring_requirements(fn):
    docstr = getdoc(fn)
    if docstr:
        reqs = _extract_requirements_from_doc_string(docstr)
        if reqs:
            parsed_reqs = _validate_and_parse_requirements(reqs)
            return parsed_reqs
    return []


class FunctionCallsAPI(APIClient):
    _LIST_CLASS = FunctionCallList

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cognite_client: CogniteClient = cast("CogniteClient", self._cognite_client)

    def list(
        self,
        function_id=None,
        function_external_id=None,
        status=None,
        schedule_id=None,
        start_time=None,
        end_time=None,
        limit=LIST_LIMIT_DEFAULT,
    ):
        identifier = _get_function_identifier(function_id, function_external_id)
        function_id = _get_function_internal_id(self._cognite_client, identifier)
        filter = FunctionCallsFilter(
            status=status, schedule_id=schedule_id, start_time=start_time, end_time=end_time
        ).dump(camel_case=True)
        resource_path = f"/functions/{function_id}/calls"
        return self._list(
            method="POST",
            resource_path=resource_path,
            filter=filter,
            limit=limit,
            resource_cls=FunctionCall,
            list_cls=FunctionCallList,
        )

    def retrieve(self, call_id, function_id=None, function_external_id=None):
        identifier = _get_function_identifier(function_id, function_external_id)
        function_id = _get_function_internal_id(self._cognite_client, identifier)
        resource_path = f"/functions/{function_id}/calls"
        identifiers = IdentifierSequence.load(ids=call_id).as_singleton()
        return self._retrieve_multiple(
            resource_path=resource_path, identifiers=identifiers, resource_cls=FunctionCall, list_cls=FunctionCallList
        )

    def get_response(self, call_id, function_id=None, function_external_id=None):
        identifier = _get_function_identifier(function_id, function_external_id)
        function_id = _get_function_internal_id(self._cognite_client, identifier)
        url = f"/functions/{function_id}/calls/{call_id}/response"
        res = self._get(url)
        return res.json().get("response")

    def get_logs(self, call_id, function_id=None, function_external_id=None):
        identifier = _get_function_identifier(function_id, function_external_id)
        function_id = _get_function_internal_id(self._cognite_client, identifier)
        url = f"/functions/{function_id}/calls/{call_id}/logs"
        res = self._get(url)
        return FunctionCallLog._load(res.json()["items"])


class FunctionSchedulesAPI(APIClient):
    _RESOURCE_PATH = "/functions/schedules"
    _LIST_CLASS = FunctionSchedulesList

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cognite_client: CogniteClient = cast("CogniteClient", self._cognite_client)

    def retrieve(self, id):
        return self._retrieve_multiple(
            identifiers=IdentifierSequence.load(ids=id), resource_cls=FunctionSchedule, list_cls=FunctionSchedulesList
        )

    def list(
        self,
        name=None,
        function_id=None,
        function_external_id=None,
        created_time=None,
        cron_expression=None,
        limit=LIST_LIMIT_DEFAULT,
    ):
        if function_id or function_external_id:
            try:
                IdentifierSequence.load(ids=function_id, external_ids=function_external_id).assert_singleton()
            except ValueError:
                raise AssertionError("Only function_id or function_external_id allowed when listing schedules.")
        if is_unlimited(limit):
            limit = LIST_LIMIT_CEILING
        filter = FunctionSchedulesFilter(
            name=name,
            function_id=function_id,
            function_external_id=function_external_id,
            created_time=created_time,
            cron_expression=cron_expression,
        ).dump(camel_case=True)
        res = self._post(url_path=f"{self._RESOURCE_PATH}/list", json={"filter": filter, "limit": limit})
        return self._LIST_CLASS._load(res.json()["items"], cognite_client=self._cognite_client)

    def create(
        self,
        name,
        cron_expression,
        function_id=None,
        function_external_id=None,
        client_credentials=None,
        description="",
        data=None,
    ):
        _get_function_identifier(function_id, function_external_id)
        nonce = None
        if client_credentials:
            assert function_id is not None, "function_id must be set when creating a schedule with client_credentials."
            nonce = _use_client_credentials(self._cognite_client, client_credentials)
        body: Dict[(str, List[Dict[(str, Union[(str, int, None, Dict)])]])] = {
            "items": [
                {
                    "name": name,
                    "description": description,
                    "functionId": function_id,
                    "functionExternalId": function_external_id,
                    "cronExpression": cron_expression,
                    "nonce": nonce,
                }
            ]
        }
        if data:
            body["items"][0]["data"] = data
        url = "/functions/schedules"
        res = self._post(url, json=body)
        return FunctionSchedule._load(res.json()["items"][0])

    def delete(self, id):
        body = {"items": [{"id": id}]}
        url = "/functions/schedules/delete"
        self._post(url, json=body)

    def get_input_data(self, id):
        url = f"/functions/schedules/{id}/input_data"
        res = self._get(url)
        return res.json()["data"]
