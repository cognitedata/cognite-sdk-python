from __future__ import annotations

import importlib.util
import os
import re
import sys
import time
from inspect import getdoc, getsource
from numbers import Number
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Sequence, Union, cast
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
    TimestampRange,
)
from cognite.client.data_classes.files import FileMetadata
from cognite.client.data_classes.functions import FunctionCallsFilter, FunctionsStatus
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._auxiliary import is_unlimited
from cognite.client.utils._identifier import IdentifierSequence, SingletonIdentifierSequence

if TYPE_CHECKING:
    from cognite.client import CogniteClient

HANDLER_FILE_NAME = "handler.py"
MAX_RETRIES = 5
REQUIREMENTS_FILE_NAME = "requirements.txt"
REQUIREMENTS_REG = re.compile(r"(\[\/?requirements\]){1}$", flags=re.M)  # Matches [requirements] and [/requirements]
UNCOMMENTED_LINE_REG = re.compile(r"^[^\#]]*.*")


def _get_function_internal_id(_cognite_client: CogniteClient, identifier: SingletonIdentifierSequence) -> int:
    id_object = identifier[0]
    id_dict = id_object.as_dict()

    if "id" in id_dict:
        return id_object.as_primitive()

    if "externalId" in id_dict:
        function = _cognite_client.functions.retrieve(external_id=id_object.as_primitive())
        if function:
            return function.id

    raise ValueError(f'Function with external ID "{id_object.as_primitive()}" is not found')


def _get_function_identifier(id: Optional[int], external_id: Optional[str]) -> SingletonIdentifierSequence:
    identifier = IdentifierSequence.load(ids=id, external_ids=external_id)
    if identifier.is_singleton():
        return identifier.as_singleton()
    raise AssertionError("Exactly one of function_id and function_external_id must be specified")


class FunctionsAPI(APIClient):
    _RESOURCE_PATH = "/functions"
    _LIST_CLASS = FunctionList

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.calls = FunctionCallsAPI(*args, **kwargs)
        self.schedules = FunctionSchedulesAPI(*args, **kwargs)
        self._cognite_client: CogniteClient = cast("CogniteClient", self._cognite_client)

    def create(
        self,
        name: str,
        folder: Optional[str] = None,
        file_id: Optional[int] = None,
        function_path: str = HANDLER_FILE_NAME,
        function_handle: Optional[Callable] = None,
        external_id: Optional[str] = None,
        description: Optional[str] = "",
        owner: Optional[str] = "",
        api_key: Optional[str] = None,
        secrets: Optional[Dict] = None,
        env_vars: Optional[Dict] = None,
        cpu: Optional[Number] = None,
        memory: Optional[Number] = None,
        runtime: Optional[str] = None,
        metadata: Optional[Dict] = None,
        index_url: Optional[str] = None,
        extra_index_urls: Optional[List[str]] = None,
    ) -> Function:
        """`When creating a function, <https://docs.cognite.com/api/v1/#operation/postFunctions>`_
        the source code can be specified in one of three ways:\n
        - Via the `folder` argument, which is the path to the folder where the source code is located. `function_path` must point to a python file in the folder within which a function named `handle` must be defined.\n
        - Via the `file_id` argument, which is the ID of a zip-file uploaded to the files API. `function_path` must point to a python file in the zipped folder within which a function named `handle` must be defined.\n
        - Via the `function_handle` argument, which is a reference to a function object, which must be named `handle`.\n

        The function named `handle` is the entrypoint of the created function. Valid arguments to `handle` are `data`, `client`, `secrets` and `function_call_info`:\n
        - If the user calls the function with input data, this is passed through the `data` argument.\n
        - If the user gives an `api_key` when creating the function, a pre instantiated CogniteClient is passed through the `client` argument.\n
        - If the user gives one or more secrets when creating the function, these are passed through the `secrets` argument. The API key can be access through `secrets["apikey"]`.\n
        - Data about the function call can be accessed via the argument `function_call_info`, which is a dictionary with keys `function_id` and, if the call is scheduled, `schedule_id` and `scheduled_time`.\n

        Args:
            name (str):                              The name of the function.
            folder (str, optional):                  Path to the folder where the function source code is located.
            file_id (int, optional):                 File ID of the code uploaded to the Files API.
            function_path (str):                     Relative path from the root folder to the file containing the `handle` function. Defaults to `handler.py`. Must be on POSIX path format.
            function_handle (Callable, optional):    Reference to a function object, which must be named `handle`.
            external_id (str, optional):             External id of the function.
            description (str, optional):             Description of the function.
            owner (str, optional):                   Owner of this function. Typically used to know who created it.
            api_key (str, optional):                 API key that can be used inside the function to access data in CDF.
            secrets (Dict[str, str]):                Additional secrets as key/value pairs. These can e.g. password to simulators or other data sources. Keys must be lowercase characters, numbers or dashes (-) and at most 15 characters. You can create at most 30 secrets, all keys must be unique, and cannot be apikey.
            env_vars (Dict[str, str]):               Environment variables as key/value pairs. Keys can contain only letters, numbers or the underscore character. You can create at most 100 environment variables.
            cpu (Number, optional):                  Number of CPU cores per function. Allowed values are in the range [0.1, 0.6], and None translates to the API default which is 0.25 in GCP. The argument is unavailable in Azure.
            memory (Number, optional):               Memory per function measured in GB. Allowed values are in the range [0.1, 2.5], and None translates to the API default which is 1 GB in GCP. The argument is unavailable in Azure.
            runtime (str, optional):                 The function runtime. Valid values are ["py37", "py38", "py39", `None`], and `None` translates to the API default which currently is "py38". The runtime "py38" resolves to the latest version of the Python 3.8 series.
            metadata (Dict[str, str], optional):     Metadata for the function as key/value pairs. Key & values can be at most 32, 512 characters long respectively. You can have at the most 16 key-value pairs, with a maximum size of 512 bytes.
            index_url (str, optional):               Index URL for Python Package Manager to use. Be aware of the intrinsic security implications of using the `index_url` option. `More information can be found on official docs, <https://docs.cognite.com/cdf/functions/#additional-arguments>`_
            extra_index_urls (List[str], optional):  Extra Index URLs for Python Package Manager to use. Be aware of the intrinsic security implications of using the `extra_index_urls` option. `More information can be found on official docs, <https://docs.cognite.com/cdf/functions/#additional-arguments>`_
        Returns:
            Function: The created function.

        Examples:

            Create function with source code in folder::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> function = c.functions.create(name="myfunction", folder="path/to/code", function_path="path/to/function.py")

            Create function with file_id from already uploaded source code::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> function = c.functions.create(name="myfunction", file_id=123, function_path="path/to/function.py")

            Create function with predefined function object named `handle`::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> function = c.functions.create(name="myfunction", function_handle=handle)

            Create function with predefined function object named `handle` with dependencies::

                >>> from cognite.client import CogniteClient
                >>>
                >>> def handle(client, data):
                >>>     \"\"\"
                >>>     [requirements]
                >>>     numpy
                >>>     [/requirements]
                >>>     \"\"\"
                >>>     ...
                >>>
                >>> c = CogniteClient()
                >>> function = c.functions.create(name="myfunction", function_handle=handle)

            .. note::
                When using a predefined function object, you can list dependencies between the tags `[requirements]` and `[/requirements]` in the function's docstring. The dependencies will be parsed and validated in accordance with requirement format specified in `PEP 508 <https://peps.python.org/pep-0508/>`_.
        """
        self._assert_exactly_one_of_folder_or_file_id_or_function_handle(folder, file_id, function_handle)

        if folder:
            validate_function_folder(folder, function_path)
            file_id = self._zip_and_upload_folder(folder, name, external_id)
        elif function_handle:
            _validate_function_handle(function_handle)
            file_id = self._zip_and_upload_handle(function_handle, name, external_id)
        utils._auxiliary.assert_type(cpu, "cpu", [Number], allow_none=True)
        utils._auxiliary.assert_type(memory, "memory", [Number], allow_none=True)

        sleep_time = 1.0  # seconds
        for i in range(MAX_RETRIES):
            file = self._cognite_client.files.retrieve(id=file_id)
            if file is None or not file.uploaded:
                time.sleep(sleep_time)
                sleep_time *= 2
            else:
                break
        else:
            raise OSError("Could not retrieve file from files API")

        url = "/functions"
        function: Dict[str, Any] = {
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

    def delete(self, id: Union[int, Sequence[int]] = None, external_id: Union[str, Sequence[str]] = None) -> None:
        """`Delete one or more functions. <https://docs.cognite.com/api/v1/#operation/deleteFunctions>`_

        Args:
            id (Union[int, Sequence[int]): Id or list of ids.
            external_id (Union[str, Sequence[str]]): External ID or list of external ids.

        Returns:
            None

        Example:

            Delete functions by id or external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.functions.delete(id=[1,2,3], external_id="function3")
        """
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id, external_ids=external_id), wrap_ids=True)

    def list(
        self,
        name: str = None,
        owner: str = None,
        file_id: int = None,
        status: str = None,
        external_id_prefix: str = None,
        created_time: Union[Dict[str, int], TimestampRange] = None,
        limit: Optional[int] = LIST_LIMIT_DEFAULT,
    ) -> FunctionList:
        """`List all functions. <https://docs.cognite.com/api/v1/#operation/listFunctions>`_

        Args:
            name (str): The name of the function.
            owner (str): Owner of the function.
            file_id (int): The file ID of the zip-file used to create the function.
            status (str): Status of the function. Possible values: ["Queued", "Deploying", "Ready", "Failed"].
            external_id_prefix (str): External ID prefix to filter on.
            created_time (Union[Dict[str, int], TimestampRange]):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            limit (int): Maximum number of functions to return. Pass in -1, float('inf') or None to list all.

        Returns:
            FunctionList: List of functions

        Example:

            List functions::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> functions_list = c.functions.list()
        """
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

    def retrieve(
        self, id: Optional[int] = None, external_id: Optional[str] = None
    ) -> Union[FunctionList, Function, None]:
        """`Retrieve a single function by id. <https://docs.cognite.com/api/v1/#operation/byIdsFunctions>`_

        Args:
            id (int, optional): ID
            external_id (str, optional): External ID

        Returns:
            Optional[Function]: Requested function or None if it does not exist.

        Examples:

            Get function by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.functions.retrieve(id=1)

            Get function by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.functions.retrieve(external_id="1")
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(identifiers=identifiers, resource_cls=Function, list_cls=FunctionList)

    def retrieve_multiple(
        self, ids: Optional[Sequence[int]] = None, external_ids: Optional[Sequence[str]] = None
    ) -> Union[FunctionList, Function, None]:
        """`Retrieve multiple functions by id. <https://docs.cognite.com/api/v1/#operation/byIdsFunctions>`_

        Args:
            ids (Sequence[int], optional): IDs
            external_ids (Sequence[str], optional): External IDs

        Returns:
            FunctionList: The requested functions.

        Examples:

            Get function by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.functions.retrieve_multiple(ids=[1, 2, 3])

            Get functions by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.functions.retrieve_multiple(external_ids=["func1", "func2"])
        """
        utils._auxiliary.assert_type(ids, "id", [Sequence], allow_none=True)
        utils._auxiliary.assert_type(external_ids, "external_id", [Sequence], allow_none=True)
        return self._retrieve_multiple(
            identifiers=IdentifierSequence.load(ids=ids, external_ids=external_ids),
            resource_cls=Function,
            list_cls=FunctionList,
        )

    def call(
        self,
        id: Optional[int] = None,
        external_id: Optional[str] = None,
        data: Optional[Dict] = None,
        wait: bool = True,
    ) -> FunctionCall:
        """`Call a function by its ID or external ID. <https://docs.cognite.com/api/v1/#operation/postFunctionsCall>`_.

        Args:
            id (int, optional): ID
            external_id (str, optional): External ID
            data (Union[str, dict], optional): Input data to the function (JSON serializable). This data is passed deserialized into the function through one of the arguments called data. **WARNING:** Secrets or other confidential information should not be passed via this argument. There is a dedicated `secrets` argument in FunctionsAPI.create() for this purpose.'
            wait (bool): Wait until the function call is finished. Defaults to True.

        Returns:
            FunctionCall: A function call object.

        Examples:

            Call a function by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> call = c.functions.call(id=1)

            Call a function directly on the `Function` object::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> func = c.functions.retrieve(id=1)
                >>> call = func.call()
        """
        identifier = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        id = _get_function_internal_id(self._cognite_client, identifier)

        # Case 1: Client credentials inferred from the instantiated client.
        # Case 2: Token on behalf of the user. We use token exchange.
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

    def limits(self) -> FunctionsLimits:
        """`Get service limits. <https://docs.cognite.com/api/v1/#operation/functionsLimits>`_.

        Returns:
            FunctionsLimits: A function limits object.

        Examples:

            Call a function by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> limits = c.functions.limits()
        """
        res = self._get("/functions/limits")
        return FunctionsLimits._load(res.json())

    def _zip_and_upload_folder(self, folder: str, name: str, external_id: Optional[str] = None) -> int:
        # / is not allowed in file names
        name = name.replace("/", "-")

        current_dir = os.getcwd()
        os.chdir(folder)

        try:
            with TemporaryDirectory() as tmpdir:
                zip_path = os.path.join(tmpdir, "function.zip")
                with ZipFile(zip_path, "w") as zf:
                    for root, dirs, files in os.walk("."):
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

    def _zip_and_upload_handle(self, function_handle: Callable, name: str, external_id: Optional[str] = None) -> int:
        # / is not allowed in file names
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
                        f.write(f"{req}\n")

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
    def _assert_exactly_one_of_folder_or_file_id_or_function_handle(
        folder: Optional[str], file_id: Optional[int], function_handle: Optional[Callable[..., Any]]
    ) -> None:
        source_code_options = {"folder": folder, "file_id": file_id, "function_handle": function_handle}
        given_source_code_options = [key for key in source_code_options.keys() if source_code_options[key]]
        if len(given_source_code_options) < 1:
            raise TypeError("Exactly one of the arguments folder, file_id and handle is required, but none were given.")
        elif len(given_source_code_options) > 1:
            raise TypeError(
                "Exactly one of the arguments folder, file_id and handle is required, but "
                + ", ".join(given_source_code_options)
                + " were given."
            )

    def activate(self) -> FunctionsStatus:
        """`Activate functions for the Project. <https://docs.cognite.com/api/v1/#tag/Functions/operation/postFunctionsStatus>`_.

        Returns:
            FunctionsStatus: A function activation status.

        Examples:

            Call activate::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> status = c.functions.activate()
        """
        res = self._post("/functions/status")
        return FunctionsStatus._load(res.json())

    def status(self) -> FunctionsStatus:
        """`Functions activation status for the Project. <https://docs.cognite.com/api/v1/#tag/Functions/operation/getFunctionsStatus>`_.

        Returns:
            FunctionsStatus: A function activation status.

        Examples:

            Call status::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> status = c.functions.status()
        """
        res = self._get("/functions/status")
        return FunctionsStatus._load(res.json())


def _use_client_credentials(
    cognite_client: CogniteClient,
    client_credentials: Optional[Dict] = None,
) -> str:
    """
    If client_credentials is passed, will use those, otherwise will implicitly use those the client was instantiated
    with
    Args:
        client_credentials: a dictionary containing:
            client_id
            client_secret

    Returns:
        nonce (optional, str): a nonce if able to obtain, otherwise raises CogniteAPIError.

    """

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


def _use_token_exchange(
    cognite_client: CogniteClient,
) -> str:
    session_url = f"/api/v1/projects/{cognite_client.config.project}/sessions"
    payload = {"items": [{"tokenExchange": True}]}
    try:
        res = cognite_client.post(url=session_url, json=payload)
        nonce = res.json()["items"][0]["nonce"]
        return nonce
    except CogniteAPIError as e:
        raise CogniteAPIError("Failed to create session using token exchange flow.", 403) from e


def _using_token_exchange_flow(cognite_client: CogniteClient) -> bool:
    """Determine whether the Cognite client is configured with a token or token factory."""
    return isinstance(cognite_client.config.credentials, Token)


def _using_client_credential_flow(cognite_client: CogniteClient) -> bool:
    """Determine whether the Cognite client is configured for client-credential flow."""
    return isinstance(cognite_client.config.credentials, OAuthClientCredentials)


def convert_file_path_to_module_path(file_path: str) -> str:
    return ".".join(Path(file_path).with_suffix("").parts)


def validate_function_folder(root_path: str, function_path: str) -> None:
    file_extension = Path(function_path).suffix
    if file_extension != ".py":
        raise TypeError(f"{function_path} is not a valid value for function_path. File extension must be .py.")

    function_path_full = Path(root_path) / Path(
        function_path
    )  # This converts function_path to a Windows path if running on Windows
    if not function_path_full.is_file():
        raise FileNotFoundError(f"No file found at location '{function_path}' in '{root_path}'.")

    sys.path.insert(0, root_path)

    # Necessary to clear the cache if you have previously imported the module (this would have precedence over sys.path)
    cached_handler_module = sys.modules.get("handler")
    if cached_handler_module:
        del sys.modules["handler"]

    module_path = convert_file_path_to_module_path(function_path)
    handler = importlib.import_module(module_path)

    if "handle" not in dir(handler):
        raise TypeError(f"{function_path} must contain a function named 'handle'.")

    _validate_function_handle(handler.handle)
    sys.path.remove(root_path)


def _validate_function_handle(function_handle: Callable[..., Any]) -> None:
    if not function_handle.__code__.co_name == "handle":
        raise TypeError("Function referenced by function_handle must be named handle.")
    if not set(function_handle.__code__.co_varnames[: function_handle.__code__.co_argcount]).issubset(
        {"data", "client", "secrets", "function_call_info"}
    ):
        raise TypeError(
            "Arguments to function referenced by function_handle must be a subset of (data, client, secrets, function_call_info)"
        )


def _extract_requirements_from_file(file_name: str) -> List[str]:
    """Extracts a list of library requirements from a file. Comments, lines starting with '#', are ignored.

    Args:
        file_name (str): name of the file to parse

    Returns:
        (list[str]): returns a list of library records
    """
    requirements: List[str] = []
    with open(file_name, "r+") as f:
        for line in f:
            line = line.strip()
            if UNCOMMENTED_LINE_REG.match(line):
                requirements.append(line)
    return requirements


def _extract_requirements_from_doc_string(docstr: str) -> Optional[List[str]]:
    """Extracts a list of library requirements defined between [requirements] and [/requirements] in a functions docstring.

    Args:
        docstr (str): the docstring to extract requirements from

    Returns:
        (list[str] | None): returns a list of library records if requirements are defined in the docstring, else None
    """
    substr_start, substr_end = None, None

    # Get index values for the start and end of the requirements list
    for match in REQUIREMENTS_REG.finditer(docstr):
        val = match.group()
        if val == "[requirements]":
            substr_start = match.end()
        elif val == "[/requirements]":
            substr_end = match.start()

    if substr_start and substr_end:
        # Return a list of requirement entries
        return docstr[substr_start:substr_end].splitlines()[1:]
    return None


def _validate_and_parse_requirements(requirements: List[str]) -> List[str]:
    """Validates the requirement specifications

    Args:
        requirements (list[str]): list of requirement specifications
    Raises:
        ValueError: if validation of requirements fails
    Returns:
        List[str]: The parsed requirements
    """
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


def _get_fn_docstring_requirements(fn: Callable) -> List[str]:
    """Read requirements from a function docstring, validate them and return.

    Args:
        fn (Callable): the function to read requirements from
        file_path (str): Path of file to write requirements to

    Returns:
        List[str]: A (possibly empty) list of requirements.
    """
    docstr = getdoc(fn)

    if docstr:
        reqs = _extract_requirements_from_doc_string(docstr)
        if reqs:
            parsed_reqs = _validate_and_parse_requirements(reqs)
            return parsed_reqs

    return []


class FunctionCallsAPI(APIClient):
    _LIST_CLASS = FunctionCallList

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._cognite_client: CogniteClient = cast("CogniteClient", self._cognite_client)

    def list(
        self,
        function_id: Optional[int] = None,
        function_external_id: Optional[str] = None,
        status: Optional[str] = None,
        schedule_id: Optional[int] = None,
        start_time: Optional[Dict[str, int]] = None,
        end_time: Optional[Dict[str, int]] = None,
        limit: Optional[int] = LIST_LIMIT_DEFAULT,
    ) -> FunctionCallList:
        """`List all calls associated with a specific function id. <https://docs.cognite.com/api/v1/#operation/listFunctionCalls>`_ Either function_id or function_external_id must be specified.

        Args:
            function_id (int, optional): ID of the function on which the calls were made.
            function_external_id (str, optional): External ID of the function on which the calls were made.
            status (str, optional): Status of the call. Possible values ["Running", "Failed", "Completed", "Timeout"].
            schedule_id (int, optional): Schedule id from which the call belongs (if any).
            start_time (Dict[str, int], optional): Start time of the call. Possible keys are `min` and `max`, with values given as time stamps in ms.
            end_time (Dict[str, int], optional): End time of the call. Possible keys are `min` and `max`, with values given as time stamps in ms.
            limit (int, optional): Maximum number of function calls to list. Pass in -1, float('inf') or None to list all Function Calls.

        Returns:
            FunctionCallList: List of function calls

        Examples:

            List function calls::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> calls = c.functions.calls.list(function_id=1)

            List function calls directly on a function object::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> func = c.functions.retrieve(id=1)
                >>> calls = func.list_calls()

        """
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

    def retrieve(
        self, call_id: int, function_id: Optional[int] = None, function_external_id: Optional[str] = None
    ) -> Union[FunctionCallList, FunctionCall, None]:
        """`Retrieve a single function call by id. <https://docs.cognite.com/api/v1/#operation/byIdsFunctionCalls>`_

        Args:
            call_id (int): ID of the call.
            function_id (int, optional): ID of the function on which the call was made.
            function_external_id (str, optional): External ID of the function on which the call was made.

        Returns:
            Union[FunctionCallList, FunctionCall, None]: Requested function call.

        Examples:

            Retrieve single function call by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> call = c.functions.calls.retrieve(call_id=2, function_id=1)

            Retrieve function call directly on a function object::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> func = c.functions.retrieve(id=1)
                >>> call = func.retrieve_call(id=2)

        """
        identifier = _get_function_identifier(function_id, function_external_id)
        function_id = _get_function_internal_id(self._cognite_client, identifier)

        resource_path = f"/functions/{function_id}/calls"
        identifiers = IdentifierSequence.load(ids=call_id).as_singleton()

        return self._retrieve_multiple(
            resource_path=resource_path,
            identifiers=identifiers,
            resource_cls=FunctionCall,
            list_cls=FunctionCallList,
        )

    def get_response(
        self, call_id: int, function_id: Optional[int] = None, function_external_id: Optional[str] = None
    ) -> Dict:
        """`Retrieve the response from a function call. <https://docs.cognite.com/api/v1/#operation/getFunctionCallResponse>`_

        Args:
            call_id (int): ID of the call.
            function_id (int, optional): ID of the function on which the call was made.
            function_external_id (str, optional): External ID of the function on which the call was made.

        Returns:
            Response from the function call.

        Examples:

            Retrieve function call response by call ID::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> response = c.functions.calls.get_response(call_id=2, function_id=1)

            Retrieve function call response directly on a call object::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> call = c.functions.calls.retrieve(call_id=2, function_id=1)
                >>> response = call.get_response()

        """
        identifier = _get_function_identifier(function_id, function_external_id)
        function_id = _get_function_internal_id(self._cognite_client, identifier)
        url = f"/functions/{function_id}/calls/{call_id}/response"
        res = self._get(url)
        return res.json().get("response")

    def get_logs(
        self, call_id: int, function_id: Optional[int] = None, function_external_id: Optional[str] = None
    ) -> FunctionCallLog:
        """`Retrieve logs for function call. <https://docs.cognite.com/api/v1/#operation/getFunctionCalls>`_

        Args:
            call_id (int): ID of the call.
            function_id (int, optional): ID of the function on which the call was made.
            function_external_id (str, optional): External ID of the function on which the call was made.

        Returns:
            FunctionCallLog: Log for the function call.

        Examples:

            Retrieve function call logs by call ID::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> logs = c.functions.calls.get_logs(call_id=2, function_id=1)

            Retrieve function call logs directly on a call object::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> call = c.functions.calls.retrieve(call_id=2, function_id=1)
                >>> logs = call.get_logs()

        """
        identifier = _get_function_identifier(function_id, function_external_id)
        function_id = _get_function_internal_id(self._cognite_client, identifier)

        url = f"/functions/{function_id}/calls/{call_id}/logs"
        res = self._get(url)
        return FunctionCallLog._load(res.json()["items"])


class FunctionSchedulesAPI(APIClient):
    _RESOURCE_PATH = "/functions/schedules"
    _LIST_CLASS = FunctionSchedulesList

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._cognite_client: CogniteClient = cast("CogniteClient", self._cognite_client)

    def retrieve(self, id: int) -> Union[FunctionSchedule, FunctionSchedulesList, None]:
        """`Retrieve a single function schedule by id. <https://docs.cognite.com/api/v1/#operation/byIdsFunctionSchedules>`_

        Args:
            id (int): ID

        Returns:
            Optional[FunctionSchedule]: Requested function schedule.


        Examples:

            Get function schedule by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.functions.schedules.retrieve(id=1)

        """
        return self._retrieve_multiple(
            identifiers=IdentifierSequence.load(ids=id), resource_cls=FunctionSchedule, list_cls=FunctionSchedulesList
        )

    def list(
        self,
        name: str = None,
        function_id: int = None,
        function_external_id: str = None,
        created_time: Union[Dict[str, int], TimestampRange] = None,
        cron_expression: str = None,
        limit: Optional[int] = LIST_LIMIT_DEFAULT,
    ) -> FunctionSchedulesList:
        """`List all schedules associated with a specific project. <https://docs.cognite.com/api/v1/#operation/listFunctionSchedules>`_

        Args:
            name (str): Name of the function schedule.
            function_id (int): ID of the function the schedules are linked to.
            function_external_id (str): External ID of the function the schedules are linked to.
            created_time (Union[Dict[str, int], TimestampRange]):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            cron_expression (str): Cron expression.
            limit (int): Maximum number of schedules to list. Pass in -1, float('inf') or None to list all.

        Returns:
            FunctionSchedulesList: List of function schedules

        Examples:

            List function schedules::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> schedules = c.functions.schedules.list()

            List schedules directly on a function object to get only schedules associated with this particular function:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> func = c.functions.retrieve(id=1)
                >>> schedules = func.list_schedules(limit=None)

        """
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
        name: str,
        cron_expression: str,
        function_id: Optional[int] = None,
        function_external_id: Optional[str] = None,
        client_credentials: Optional[Dict] = None,
        description: str = "",
        data: Optional[Dict] = None,
    ) -> FunctionSchedule:
        """`Create a schedule associated with a specific project. <https://docs.cognite.com/api/v1/#operation/postFunctionSchedules>`_

        Args:
            name (str): Name of the schedule.
            function_id (optional, int): Id of the function. This is required if the schedule is created with client_credentials.
            function_external_id (optional, str): External id of the function. This is deprecated and cannot be used together with client_credentials.
            description (str): Description of the schedule.
            cron_expression (str): Cron expression.
            client_credentials: (optional, Dict): Dictionary containing client credentials:
                client_id
                client_secret
            data (optional, Dict): Data to be passed to the scheduled run. **WARNING:** Secrets or other confidential information should not be passed via this argument. There is a dedicated `secrets` argument in FunctionsAPI.create() for this purpose.

        Returns:
            FunctionSchedule: Created function schedule.

        Examples:

            Create function schedule::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> schedule = c.functions.schedules.create(
                ...     name= "My schedule",
                ...     function_id=123,
                ...     cron_expression="*/5 * * * *",
                ...     client_credentials={"client_id": "...", "client_secret": "..."},
                ...     description="This schedule does magic stuff."
                ... )

        """
        _get_function_identifier(function_id, function_external_id)

        nonce = None
        if client_credentials:
            assert function_id is not None, "function_id must be set when creating a schedule with client_credentials."
            nonce = _use_client_credentials(self._cognite_client, client_credentials)

        body: Dict[str, List[Dict[str, Union[str, int, None, Dict]]]] = {
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

    def delete(self, id: int) -> None:
        """`Delete a schedule associated with a specific project. <https://docs.cognite.com/api/v1/#operation/deleteFunctionSchedules>`_

        Args:
            id (int): Id of the schedule

        Returns:
            None

        Examples:

            Delete function schedule::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.functions.schedules.delete(id = 123)

        """
        body = {"items": [{"id": id}]}
        url = "/functions/schedules/delete"
        self._post(url, json=body)

    def get_input_data(self, id: int) -> Dict:
        """`Retrieve the input data to the associated function. <https://docs.cognite.com/api/v1/#operation/getFunctionScheduleInputData>`_
        Args:
            id (int): Id of the schedule

        Returns:
            Input data to the associated function. This data is passed
            deserialized into the function through the data argument.
        Examples:

            Get schedule input data::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.functions.schedules.get_input_data(id = 123)
        """
        url = f"/functions/schedules/{id}/input_data"
        res = self._get(url)

        return res.json()["data"]
