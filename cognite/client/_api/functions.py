from __future__ import annotations

import ast
import importlib
import os
import re
import sys
import textwrap
import time
from inspect import getdoc, getsource, signature
from multiprocessing import Process, Queue
from numbers import Number
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, Callable, Sequence, cast
from zipfile import ZipFile

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    ClientCredentials,
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
from cognite.client.data_classes.functions import FunctionCallsFilter, FunctionsStatus
from cognite.client.utils._auxiliary import assert_type, is_unlimited, local_import
from cognite.client.utils._identifier import Identifier, IdentifierSequence
from cognite.client.utils._session import create_session_and_return_nonce

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig

HANDLER_FILE_NAME = "handler.py"
MAX_RETRIES = 5
REQUIREMENTS_FILE_NAME = "requirements.txt"
REQUIREMENTS_REG = re.compile(r"(\[\/?requirements\]){1}$", flags=re.M)  # Matches [requirements] and [/requirements]
UNCOMMENTED_LINE_REG = re.compile(r"^[^\#]]*.*")
ALLOWED_HANDLE_ARGS = frozenset({"data", "client", "secrets", "function_call_info"})


def _get_function_internal_id(cognite_client: CogniteClient, identifier: Identifier) -> int:
    primitive = identifier.as_primitive()
    if identifier.is_id:
        return primitive

    if identifier.is_external_id:
        function = cognite_client.functions.retrieve(external_id=primitive)
        if function:
            return function.id

    raise ValueError(f'Function with external ID "{primitive}" is not found')


def _get_function_identifier(function_id: int | None, function_external_id: str | None) -> Identifier:
    identifier = IdentifierSequence.load(function_id, function_external_id, id_name="function")
    if identifier.is_singleton():
        return identifier[0]
    raise AssertionError("Exactly one of function_id and function_external_id must be specified")


class FunctionsAPI(APIClient):
    _RESOURCE_PATH = "/functions"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.calls = FunctionCallsAPI(config, api_version, cognite_client)
        self.schedules = FunctionSchedulesAPI(config, api_version, cognite_client)
        # Variable used to guarantee all items are returned when list(limit) is None, inf or -1.
        self._LIST_LIMIT_CEILING = 10_000

    def create(
        self,
        name: str,
        folder: str | None = None,
        file_id: int | None = None,
        function_path: str = HANDLER_FILE_NAME,
        function_handle: Callable | None = None,
        external_id: str | None = None,
        description: str | None = "",
        owner: str | None = "",
        secrets: dict | None = None,
        env_vars: dict | None = None,
        cpu: Number | None = None,
        memory: Number | None = None,
        runtime: str | None = None,
        metadata: dict | None = None,
        index_url: str | None = None,
        extra_index_urls: list[str] | None = None,
        skip_folder_validation: bool = False,
    ) -> Function:
        '''`When creating a function, <https://developer.cognite.com/api#tag/Functions/operation/postFunctions>`_
        the source code can be specified in one of three ways:

        - Via the `folder` argument, which is the path to the folder where the source code is located. `function_path` must point to a python file in the folder within which a function named `handle` must be defined.
        - Via the `file_id` argument, which is the ID of a zip-file uploaded to the files API. `function_path` must point to a python file in the zipped folder within which a function named `handle` must be defined.
        - Via the `function_handle` argument, which is a reference to a function object, which must be named `handle`.

        The function named `handle` is the entrypoint of the created function. Valid arguments to `handle` are `data`, `client`, `secrets` and `function_call_info`:
        - If the user calls the function with input data, this is passed through the `data` argument.
        - If the user gives one or more secrets when creating the function, these are passed through the `secrets` argument.
        - Data about the function call can be accessed via the argument `function_call_info`, which is a dictionary with keys `function_id` and, if the call is scheduled, `schedule_id` and `scheduled_time`.

        By default, the function is deployed with the latest version of cognite-sdk. If a specific version is desired, it can be specified either in a requirements.txt file when deploying via the `folder` argument or between `[requirements]` tags when deploying via the `function_handle` argument (see example below).

        For help with troubleshooting, please see `this page. <https://docs.cognite.com/cdf/functions/known_issues/>`_

        Args:
            name (str): The name of the function.
            folder (str | None): Path to the folder where the function source code is located.
            file_id (int | None): File ID of the code uploaded to the Files API.
            function_path (str): Relative path from the root folder to the file containing the `handle` function. Defaults to `handler.py`. Must be on POSIX path format.
            function_handle (Callable | None): Reference to a function object, which must be named `handle`.
            external_id (str | None): External id of the function.
            description (str | None): Description of the function.
            owner (str | None): Owner of this function. Typically used to know who created it.
            secrets (dict | None): Additional secrets as key/value pairs. These can e.g. password to simulators or other data sources. Keys must be lowercase characters, numbers or dashes (-) and at most 15 characters. You can create at most 30 secrets, all keys must be unique.
            env_vars (dict | None): Environment variables as key/value pairs. Keys can contain only letters, numbers or the underscore character. You can create at most 100 environment variables.
            cpu (Number | None): Number of CPU cores per function. Allowed values are in the range [0.1, 0.6], and None translates to the API default which is 0.25 in GCP. The argument is unavailable in Azure.
            memory (Number | None): Memory per function measured in GB. Allowed values are in the range [0.1, 2.5], and None translates to the API default which is 1 GB in GCP. The argument is unavailable in Azure.
            runtime (str | None): The function runtime. Valid values are ["py38", "py39", "py310", `None`], and `None` translates to the API default which will change over time. The runtime "py38" resolves to the latest version of the Python 3.8 series.
            metadata (dict | None): Metadata for the function as key/value pairs. Key & values can be at most 32, 512 characters long respectively. You can have at the most 16 key-value pairs, with a maximum size of 512 bytes.
            index_url (str | None): Index URL for Python Package Manager to use. Be aware of the intrinsic security implications of using the `index_url` option. `More information can be found on official docs, <https://docs.cognite.com/cdf/functions/#additional-arguments>`_
            extra_index_urls (list[str] | None): Extra Index URLs for Python Package Manager to use. Be aware of the intrinsic security implications of using the `extra_index_urls` option. `More information can be found on official docs, <https://docs.cognite.com/cdf/functions/#additional-arguments>`_
            skip_folder_validation (bool): When creating a function using the 'folder' argument, pass True to skip the extra validation step that attempts to import the module. Skipping can be useful when your function requires several heavy packages to already be installed locally. Defaults to False.

        Returns:
            Function: The created function.

        Examples:

            Create function with source code in folder::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> function = c.functions.create(
                ...     name="myfunction",
                ...     folder="path/to/code",
                ...     function_path="path/to/function.py")

            Create function with file_id from already uploaded source code::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> function = c.functions.create(
                ...     name="myfunction", file_id=123, function_path="path/to/function.py")

            Create function with predefined function object named `handle`::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> function = c.functions.create(name="myfunction", function_handle=handle)

            Create function with predefined function object named `handle` with dependencies::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>>
                >>> def handle(client, data):
                >>>     """
                >>>     [requirements]
                >>>     numpy
                >>>     [/requirements]
                >>>     """
                >>>     ...
                >>>
                >>> function = c.functions.create(name="myfunction", function_handle=handle)

            .. note::
                When using a predefined function object, you can list dependencies between the tags `[requirements]` and `[/requirements]` in the function's docstring. The dependencies will be parsed and validated in accordance with requirement format specified in `PEP 508 <https://peps.python.org/pep-0508/>`_.
        '''
        self._assert_exactly_one_of_folder_or_file_id_or_function_handle(folder, file_id, function_handle)

        if folder:
            validate_function_folder(folder, function_path, skip_folder_validation)
            file_id = self._zip_and_upload_folder(folder, name, external_id)
        elif function_handle:
            _validate_function_handle(function_handle)
            file_id = self._zip_and_upload_handle(function_handle, name, external_id)
        assert_type(cpu, "cpu", [Number], allow_none=True)
        assert_type(memory, "memory", [Number], allow_none=True)

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
        function: dict[str, Any] = {
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
        if secrets:
            function["secrets"] = secrets
        if extra_index_urls:
            function["extraIndexUrls"] = extra_index_urls
        if index_url:
            function["indexUrl"] = index_url

        res = self._post(url, json={"items": [function]})
        return Function._load(res.json()["items"][0], cognite_client=self._cognite_client)

    def delete(self, id: int | Sequence[int] | None = None, external_id: str | Sequence[str] | None = None) -> None:
        """`Delete one or more functions. <https://developer.cognite.com/api#tag/Functions/operation/deleteFunctions>`_

        Args:
            id (int | Sequence[int] | None): Id or list of ids.
            external_id (str | Sequence[str] | None): External ID or list of external ids.

        Example:

            Delete functions by id or external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.functions.delete(id=[1,2,3], external_id="function3")
        """
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id, external_ids=external_id), wrap_ids=True)

    def list(
        self,
        name: str | None = None,
        owner: str | None = None,
        file_id: int | None = None,
        status: str | None = None,
        external_id_prefix: str | None = None,
        created_time: dict[str, int] | TimestampRange | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> FunctionList:
        """`List all functions. <https://developer.cognite.com/api#tag/Functions/operation/listFunctions>`_

        Args:
            name (str | None): The name of the function.
            owner (str | None): Owner of the function.
            file_id (int | None): The file ID of the zip-file used to create the function.
            status (str | None): Status of the function. Possible values: ["Queued", "Deploying", "Ready", "Failed"].
            external_id_prefix (str | None): External ID prefix to filter on.
            created_time (dict[str, int] | TimestampRange | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            limit (int | None): Maximum number of functions to return. Pass in -1, float('inf') or None to list all.

        Returns:
            FunctionList: List of functions

        Example:

            List functions::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> functions_list = c.functions.list()
        """
        if is_unlimited(limit):
            limit = self._LIST_LIMIT_CEILING

        filter = FunctionFilter(
            name=name,
            owner=owner,
            file_id=file_id,
            status=status,
            external_id_prefix=external_id_prefix,
            created_time=created_time,
        ).dump(camel_case=True)
        res = self._post(url_path=f"{self._RESOURCE_PATH}/list", json={"filter": filter, "limit": limit})

        return FunctionList._load(res.json()["items"], cognite_client=self._cognite_client)

    def retrieve(self, id: int | None = None, external_id: str | None = None) -> Function | None:
        """`Retrieve a single function by id. <https://developer.cognite.com/api#tag/Functions/operation/byIdsFunctions>`_

        Args:
            id (int | None): ID
            external_id (str | None): External ID

        Returns:
            Function | None: Requested function or None if it does not exist.

        Examples:

            Get function by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.functions.retrieve(id=1)

            Get function by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.functions.retrieve(external_id="abc")
        """
        identifier = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(identifiers=identifier, resource_cls=Function, list_cls=FunctionList)

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: Sequence[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> FunctionList | Function | None:
        """`Retrieve multiple functions by id. <https://developer.cognite.com/api#tag/Functions/operation/byIdsFunctions>`_

        Args:
            ids (Sequence[int] | None): IDs
            external_ids (Sequence[str] | None): External IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            FunctionList | Function | None: The requested functions.

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
        assert_type(ids, "id", [Sequence], allow_none=True)
        assert_type(external_ids, "external_id", [Sequence], allow_none=True)
        return self._retrieve_multiple(
            identifiers=IdentifierSequence.load(ids=ids, external_ids=external_ids),
            resource_cls=Function,
            list_cls=FunctionList,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    def call(
        self,
        id: int | None = None,
        external_id: str | None = None,
        data: dict | None = None,
        wait: bool = True,
    ) -> FunctionCall:
        """`Call a function by its ID or external ID. <https://developer.cognite.com/api#tag/Function-calls/operation/postFunctionsCall>`_.

        Args:
            id (int | None): ID
            external_id (str | None): External ID
            data (dict | None): Input data to the function (JSON serializable). This data is passed deserialized into the function through one of the arguments called data. **WARNING:** Secrets or other confidential information should not be passed via this argument. There is a dedicated `secrets` argument in FunctionsAPI.create() for this purpose.'
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
        identifier = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()[0]
        id = _get_function_internal_id(self._cognite_client, identifier)
        nonce = create_session_and_return_nonce(self._cognite_client, api_name="Functions API")

        if data is None:
            data = {}
        url = f"/functions/{id}/call"
        res = self._post(url, json={"data": data, "nonce": nonce})

        function_call = FunctionCall._load(res.json(), cognite_client=self._cognite_client)
        if wait:
            function_call.wait()
        return function_call

    def limits(self) -> FunctionsLimits:
        """`Get service limits. <https://developer.cognite.com/api#tag/Functions/operation/functionsLimits>`_.

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

    def _zip_and_upload_folder(self, folder: str, name: str, external_id: str | None = None) -> int:
        name = _sanitize_filename(name)
        current_dir = os.getcwd()
        os.chdir(folder)
        try:
            with TemporaryDirectory() as tmpdir:
                zip_path = Path(tmpdir, "function.zip")
                with ZipFile(zip_path, "w") as zf:
                    for root, dirs, files in os.walk("."):
                        zf.write(root)

                        for filename in files:
                            zf.write(Path(root, filename))

                overwrite = True if external_id else False
                file = self._cognite_client.files.upload_bytes(
                    zip_path.read_bytes(), name=f"{name}.zip", external_id=external_id, overwrite=overwrite
                )
                return cast(int, file.id)
        finally:
            os.chdir(current_dir)

    def _zip_and_upload_handle(self, function_handle: Callable, name: str, external_id: str | None = None) -> int:
        name = _sanitize_filename(name)
        docstr_requirements = _get_fn_docstring_requirements(function_handle)

        with TemporaryDirectory() as tmpdir:
            handle_path = Path(tmpdir, HANDLER_FILE_NAME)
            with handle_path.open("w") as f:
                f.write(textwrap.dedent(getsource(function_handle)))

            if docstr_requirements:
                requirements_path = Path(tmpdir, REQUIREMENTS_FILE_NAME)
                with requirements_path.open("w") as f:
                    for req in docstr_requirements:
                        f.write(f"{req}\n")

            zip_path = Path(tmpdir, "function.zip")
            with ZipFile(zip_path, "w") as zf:
                zf.write(handle_path, arcname=HANDLER_FILE_NAME)
                if docstr_requirements:
                    zf.write(requirements_path, arcname=REQUIREMENTS_FILE_NAME)

            overwrite = True if external_id else False
            file = self._cognite_client.files.upload_bytes(
                zip_path.read_bytes(), name=f"{name}.zip", external_id=external_id, overwrite=overwrite
            )
            return cast(int, file.id)

    @staticmethod
    def _assert_exactly_one_of_folder_or_file_id_or_function_handle(
        folder: str | None, file_id: int | None, function_handle: Callable[..., Any] | None
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
        """`Activate functions for the Project. <https://developer.cognite.com/api#tag/Functions/operation/postFunctionsStatus>`_.

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
        """`Functions activation status for the Project. <https://developer.cognite.com/api#tag/Functions/operation/getFunctionsStatus>`_.

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


def get_handle_function_node(file_path: Path) -> ast.FunctionDef | None:
    return next(
        (
            item
            for item in ast.walk(ast.parse(file_path.read_text()))
            if isinstance(item, ast.FunctionDef) and item.name == "handle"
        ),
        None,
    )


def _run_import_check(queue: Queue, root_path: str, module_path: str) -> None:
    if __name__ == "__main__":
        raise RuntimeError("This should not be run in the main process (has side-effects)")

    import sys as internal_sys  # let's not shadow outer scope

    internal_sys.path.insert(0, root_path)
    try:
        importlib.import_module(module_path)
        queue.put(None)
    except Exception as err:
        queue.put(err)


def _run_import_check_backup(root_path: str, module_path: str) -> None:
    existing_modules = set(sys.modules)
    sys.path.insert(0, root_path)
    try:
        importlib.import_module(module_path)
    finally:
        sys.path.remove(root_path)
        # Properly unloading modules is not supported in Python, but we can come close:
        # https://github.com/python/cpython/issues/53318
        for new_mod in set(sys.modules) - existing_modules:
            sys.modules.pop(new_mod, None)


def _check_imports(root_path: str, module_path: str) -> None:
    queue: Queue[Exception | None] = Queue()
    validator = Process(
        target=_run_import_check,
        name="import-validator",
        args=(queue, root_path, module_path),
        daemon=True,
    )
    try:
        validator.start()
    except OSError:
        # Handle Pyodide/WASM exception: 'OSError: [Errno 52] Function not implemented'
        _run_import_check_backup(root_path, module_path)
    else:
        validator.join()
        if (error := queue.get_nowait()) is not None:
            raise error


def validate_function_folder(root_path: str, function_path: str, skip_folder_validation: bool) -> None:
    if not function_path.endswith(".py"):
        raise TypeError(f"{function_path} must be a Python file.")

    file_path = Path(root_path, function_path)
    if not file_path.is_file():
        raise FileNotFoundError(f"No file found at '{file_path}'.")

    if node := get_handle_function_node(file_path):
        _validate_function_handle(node)
    else:
        raise TypeError(f"{function_path} must contain a function named 'handle'.")

    if not skip_folder_validation:
        module_path = ".".join(Path(function_path).with_suffix("").parts)
        _check_imports(root_path, module_path)


def _validate_function_handle(handle_obj: Callable | ast.FunctionDef) -> None:
    if isinstance(handle_obj, ast.FunctionDef):
        name = handle_obj.name
        accepts_args = set(arg.arg for arg in handle_obj.args.args)
    else:
        name = handle_obj.__name__
        accepts_args = set(signature(handle_obj).parameters)

    if name != "handle":
        raise TypeError(f"Function is named '{name}' but must be named 'handle'.")

    if not accepts_args <= ALLOWED_HANDLE_ARGS:
        raise TypeError(f"Arguments {accepts_args} to the function must be a subset of {ALLOWED_HANDLE_ARGS}.")


def _extract_requirements_from_file(file_name: str) -> list[str]:
    """Extracts a list of library requirements from a file. Comments, lines starting with '#', are ignored.

    Args:
        file_name (str): name of the file to parse

    Returns:
        list[str]: returns a list of library records
    """
    requirements: list[str] = []
    with open(file_name, "r+") as f:
        for line in f:
            line = line.strip()
            if UNCOMMENTED_LINE_REG.match(line):
                requirements.append(line)
    return requirements


def _extract_requirements_from_doc_string(docstr: str) -> list[str] | None:
    """Extracts a list of library requirements defined between [requirements] and [/requirements] in a functions docstring.

    Args:
        docstr (str): the docstring to extract requirements from

    Returns:
        list[str] | None: returns a list of library records if requirements are defined in the docstring, else None
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


def _validate_and_parse_requirements(requirements: list[str]) -> list[str]:
    """Validates the requirement specifications

    Args:
        requirements (list[str]): list of requirement specifications
    Raises:
        ValueError: if validation of requirements fails
    Returns:
        list[str]: The parsed requirements
    """
    constructors = cast(Any, local_import("pip._internal.req.constructors"))
    install_req_from_line = constructors.install_req_from_line
    parsed_reqs: list[str] = []
    for req in requirements:
        try:
            parsed = install_req_from_line(req)
        except Exception as e:
            raise ValueError(str(e))

        parsed_reqs.append(str(parsed).strip())
    return parsed_reqs


def _get_fn_docstring_requirements(fn: Callable) -> list[str]:
    """Read requirements from a function docstring, validate them and return.

    Args:
        fn (Callable): the function to read requirements from

    Returns:
        list[str]: A (possibly empty) list of requirements.
    """
    docstr = getdoc(fn)

    if docstr:
        reqs = _extract_requirements_from_doc_string(docstr)
        if reqs:
            parsed_reqs = _validate_and_parse_requirements(reqs)
            return parsed_reqs

    return []


def _sanitize_filename(filename: str) -> str:
    # Forwardslash, '/', is not allowed in file names:
    return filename.replace("/", "-")


class FunctionCallsAPI(APIClient):
    _RESOURCE_PATH = "/functions/{}/calls"
    _RESOURCE_PATH_RESPONSE = "/functions/{}/calls/{}/response"
    _RESOURCE_PATH_LOGS = "/functions/{}/calls/{}/logs"

    def list(
        self,
        function_id: int | None = None,
        function_external_id: str | None = None,
        status: str | None = None,
        schedule_id: int | None = None,
        start_time: dict[str, int] | None = None,
        end_time: dict[str, int] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> FunctionCallList:
        """`List all calls associated with a specific function id. <https://developer.cognite.com/api#tag/Function-calls/operation/listFunctionCalls>`_ Either function_id or function_external_id must be specified.

        Args:
            function_id (int | None): ID of the function on which the calls were made.
            function_external_id (str | None): External ID of the function on which the calls were made.
            status (str | None): Status of the call. Possible values ["Running", "Failed", "Completed", "Timeout"].
            schedule_id (int | None): Schedule id from which the call belongs (if any).
            start_time (dict[str, int] | None): Start time of the call. Possible keys are `min` and `max`, with values given as time stamps in ms.
            end_time (dict[str, int] | None): End time of the call. Possible keys are `min` and `max`, with values given as time stamps in ms.
            limit (int | None): Maximum number of function calls to list. Pass in -1, float('inf') or None to list all Function Calls.

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
        resource_path = self._RESOURCE_PATH.format(function_id)
        return self._list(
            method="POST",
            resource_path=resource_path,
            filter=filter,
            limit=limit,
            resource_cls=FunctionCall,
            list_cls=FunctionCallList,
        )

    def retrieve(
        self, call_id: int, function_id: int | None = None, function_external_id: str | None = None
    ) -> FunctionCall | None:
        """`Retrieve a single function call by id. <https://developer.cognite.com/api#tag/Function-calls/operation/byIdsFunctionCalls>`_

        Args:
            call_id (int): ID of the call.
            function_id (int | None): ID of the function on which the call was made.
            function_external_id (str | None): External ID of the function on which the call was made.

        Returns:
            FunctionCall | None: Requested function call or None if either call ID or function identifier is not found.

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

        resource_path = self._RESOURCE_PATH.format(function_id)

        return self._retrieve_multiple(
            resource_path=resource_path,
            identifiers=IdentifierSequence.load(ids=call_id).as_singleton(),
            resource_cls=FunctionCall,
            list_cls=FunctionCallList,
        )

    def get_response(
        self, call_id: int, function_id: int | None = None, function_external_id: str | None = None
    ) -> dict | None:
        """`Retrieve the response from a function call. <https://developer.cognite.com/api#tag/Function-calls/operation/getFunctionCallResponse>`_

        Args:
            call_id (int): ID of the call.
            function_id (int | None): ID of the function on which the call was made.
            function_external_id (str | None): External ID of the function on which the call was made.

        Returns:
            dict | None: Response from the function call.

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

        resource_path = self._RESOURCE_PATH_RESPONSE.format(function_id, call_id)
        return self._get(resource_path).json().get("response")

    def get_logs(
        self, call_id: int, function_id: int | None = None, function_external_id: str | None = None
    ) -> FunctionCallLog:
        """`Retrieve logs for function call. <https://developer.cognite.com/api#tag/Function-calls/operation/getFunctionCalls>`_

        Args:
            call_id (int): ID of the call.
            function_id (int | None): ID of the function on which the call was made.
            function_external_id (str | None): External ID of the function on which the call was made.

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

        resource_path = self._RESOURCE_PATH_LOGS.format(function_id, call_id)
        return FunctionCallLog._load(self._get(resource_path).json()["items"])


class FunctionSchedulesAPI(APIClient):
    _RESOURCE_PATH = "/functions/schedules"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._LIST_LIMIT_CEILING = 10_000

    def retrieve(self, id: int) -> FunctionSchedule | None:
        """`Retrieve a single function schedule by id. <https://developer.cognite.com/api#tag/Function-schedules/operation/byIdsFunctionSchedules>`_

        Args:
            id (int): Schedule ID

        Returns:
            FunctionSchedule | None: Requested function schedule or None if not found.

        Examples:

            Get function schedule by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.functions.schedules.retrieve(id=1)

        """
        identifier = IdentifierSequence.load(ids=id).as_singleton()
        return self._retrieve_multiple(
            identifiers=identifier, resource_cls=FunctionSchedule, list_cls=FunctionSchedulesList
        )

    def list(
        self,
        name: str | None = None,
        function_id: int | None = None,
        function_external_id: str | None = None,
        created_time: dict[str, int] | TimestampRange | None = None,
        cron_expression: str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> FunctionSchedulesList:
        """`List all schedules associated with a specific project. <https://developer.cognite.com/api#tag/Function-schedules/operation/listFunctionSchedules>`_

        Args:
            name (str | None): Name of the function schedule.
            function_id (int | None): ID of the function the schedules are linked to.
            function_external_id (str | None): External ID of the function the schedules are linked to.
            created_time (dict[str, int] | TimestampRange | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            cron_expression (str | None): Cron expression.
            limit (int | None): Maximum number of schedules to list. Pass in -1, float('inf') or None to list all.

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
                raise AssertionError(
                    "Both 'function_id' and 'function_external_id' were supplied, pass exactly one or neither."
                )

        if is_unlimited(limit):
            limit = self._LIST_LIMIT_CEILING

        filter = FunctionSchedulesFilter(
            name=name,
            function_id=function_id,
            function_external_id=function_external_id,
            created_time=created_time,
            cron_expression=cron_expression,
        ).dump(camel_case=True)
        res = self._post(url_path=f"{self._RESOURCE_PATH}/list", json={"filter": filter, "limit": limit})

        return FunctionSchedulesList._load(res.json()["items"], cognite_client=self._cognite_client)

    # TODO: Major version 7, remove 'function_external_id' which only worked when using API-keys.
    def create(
        self,
        name: str,
        cron_expression: str,
        function_id: int | None = None,
        function_external_id: str | None = None,
        client_credentials: dict | ClientCredentials | None = None,
        description: str = "",
        data: dict | None = None,
    ) -> FunctionSchedule:
        """`Create a schedule associated with a specific project. <https://developer.cognite.com/api#tag/Function-schedules/operation/postFunctionSchedules>`_

        Args:
            name (str): Name of the schedule.
            cron_expression (str): Cron expression.
            function_id (int | None): Id of the function. This is required if the schedule is created with client_credentials.
            function_external_id (str | None): External id of the function. **NOTE**: This is deprecated and will be removed in a future major version.
            client_credentials (dict | ClientCredentials | None): (optional, ClientCredentials, Dict): Instance of ClientCredentials or a dictionary containing client credentials: client_id client_secret
            description (str): Description of the schedule.
            data (dict | None): Data to be passed to the scheduled run.

        Returns:
            FunctionSchedule: Created function schedule.

        Warning:
            Do not pass secrets or other confidential information via the ``data`` argument. There is a dedicated
            ``secrets`` argument in FunctionsAPI.create() for this purpose.

        Examples:

            Create a function schedule that runs using specified client credentials (**recommended**)::

                >>> import os
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ClientCredentials
                >>> c = CogniteClient()
                >>> schedule = c.functions.schedules.create(
                ...     name="My schedule",
                ...     function_id=123,
                ...     cron_expression="*/5 * * * *",
                ...     client_credentials=ClientCredentials("my-client-id", os.environ["MY_CLIENT_SECRET"]),
                ...     description="This schedule does magic stuff.",
                ...     data={"magic": "stuff"},
                ... )

            You may also create a schedule that runs with your -current- credentials, i.e. the same credentials you used
            to instantiate the ``CogniteClient`` (that you're using right now). **Note**: Unless you happen to already use
            client credentials, *this is not a recommended way to create schedules*, as it will create an explicit dependency
            on your user account, which it will run the function "on behalf of" (until the schedule is eventually removed)::

                >>> schedule = c.functions.schedules.create(
                ...     name="My schedule",
                ...     function_id=456,
                ...     cron_expression="*/5 * * * *",
                ...     description="A schedule just used for some temporary testing.",
                ... )

        """
        _get_function_identifier(function_id, function_external_id)
        nonce = create_session_and_return_nonce(
            self._cognite_client, api_name="Functions API", client_credentials=client_credentials
        )
        body: dict[str, list[dict[str, str | int | None | dict]]] = {
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

        res = self._post(self._RESOURCE_PATH, json=body)
        return FunctionSchedule._load(res.json()["items"][0], cognite_client=self._cognite_client)

    def delete(self, id: int) -> None:
        """`Delete a schedule associated with a specific project. <https://developer.cognite.com/api#tag/Function-schedules/operation/deleteFunctionSchedules>`_

        Args:
            id (int): Id of the schedule

        Examples:

            Delete function schedule::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.functions.schedules.delete(id = 123)

        """
        url = f"{self._RESOURCE_PATH}/delete"
        self._post(url, json={"items": [{"id": id}]})

    def get_input_data(self, id: int) -> dict | None:
        """`Retrieve the input data to the associated function. <https://developer.cognite.com/api#tag/Function-schedules/operation/getFunctionScheduleInputData>`_
        Args:
            id (int): Id of the schedule

        Returns:
            dict | None: Input data to the associated function or None if not set. This data is passed deserialized into the function through the data argument.

        Examples:

            Get schedule input data::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.functions.schedules.get_input_data(id=123)
        """
        url = f"{self._RESOURCE_PATH}/{id}/input_data"
        res = self._get(url)

        return res.json().get("data")
