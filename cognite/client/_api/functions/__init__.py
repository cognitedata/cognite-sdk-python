from __future__ import annotations

import ast
import importlib
import os
import re
import sys
import textwrap
import time
from collections.abc import Callable, Iterator, Sequence
from inspect import getdoc, getsource, signature
from multiprocessing import Process, Queue
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Literal, cast, overload
from zipfile import ZipFile

from cognite.client._api.functions.calls import FunctionCallsAPI
from cognite.client._api.functions.schedules import FunctionSchedulesAPI
from cognite.client._api_client import APIClient
from cognite.client._constants import _RUNNING_IN_BROWSER, DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    Function,
    FunctionFilter,
    FunctionList,
    FunctionsLimits,
    TimestampRange,
)
from cognite.client.data_classes.functions import (
    HANDLER_FILE_NAME,
    FunctionCall,
    FunctionHandle,
    FunctionsStatus,
    FunctionStatus,
    FunctionWrite,
    RunTime,
)
from cognite.client.utils._auxiliary import (
    is_unlimited,
    split_into_chunks,
)
from cognite.client.utils._identifier import Identifier, IdentifierSequence
from cognite.client.utils._importing import local_import
from cognite.client.utils._session import create_session_and_return_nonce
from cognite.client.utils._validation import assert_type
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


MAX_RETRIES = 5
REQUIREMENTS_FILE_NAME = "requirements.txt"
# Match [requirements] and [/requirements]:
REQUIREMENTS_REG = re.compile(r"(\[\/?requirements\]){1}$", flags=re.M)
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


class FunctionsAPI(APIClient):
    _RESOURCE_PATH = "/functions"
    _RESOURCE_PATH_CALL = "/functions/{}/call"

    def __init__(
        self,
        config: ClientConfig,
        api_version: str | None,
        cognite_client: CogniteClient,
    ) -> None:
        super().__init__(config, api_version, cognite_client)
        self.calls = FunctionCallsAPI(config, api_version, cognite_client)
        self.schedules = FunctionSchedulesAPI(config, api_version, cognite_client)
        self._RETRIEVE_LIMIT = 10
        self._DELETE_LIMIT = 10

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        name: str | None = None,
        owner: str | None = None,
        file_id: int | None = None,
        status: FunctionStatus | None = None,
        external_id_prefix: str | None = None,
        created_time: dict[Literal["min", "max"], int] | TimestampRange | None = None,
        metadata: dict[str, str] | None = None,
        limit: int | None = None,
    ) -> Iterator[Function]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        name: str | None = None,
        owner: str | None = None,
        file_id: int | None = None,
        status: FunctionStatus | None = None,
        external_id_prefix: str | None = None,
        created_time: dict[Literal["min", "max"], int] | TimestampRange | None = None,
        metadata: dict[str, str] | None = None,
        limit: int | None = None,
    ) -> Iterator[FunctionList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        name: str | None = None,
        owner: str | None = None,
        file_id: int | None = None,
        status: FunctionStatus | None = None,
        external_id_prefix: str | None = None,
        created_time: dict[Literal["min", "max"], int] | TimestampRange | None = None,
        metadata: dict[str, str] | None = None,
        limit: int | None = None,
    ) -> Iterator[Function] | Iterator[FunctionList]:
        """Iterate over functions.

        Args:
            chunk_size (int | None): Number of functions to yield per chunk. Defaults to yielding functions one by one.
            name (str | None): The name of the function.
            owner (str | None): Owner of the function.
            file_id (int | None): The file ID of the zip-file used to create the function.
            status (FunctionStatus | None): Status of the function. Possible values: ["Queued", "Deploying", "Ready", "Failed"].
            external_id_prefix (str | None): External ID prefix to filter on.
            created_time (dict[Literal['min', 'max'], int] | TimestampRange | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            metadata (dict[str, str] | None): No description.
            limit (int | None): Maximum number of functions to return. Defaults to yielding all functions.

        Returns:
            Iterator[Function] | Iterator[FunctionList]: An iterator over functions.
        """
        # The _list_generator method is not used as the /list endpoint does not
        # respond with a cursor (pagination is not supported)
        functions = self.list(
            name=name,
            owner=owner,
            file_id=file_id,
            status=status,
            external_id_prefix=external_id_prefix,
            created_time=created_time,
            metadata=metadata,
            limit=limit,
        )
        if chunk_size is None:
            return iter(functions)
        return (
            FunctionList(chunk, cognite_client=self._cognite_client)
            for chunk in split_into_chunks(functions.data, chunk_size)
        )

    def __iter__(self) -> Iterator[Function]:
        """Iterate over all functions."""
        return self()

    def create(
        self,
        name: str | FunctionWrite,
        folder: str | None = None,
        file_id: int | None = None,
        function_path: str = HANDLER_FILE_NAME,
        function_handle: FunctionHandle | None = None,
        external_id: str | None = None,
        description: str | None = None,
        owner: str | None = None,
        secrets: dict[str, str] | None = None,
        env_vars: dict[str, str] | None = None,
        cpu: float | None = None,
        memory: float | None = None,
        runtime: RunTime | None = None,
        metadata: dict[str, str] | None = None,
        index_url: str | None = None,
        extra_index_urls: list[str] | None = None,
        skip_folder_validation: bool = False,
        data_set_id: int | None = None,
    ) -> Function:
        '''`When creating a function, <https://developer.cognite.com/api#tag/Functions/operation/postFunctions>`_
        the source code can be specified in one of three ways:

        - Via the `folder` argument, which is the path to the folder where the source code is located. `function_path` must point to a python file in the folder within which a function named `handle` must be defined.
        - Via the `file_id` argument, which is the ID of a zip-file uploaded to the files API. `function_path` must point to a python file in the zipped folder within which a function named `handle` must be defined.
        - Via the `function_handle` argument, which is a reference to a function object, which must be named `handle`.

        The function named `handle` is the entrypoint of the created function. Valid arguments to `handle` are `data`, `client`, `secrets` and `function_call_info`:
        - If the user calls the function with input data, this is passed through the `data` argument.
        - If the user gives one or more secrets when creating the function, these are passed through the `secrets` argument.
        - Data about the function call can be accessed via the argument `function_call_info`, which is a dictionary with keys `function_id`, `call_id`, and, if the call is scheduled, `schedule_id` and `scheduled_time`.

        By default, the function is deployed with the latest version of cognite-sdk. If a specific version is desired, it can be specified either in a requirements.txt file when deploying via the `folder` argument or between `[requirements]` tags when deploying via the `function_handle` argument (see example below).

        For help with troubleshooting, please see `this page. <https://docs.cognite.com/cdf/functions/known_issues/>`_

        Args:
            name (str | FunctionWrite): The name of the function or a FunctionWrite object. If a FunctionWrite
                object is passed, all other arguments are ignored.
            folder (str | None): Path to the folder where the function source code is located.
            file_id (int | None): File ID of the code uploaded to the Files API.
            function_path (str): Relative path from the root folder to the file containing the `handle` function. Defaults to `handler.py`. Must be on POSIX path format.
            function_handle (FunctionHandle | None): Reference to a function object, which must be named `handle`.
            external_id (str | None): External id of the function.
            description (str | None): Description of the function.
            owner (str | None): Owner of this function. Typically used to know who created it.
            secrets (dict[str, str] | None): Additional secrets as key/value pairs. These can e.g. password to simulators or other data sources. Keys must be lowercase characters, numbers or dashes (-) and at most 15 characters. You can create at most 30 secrets, all keys must be unique.
            env_vars (dict[str, str] | None): Environment variables as key/value pairs. Keys can contain only letters, numbers or the underscore character. You can create at most 100 environment variables.
            cpu (float | None): Number of CPU cores per function. Allowed range and default value are given by the `limits endpoint. <https://developer.cognite.com/api#tag/Functions/operation/functionsLimits>`_, and None translates to the API default. On Azure, only the default value is used.
            memory (float | None): Memory per function measured in GB. Allowed range and default value are given by the `limits endpoint. <https://developer.cognite.com/api#tag/Functions/operation/functionsLimits>`_, and None translates to the API default. On Azure, only the default value is used.
            runtime (RunTime | None): The function runtime. Valid values are ["py310", "py311", "py312", `None`], and `None` translates to the API default which will change over time. The runtime "py312" resolves to the latest version of the Python 3.12 series.
            metadata (dict[str, str] | None): Metadata for the function as key/value pairs. Key & values can be at most 32, 512 characters long respectively. You can have at the most 16 key-value pairs, with a maximum size of 512 bytes.
            index_url (str | None): Index URL for Python Package Manager to use. Be aware of the intrinsic security implications of using the `index_url` option. `More information can be found on official docs, <https://docs.cognite.com/cdf/functions/#additional-arguments>`_
            extra_index_urls (list[str] | None): Extra Index URLs for Python Package Manager to use. Be aware of the intrinsic security implications of using the `extra_index_urls` option. `More information can be found on official docs, <https://docs.cognite.com/cdf/functions/#additional-arguments>`_
            skip_folder_validation (bool): When creating a function using the 'folder' argument, pass True to skip the extra validation step that attempts to import the module. Skipping can be useful when your function requires several heavy packages to already be installed locally. Defaults to False.
            data_set_id (int | None): Data set to upload the function code to. Note: Does not affect the function itself.

        Returns:
            Function: The created function.

        Examples:

            Create function with source code in folder:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> function = client.functions.create(
                ...     name="myfunction",
                ...     folder="path/to/code",
                ...     function_path="path/to/function.py")

            Create function with file_id from already uploaded source code:

                >>> function = client.functions.create(
                ...     name="myfunction", file_id=123, function_path="path/to/function.py")

            Create function with predefined function object named `handle`:

                >>> function = client.functions.create(name="myfunction", function_handle=handle)

            Create function with predefined function object named `handle` with dependencies:

                >>> def handle(client, data):
                >>>     """
                >>>     [requirements]
                >>>     numpy
                >>>     [/requirements]
                >>>     """
                >>>     pass
                >>>
                >>> function = client.functions.create(name="myfunction", function_handle=handle)

            .. note:
                When using a predefined function object, you can list dependencies between the tags `[requirements]` and `[/requirements]` in the function's docstring.
                The dependencies will be parsed and validated in accordance with requirement format specified in `PEP 508 <https://peps.python.org/pep-0508/>`_.
        '''
        if isinstance(name, FunctionWrite):
            function_input = name
        else:
            function_input = self._create_function_obj(
                name,
                folder,
                file_id,
                function_path,
                function_handle,
                external_id,
                description,
                owner,
                secrets,
                env_vars,
                cpu,
                memory,
                runtime,
                metadata,
                index_url,
                extra_index_urls,
                skip_folder_validation,
                data_set_id,
            )

        # The exactly_one_is_not_none check ensures that function is not None
        res = self._post(self._RESOURCE_PATH, json={"items": [function_input.dump(camel_case=True)]})
        return Function._load(res.json()["items"][0], cognite_client=self._cognite_client)

    def _create_function_obj(
        self,
        name: str,
        folder: str | None = None,
        file_id: int | None = None,
        function_path: str = HANDLER_FILE_NAME,
        function_handle: FunctionHandle | None = None,
        external_id: str | None = None,
        description: str | None = None,
        owner: str | None = None,
        secrets: dict[str, str] | None = None,
        env_vars: dict[str, str] | None = None,
        cpu: float | None = None,
        memory: float | None = None,
        runtime: RunTime | None = None,
        metadata: dict[str, str] | None = None,
        index_url: str | None = None,
        extra_index_urls: list[str] | None = None,
        skip_folder_validation: bool = False,
        data_set_id: int | None = None,
    ) -> FunctionWrite:
        self._assert_exactly_one_of_folder_or_file_id_or_function_handle(folder, file_id, function_handle)
        # This is extra functionality on top of the API to allow deploying functions
        # without having uploaded the code to the files API.
        if folder:
            validate_function_folder(folder, function_path, skip_folder_validation)
            file_id = self._zip_and_upload_folder(folder, name, external_id, data_set_id)
        elif function_handle:
            _validate_function_handle(function_handle)
            file_id = self._zip_and_upload_handle(function_handle, name, external_id, data_set_id)
        assert_type(cpu, "cpu", [float], allow_none=True)
        assert_type(memory, "memory", [float], allow_none=True)
        sleep_time = 1.0  # seconds
        for i in range(MAX_RETRIES):
            file = self._cognite_client.files.retrieve(id=file_id)
            if file and file.uploaded:
                break
            time.sleep(sleep_time)
            sleep_time *= 2
        else:
            raise RuntimeError("Could not retrieve file from files API")
        function = FunctionWrite(
            name=name,
            # Due to _assert_exactly_one_of_folder_or_file_id_or_function_handle we know that file_id is not None
            file_id=cast(int, file_id),
            external_id=external_id,
            description=description,
            owner=owner,
            secrets=secrets,
            env_vars=env_vars,
            function_path=function_path,
            cpu=cpu,
            memory=memory,
            runtime=runtime,
            metadata=metadata,
            index_url=index_url,
            extra_index_urls=extra_index_urls,
        )
        return function

    def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
    ) -> None:
        """`Delete one or more functions. <https://developer.cognite.com/api#tag/Functions/operation/deleteFunctions>`_

        Args:
            id (int | Sequence[int] | None): Id or list of ids.
            external_id (str | SequenceNotStr[str] | None): External ID or list of external ids.

        Example:

            Delete functions by id or external id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.functions.delete(id=[1,2,3], external_id="function3")
        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=id, external_ids=external_id),
            wrap_ids=True,
        )

    def list(
        self,
        name: str | None = None,
        owner: str | None = None,
        file_id: int | None = None,
        status: FunctionStatus | None = None,
        external_id_prefix: str | None = None,
        created_time: dict[Literal["min", "max"], int] | TimestampRange | None = None,
        metadata: dict[str, str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> FunctionList:
        """`List all functions. <https://developer.cognite.com/api#tag/Functions/operation/listFunctions>`_

        Args:
            name (str | None): The name of the function.
            owner (str | None): Owner of the function.
            file_id (int | None): The file ID of the zip-file used to create the function.
            status (FunctionStatus | None): Status of the function. Possible values: ["Queued", "Deploying", "Ready", "Failed"].
            external_id_prefix (str | None): External ID prefix to filter on.
            created_time (dict[Literal['min', 'max'], int] | TimestampRange | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            metadata (dict[str, str] | None): Custom, application-specific metadata. String key -> String value. Limits: Maximum length of key is 32, value 512 characters, up to 16 key-value pairs. Maximum size of entire metadata is 4096 bytes.
            limit (int | None): Maximum number of functions to return. Pass in -1, float('inf') or None to list all.

        Returns:
            FunctionList: List of functions

        Example:

            List functions::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> functions_list = client.functions.list()
        """
        if is_unlimited(limit):
            # Variable used to guarantee all items are returned when list(limit) is None, inf or -1.
            limit = 10_000

        filter = FunctionFilter(
            name=name,
            owner=owner,
            file_id=file_id,
            status=status,
            external_id_prefix=external_id_prefix,
            created_time=created_time,
            metadata=metadata,
        ).dump(camel_case=True)

        # The _list method is not used as the /list endpoint does not
        # respond with a cursor (pagination is not supported)
        res = self._post(
            url_path=f"{self._RESOURCE_PATH}/list",
            json={"filter": filter, "limit": limit},
        )

        return FunctionList._load(res.json()["items"], cognite_client=self._cognite_client)

    def retrieve(self, id: int | None = None, external_id: str | None = None) -> Function | None:
        """`Retrieve a single function by id. <https://developer.cognite.com/api#tag/Functions/operation/byIdsFunctions>`_

        Args:
            id (int | None): ID
            external_id (str | None): External ID

        Returns:
            Function | None: Requested function or None if it does not exist.

        Examples:

            Get function by id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.functions.retrieve(id=1)

            Get function by external id:

                >>> res = client.functions.retrieve(external_id="abc")
        """
        identifier = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(identifiers=identifier, resource_cls=Function, list_cls=FunctionList)

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> FunctionList:
        """`Retrieve multiple functions by id. <https://developer.cognite.com/api#tag/Functions/operation/byIdsFunctions>`_

        Args:
            ids (Sequence[int] | None): IDs
            external_ids (SequenceNotStr[str] | None): External IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            FunctionList: The requested functions.

        Examples:

            Get function by id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.functions.retrieve_multiple(ids=[1, 2, 3])

            Get functions by external id:

                >>> res = client.functions.retrieve_multiple(external_ids=["func1", "func2"])
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
        data: dict[str, object] | None = None,
        wait: bool = True,
        nonce: str | None = None,
    ) -> FunctionCall:
        """`Call a function by its ID or external ID. <https://developer.cognite.com/api#tag/Function-calls/operation/postFunctionsCall>`_.

        Args:
            id (int | None): ID
            external_id (str | None): External ID
            data (dict[str, object] | None): Input data to the function (JSON serializable). This data is passed deserialized into the function through one of the arguments called data. **WARNING:** Secrets or other confidential information should not be passed via this argument. There is a dedicated `secrets` argument in FunctionsAPI.create() for this purpose.'
            wait (bool): Wait until the function call is finished. Defaults to True.
            nonce (str | None): Nonce retrieved from sessions API when creating a session. This will be used to bind the session before executing the function. If not provided, a new session will be created based on the client credentials.

        Tip:
            You can create a session via the Sessions API, using the client.iam.session.create() method.

        Returns:
            FunctionCall: A function call object.

        Examples:

            Call a function by id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> call = client.functions.call(id=1)

            Call a function directly on the `Function` object:

                >>> func = client.functions.retrieve(id=1)
                >>> call = func.call()
        """
        identifier = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()[0]
        id = _get_function_internal_id(self._cognite_client, identifier)
        nonce = nonce or create_session_and_return_nonce(self._cognite_client, api_name="Functions API")

        if data is None:
            data = {}
        url = self._RESOURCE_PATH_CALL.format(id)
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

            Call a function by id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> limits = client.functions.limits()
        """
        res = self._get(self._RESOURCE_PATH + "/limits")
        return FunctionsLimits.load(res.json())

    def _zip_and_upload_folder(
        self,
        folder: str,
        name: str,
        external_id: str | None = None,
        data_set_id: int | None = None,
    ) -> int:
        name = _sanitize_filename(name)
        current_dir = os.getcwd()
        os.chdir(folder)
        try:
            with TemporaryDirectory() as tmpdir:
                zip_path = Path(tmpdir, "function.zip")
                with ZipFile(zip_path, "w", strict_timestamps=False) as zf:
                    for root, dirs, files in os.walk("."):
                        zf.write(root)

                        for filename in files:
                            zf.write(Path(root, filename))

                overwrite = bool(external_id)
                file = self._cognite_client.files.upload_bytes(
                    zip_path.read_bytes(),
                    name=f"{name}.zip",
                    external_id=external_id,
                    overwrite=overwrite,
                    data_set_id=data_set_id,
                )
                return file.id
        finally:
            os.chdir(current_dir)

    def _zip_and_upload_handle(
        self,
        function_handle: Callable,
        name: str,
        external_id: str | None = None,
        data_set_id: int | None = None,
    ) -> int:
        name = _sanitize_filename(name)
        docstr_requirements = _get_fn_docstring_requirements(function_handle)

        with TemporaryDirectory() as tmpdir:
            handle_path = Path(tmpdir, HANDLER_FILE_NAME)
            with handle_path.open("w") as f:
                f.write(textwrap.dedent(getsource(function_handle)))

            requirements_path = Path(tmpdir, REQUIREMENTS_FILE_NAME)
            if docstr_requirements:
                with requirements_path.open("w") as f:
                    for req in docstr_requirements:
                        f.write(f"{req}\n")

            zip_path = Path(tmpdir, "function.zip")
            with ZipFile(zip_path, "w", strict_timestamps=False) as zf:
                zf.write(handle_path, arcname=HANDLER_FILE_NAME)
                if docstr_requirements:
                    zf.write(requirements_path, arcname=REQUIREMENTS_FILE_NAME)

            overwrite = bool(external_id)
            file = self._cognite_client.files.upload_bytes(
                zip_path.read_bytes(),
                name=f"{name}.zip",
                external_id=external_id,
                overwrite=overwrite,
                data_set_id=data_set_id,
            )
            return file.id

    @staticmethod
    def _assert_exactly_one_of_folder_or_file_id_or_function_handle(
        folder: str | None,
        file_id: int | None,
        function_handle: FunctionHandle | None,
    ) -> None:
        source_code_options = {
            "folder": folder,
            "file_id": file_id,
            "function_handle": function_handle,
        }
        # TODO: Fix to use exactly_one_is_not_none function
        given_source_code_options = [key for key in source_code_options if source_code_options[key]]
        if not given_source_code_options:
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

            Call activate:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> status = client.functions.activate()
        """
        res = self._post(self._RESOURCE_PATH + "/status")
        return FunctionsStatus.load(res.json())

    def status(self) -> FunctionsStatus:
        """`Functions activation status for the Project. <https://developer.cognite.com/api#tag/Functions/operation/getFunctionsStatus>`_.

        Returns:
            FunctionsStatus: A function activation status.

        Examples:

            Call status:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> status = client.functions.status()
        """
        res = self._get(self._RESOURCE_PATH + "/status")
        return FunctionsStatus.load(res.json())


def get_handle_function_node(file_content: str) -> ast.FunctionDef | ast.Assign | ast.AnnAssign | None:
    """
    Extract the last top-level 'handle' function or variable assignment.

    Returns the last occurrence to handle development workflows where developers
    may keep old versions or add debug functions. Only considers top-level functions
    and assignments since Cognite Functions require directly callable entry points.

    Args:
        file_content (str): The Python source code as a string

    Returns:
        ast.FunctionDef | ast.Assign | ast.AnnAssign | None: The AST node of the last top-level 'handle' function,
        assignment, or None if not found or if the file is not a valid Python file.
    """
    try:
        tree = ast.parse(file_content)
    except SyntaxError:
        return None

    def _target_is_handle(target: ast.expr) -> bool:
        """Check if assignment target is a simple name 'handle'."""
        return isinstance(target, ast.Name) and target.id == "handle"

    return next(
        (
            node
            for node in reversed(tree.body)  # Only look at top-level nodes
            # def handle(...): ...
            if (isinstance(node, ast.FunctionDef) and node.name == "handle")
            # handle = callable
            or (isinstance(node, ast.Assign) and len(node.targets) == 1 and _target_is_handle(node.targets[0]))
            # handle: Callable = callable
            or (isinstance(node, ast.AnnAssign) and node.value is not None and _target_is_handle(node.target))
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
    validator.start()
    validator.join()
    if (error := queue.get_nowait()) is not None:
        raise error


def validate_function_folder(root_path: str, function_path: str, skip_folder_validation: bool) -> None:
    if not function_path.endswith(".py"):
        raise TypeError(f"{function_path} must be a Python file.")

    file_path = Path(root_path, function_path)
    if not file_path.is_file():
        raise FileNotFoundError(f"No file found at '{file_path}'.")

    if node := get_handle_function_node(file_path.read_text()):
        _validate_function_handle(node)
    else:
        raise TypeError(f"{function_path} must contain a function named 'handle'.")

    if not skip_folder_validation:
        module_path = ".".join(Path(function_path).with_suffix("").parts)
        if not _RUNNING_IN_BROWSER:
            # We do an actual import to verify the files (this is done in a separate process)
            _check_imports(root_path, module_path)
        else:
            # Backup method for Pyodide/WASM envs: 'multiprocessing' not available due to browser limitations (ModuleNotFoundError)
            _run_import_check_backup(root_path, module_path)


def _validate_function_handle(
    handle_obj: Callable[..., object] | ast.FunctionDef | ast.Assign | ast.AnnAssign,
) -> None:
    match handle_obj:
        case ast.FunctionDef():
            # Handle functions like: def handle(data, client, secrets): ...
            name = handle_obj.name
            accepts_args = {arg.arg for arg in handle_obj.args.args}
        case ast.Assign():
            # Handle assignments like: handle = some_callable
            target = handle_obj.targets[0]
            if not isinstance(target, ast.Name):
                raise TypeError("Assignment target must be a simple name, not a complex expression.")
            name = target.id
            accepts_args = set()  # Callable variables are expected to do validation at runtime
        case ast.AnnAssign():
            # Handle annotated assignments like: handle: Callable = some_callable
            if not isinstance(handle_obj.target, ast.Name):
                raise TypeError("Assignment target must be a simple name, not a complex expression.")
            name = handle_obj.target.id
            accepts_args = set()  # Callable variables are expected to do validation at runtime
        case _:
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
    constructors = local_import("pip._internal.req.constructors")
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
    if docstr := getdoc(fn):
        if reqs := _extract_requirements_from_doc_string(docstr):
            return _validate_and_parse_requirements(reqs)
    return []


def _sanitize_filename(filename: str) -> str:
    # Forwardslash, '/', is not allowed in file names:
    return filename.replace("/", "-")
