"""
===============================================================================
7efd8da544029fe31e2540335d50dd97
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api.functions.calls import SyncFunctionCallsAPI
from cognite.client._sync_api.functions.schedules import SyncFunctionSchedulesAPI
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import (
    Function,
    FunctionCall,
    FunctionList,
    FunctionsLimits,
    TimestampRange,
)
from cognite.client.data_classes.functions import (
    HANDLER_FILE_NAME,
    FunctionHandle,
    FunctionsStatus,
    FunctionStatus,
    FunctionWrite,
    RunTime,
)
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncFunctionsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient):
        self.__async_client = async_client
        self.calls = SyncFunctionCallsAPI(async_client)
        self.schedules = SyncFunctionSchedulesAPI(async_client)

    @overload
    def __call__(self, chunk_size: None = None) -> Iterator[Function]: ...

    @overload
    def __call__(self, chunk_size: int) -> Iterator[FunctionList]: ...

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
    ) -> Iterator[Function | FunctionList]:
        """
        Iterate over functions.

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

        Yields:
            Function | FunctionList: An iterator over functions.
        """
        yield from SyncIterator(
            self.__async_client.functions(
                chunk_size=chunk_size,
                name=name,
                owner=owner,
                file_id=file_id,
                status=status,
                external_id_prefix=external_id_prefix,
                created_time=created_time,
                metadata=metadata,
                limit=limit,
            )
        )

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
        """
        `When creating a function, <https://developer.cognite.com/api#tag/Functions/operation/postFunctions>`_
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

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
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
                >>>     '''
                >>>     [requirements]
                >>>     numpy
                >>>     [/requirements]
                >>>     '''
                >>>     pass
                >>>
                >>> function = client.functions.create(name="myfunction", function_handle=handle)

            .. note:
                When using a predefined function object, you can list dependencies between the tags `[requirements]` and `[/requirements]` in the function's docstring.
                The dependencies will be parsed and validated in accordance with requirement format specified in `PEP 508 <https://peps.python.org/pep-0508/>`_.
        """
        return run_sync(
            self.__async_client.functions.create(
                name=name,
                folder=folder,
                file_id=file_id,
                function_path=function_path,
                function_handle=function_handle,
                external_id=external_id,
                description=description,
                owner=owner,
                secrets=secrets,
                env_vars=env_vars,
                cpu=cpu,
                memory=memory,
                runtime=runtime,
                metadata=metadata,
                index_url=index_url,
                extra_index_urls=extra_index_urls,
                skip_folder_validation=skip_folder_validation,
                data_set_id=data_set_id,
            )
        )

    def delete(
        self, id: int | Sequence[int] | None = None, external_id: str | SequenceNotStr[str] | None = None
    ) -> None:
        """
        `Delete one or more functions. <https://developer.cognite.com/api#tag/Functions/operation/deleteFunctions>`_

        Args:
            id (int | Sequence[int] | None): Id or list of ids.
            external_id (str | SequenceNotStr[str] | None): External ID or list of external ids.

        Example:

            Delete functions by id or external id::

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.functions.delete(id=[1,2,3], external_id="function3")
        """
        return run_sync(self.__async_client.functions.delete(id=id, external_id=external_id))

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
        """
        `List all functions. <https://developer.cognite.com/api#tag/Functions/operation/listFunctions>`_

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

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> functions_list = client.functions.list()
        """
        return run_sync(
            self.__async_client.functions.list(
                name=name,
                owner=owner,
                file_id=file_id,
                status=status,
                external_id_prefix=external_id_prefix,
                created_time=created_time,
                metadata=metadata,
                limit=limit,
            )
        )

    def retrieve(self, id: int | None = None, external_id: str | None = None) -> Function | None:
        """
        `Retrieve a single function by id. <https://developer.cognite.com/api#tag/Functions/operation/byIdsFunctions>`_

        Args:
            id (int | None): ID
            external_id (str | None): External ID

        Returns:
            Function | None: Requested function or None if it does not exist.

        Examples:

            Get function by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.functions.retrieve(id=1)

            Get function by external id:

                >>> res = client.functions.retrieve(external_id="abc")
        """
        return run_sync(self.__async_client.functions.retrieve(id=id, external_id=external_id))

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> FunctionList:
        """
        `Retrieve multiple functions by id. <https://developer.cognite.com/api#tag/Functions/operation/byIdsFunctions>`_

        Args:
            ids (Sequence[int] | None): IDs
            external_ids (SequenceNotStr[str] | None): External IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            FunctionList: The requested functions.

        Examples:

            Get function by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.functions.retrieve_multiple(ids=[1, 2, 3])

            Get functions by external id:

                >>> res = client.functions.retrieve_multiple(external_ids=["func1", "func2"])
        """
        return run_sync(
            self.__async_client.functions.retrieve_multiple(
                ids=ids, external_ids=external_ids, ignore_unknown_ids=ignore_unknown_ids
            )
        )

    def call(
        self,
        id: int | None = None,
        external_id: str | None = None,
        data: dict[str, object] | None = None,
        wait: bool = True,
        nonce: str | None = None,
    ) -> FunctionCall:
        """
        `Call a function by its ID or external ID. <https://developer.cognite.com/api#tag/Function-calls/operation/postFunctionsCall>`_.

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

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> call = client.functions.call(id=1)

            Call a function directly on the `Function` object:

                >>> func = client.functions.retrieve(id=1)
                >>> call = func.call()
        """
        return run_sync(
            self.__async_client.functions.call(id=id, external_id=external_id, data=data, wait=wait, nonce=nonce)
        )

    def limits(self) -> FunctionsLimits:
        """
        `Get service limits. <https://developer.cognite.com/api#tag/Functions/operation/functionsLimits>`_.

        Returns:
            FunctionsLimits: A function limits object.

        Examples:

            Call a function by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> limits = client.functions.limits()
        """
        return run_sync(self.__async_client.functions.limits())

    def activate(self) -> FunctionsStatus:
        """
        `Activate functions for the Project. <https://developer.cognite.com/api#tag/Functions/operation/postFunctionsStatus>`_.

        Note:
            May take some time to take effect (hours).

        Returns:
            FunctionsStatus: A function activation status.

        Examples:

            Call activate:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> status = client.functions.activate()
        """
        return run_sync(self.__async_client.functions.activate())

    def status(self) -> FunctionsStatus:
        """
        `Functions activation status for the Project. <https://developer.cognite.com/api#tag/Functions/operation/getFunctionsStatus>`_.

        Returns:
            FunctionsStatus: A function activation status.

        Examples:

            Call status:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> status = client.functions.status()
        """
        return run_sync(self.__async_client.functions.status())
