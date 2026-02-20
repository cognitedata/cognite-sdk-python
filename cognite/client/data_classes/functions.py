from __future__ import annotations

import asyncio
from abc import ABC
from typing import TYPE_CHECKING, Any, Literal, Protocol, TypeAlias

from typing_extensions import Self

from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteResource,
    CogniteResourceList,
    CogniteResourceWithClientRef,
    ExternalIDTransformerMixin,
    IdTransformerMixin,
    InternalIdTransformerMixin,
    WriteableCogniteResourceList,
    WriteableCogniteResourceWithClientRef,
)
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.utils._async_helpers import run_sync
from cognite.client.utils._retry import Backoff
from cognite.client.utils._text import copy_doc_from_async
from cognite.client.utils._time import ms_to_datetime

if TYPE_CHECKING:
    from cognite.client import CogniteClient

RunTime: TypeAlias = Literal["py310", "py311", "py312"]
FunctionStatus: TypeAlias = Literal["Queued", "Deploying", "Ready", "Failed"]
HANDLER_FILE_NAME = "handler.py"


class FunctionHandle(Protocol):
    """The function handle.

    This is the function that will be called when the function is executed. The function
    must be named "handle" and can take any of the following named only arguments:

    Args:
        client: Cognite client.
        data: Input data to the function.
        secrets: Secrets passed to the function.
        function_call_info: Function call information.

    Example:
        .. code-block:: python

            def handle(
                client: CogniteClient | None = None,
                data: dict[str, object] | None = None,
            ) -> object:
                # Do something with the data
                return {"result": "success"}

    :
        Return value of the function. Any JSON serializable object is allowed.
    """

    async def __call__(
        self,
        *,
        # TODO(haakonvt): change to CogniteClient | AsyncCogniteClient when/if functions start supporting it
        client: CogniteClient | None = None,
        data: dict[str, object] | None = None,
        secrets: dict[str, str] | None = None,
        function_call_info: dict[str, object] | None = None,
    ) -> object:
        """Function handle protocol.

        Args:
            client: Cognite client.
            data: Input data to the function.
            secrets: Secrets passed to the function.
            function_call_info: Function call information.

        Returns:
            Return value of the function. Any JSON serializable object is allowed.
        """
        ...


class FunctionCore(WriteableCogniteResourceWithClientRef["FunctionWrite"], ABC):
    """A representation of a Cognite Function.

    Args:
        name: Name of the function.
        external_id: External id of the function.
        description: Description of the function.
        owner: Owner of the function.
        file_id: File id of the code represented by this object.
        function_path: Relative path from the root folder to the file containing the `handle` function. Defaults to `handler.py`. Must be on posix path format.
        secrets: Secrets attached to the function ((key, value) pairs).
        env_vars: User specified environment variables on the function ((key, value) pairs).
        cpu: Number of CPU cores per function. Allowed range and default value are given by the `limits endpoint. <https://developer.cognite.com/api#tag/Functions/operation/functionsLimits>`_, and None translates to the API default. On Azure, only the default value is used.
        memory: Memory per function measured in GB. Allowed range and default value are given by the `limits endpoint. <https://developer.cognite.com/api#tag/Functions/operation/functionsLimits>`_, and None translates to the API default. On Azure, only the default value is used.
        runtime: Runtime of the function. Allowed values are ["py310", "py311", "py312"]. The runtime "py312" resolves to the latest version of the Python 3.12 series.
        metadata: Metadata associated with a function as a set of key:value pairs.
    """

    def __init__(
        self,
        name: str,
        external_id: str | None,
        description: str | None,
        owner: str | None,
        file_id: int,
        function_path: str,
        secrets: dict[str, str] | None,
        env_vars: dict[str, str] | None,
        cpu: float | None,
        memory: float | None,
        runtime: RunTime | None,
        metadata: dict[str, str] | None,
    ) -> None:
        self.name = name
        self.file_id = file_id
        self.external_id = external_id
        self.description = description
        self.owner = owner
        self.function_path = function_path
        self.secrets = secrets
        self.env_vars = env_vars
        self.cpu = cpu
        self.memory = memory
        self.runtime = runtime
        self.metadata = metadata


class Function(FunctionCore):
    """A representation of a Cognite Function.
    This is the read version, which is used when retrieving a function.

    Args:
        id: ID of the function.
        created_time: Created time in UNIX.
        name: Name of the function.
        external_id: External id of the function.
        description: Description of the function.
        owner: Owner of the function.
        status: Status of the function.
        file_id: File id of the code represented by this object.
        function_path: Relative path from the root folder to the file containing the `handle` function. Defaults to `handler.py`. Must be on posix path format.
        secrets: Secrets attached to the function ((key, value) pairs).
        env_vars: User specified environment variables on the function ((key, value) pairs).
        cpu: Number of CPU cores per function. Allowed range and default value are given by the `limits endpoint. <https://developer.cognite.com/api#tag/Functions/operation/functionsLimits>`_, and None translates to the API default. On Azure, only the default value is used.
        memory: Memory per function measured in GB. Allowed range and default value are given by the `limits endpoint. <https://developer.cognite.com/api#tag/Functions/operation/functionsLimits>`_, and None translates to the API default. On Azure, only the default value is used.
        runtime: Runtime of the function. Allowed values are ["py310", "py311", "py312"]. The runtime "py312" resolves to the latest version of the Python 3.12 series.
        runtime_version: The complete specification of the function runtime with major, minor and patch version numbers.
        metadata: Metadata associated with a function as a set of key:value pairs.
        error: Dictionary with keys "message" and "trace", which is populated if deployment fails.
        last_called: Last time the function was called, in UNIX timestamp milliseconds.
    """

    def __init__(
        self,
        id: int,
        created_time: int,
        name: str,
        external_id: str | None,
        description: str | None,
        owner: str | None,
        status: str,
        file_id: int,
        function_path: str,
        secrets: dict[str, str] | None,
        env_vars: dict[str, str] | None,
        cpu: float | None,
        memory: float | None,
        runtime: RunTime | None,
        runtime_version: str | None,
        metadata: dict[str, str] | None,
        error: dict | None,
        last_called: int | None,
    ) -> None:
        super().__init__(
            name=name,
            external_id=external_id,
            description=description,
            owner=owner,
            file_id=file_id,
            function_path=function_path,
            secrets=secrets,
            env_vars=env_vars,
            cpu=cpu,
            memory=memory,
            runtime=runtime,
            metadata=metadata,
        )
        self.id = id
        self.created_time = created_time
        self.status = status
        self.runtime_version = runtime_version
        self.error = error
        self.last_called = last_called

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            id=resource["id"],
            created_time=resource["createdTime"],
            name=resource["name"],
            external_id=resource.get("externalId"),
            description=resource.get("description"),
            owner=resource.get("owner"),
            status=resource["status"],
            file_id=resource["fileId"],
            function_path=resource.get("functionPath", HANDLER_FILE_NAME),
            secrets=resource.get("secrets"),
            env_vars=resource.get("envVars"),
            cpu=resource.get("cpu"),
            memory=resource.get("memory"),
            runtime=resource.get("runtime"),
            runtime_version=resource.get("runtimeVersion"),
            metadata=resource.get("metadata"),
            error=resource.get("error"),
            last_called=resource.get("lastCalled"),
        )

    def as_write(self) -> FunctionWrite:
        """a writeable version of this function."""
        if self.file_id is None or self.name is None:
            raise ValueError("file_id and name are required to create a function")
        return FunctionWrite(
            name=self.name,
            external_id=self.external_id,
            description=self.description,
            owner=self.owner,
            file_id=self.file_id,
            function_path=self.function_path,
            secrets=self.secrets,
            env_vars=self.env_vars,
            cpu=self.cpu,
            memory=self.memory,
            runtime=self.runtime,
            metadata=self.metadata,
        )

    async def call_async(self, data: dict[str, object] | None = None, wait: bool = True) -> FunctionCall:
        """`Call this particular function. <https://docs.cognite.com/api/v1/#operation/postFunctionsCall>`_

        Args:
            data: Input data to the function (JSON serializable). This data is passed deserialized into the function through one of the arguments called data. **WARNING:** Secrets or other confidential information should not be passed via this argument. There is a dedicated `secrets` argument in FunctionsAPI.create() for this purpose.
            wait: Wait until the function call is finished. Defaults to True.

        Returns:
            A function call object.
        """
        return await self._cognite_client.functions.call(id=self.id, data=data, wait=wait)

    @copy_doc_from_async(call_async)
    def call(self, data: dict[str, object] | None = None, wait: bool = True) -> FunctionCall:
        return run_sync(self.call_async(data=data, wait=wait))

    async def list_calls_async(
        self,
        status: str | None = None,
        schedule_id: int | None = None,
        start_time: dict[str, int] | None = None,
        end_time: dict[str, int] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> FunctionCallList:
        """List all calls to this function.

        Args:
            status: Status of the call. Possible values ["Running", "Failed", "Completed", "Timeout"].
            schedule_id: Schedule id from which the call belongs (if any).
            start_time: Start time of the call. Possible keys are `min` and `max`, with values given as time stamps in ms.
            end_time: End time of the call. Possible keys are `min` and `max`, with values given as time stamps in ms.
            limit: Maximum number of function calls to list. Pass in -1, float('inf') or None to list all Function Calls.

        Returns:
            List of function calls
        """
        return await self._cognite_client.functions.calls.list(
            function_id=self.id,
            status=status,
            schedule_id=schedule_id,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )

    @copy_doc_from_async(list_calls_async)
    def list_calls(
        self,
        status: str | None = None,
        schedule_id: int | None = None,
        start_time: dict[str, int] | None = None,
        end_time: dict[str, int] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> FunctionCallList:
        return run_sync(
            self.list_calls_async(
                status=status,
                schedule_id=schedule_id,
                start_time=start_time,
                end_time=end_time,
                limit=limit,
            )
        )

    async def list_schedules_async(self, limit: int | None = DEFAULT_LIMIT_READ) -> FunctionSchedulesList:
        """`List all schedules associated with this function. <https://docs.cognite.com/api/v1/#operation/getFunctionSchedules>`_

        Args:
            limit: Maximum number of schedules to list. Pass in -1, float('inf') or None to list all.

        Returns:
            List of function schedules
        """
        return await self._cognite_client.functions.schedules.list(function_id=self.id, limit=limit)

    @copy_doc_from_async(list_schedules_async)
    def list_schedules(self, limit: int | None = DEFAULT_LIMIT_READ) -> FunctionSchedulesList:
        return run_sync(self.list_schedules_async(limit=limit))

    async def retrieve_call_async(self, id: int) -> FunctionCall | None:
        """`Retrieve call by id. <https://docs.cognite.com/api/v1/#operation/getFunctionCall>`_

        Args:
            id: ID of the call.

        Returns:
            Requested function call or None if not found.
        """
        return await self._cognite_client.functions.calls.retrieve(call_id=id, function_id=self.id)

    @copy_doc_from_async(retrieve_call_async)
    def retrieve_call(self, id: int) -> FunctionCall | None:
        return run_sync(self.retrieve_call_async(id=id))

    async def update_async(self) -> None:
        """Update the function object. Can be useful to check for the latest status of the function ('Queued', 'Deploying', 'Ready' or 'Failed')."""
        latest = await self._cognite_client.functions.retrieve(id=self.id)
        if latest is None:
            return None

        for attribute in self.__dict__:
            if attribute.startswith("_"):
                continue
            latest_value = getattr(latest, attribute)
            setattr(self, attribute, latest_value)

    @copy_doc_from_async(update_async)
    def update(self) -> None:
        return run_sync(self.update_async())


class FunctionWrite(FunctionCore):
    """A representation of a Cognite Function.
    This is the write version, which is used when creating a function.

    Args:
        name: Name of the function.
        file_id: File id of the code represented by this object.
        external_id: External id of the function.
        description: Description of the function.
        owner: Owner of the function.
        function_path: Relative path from the root folder to the file containing the `handle` function. Defaults to `handler.py`. Must be on posix path format.
        secrets: Secrets attached to the function ((key, value) pairs).
        env_vars: User specified environment variables on the function ((key, value) pairs).
        cpu: Number of CPU cores per function. Allowed range and default value are given by the `limits endpoint. <https://developer.cognite.com/api#tag/Functions/operation/functionsLimits>`_, and None translates to the API default. On Azure, only the default value is used.
        memory: Memory per function measured in GB. Allowed range and default value are given by the `limits endpoint. <https://developer.cognite.com/api#tag/Functions/operation/functionsLimits>`_, and None translates to the API default. On Azure, only the default value is used.
        runtime: Runtime of the function. Allowed values are ["py310", "py311", "py312"]. The runtime "py312" resolves to the latest version of the Python 3.12 series.
        metadata: Metadata associated with a function as a set of key:value pairs.
        index_url: Specify a different python package index, allowing for packages published in private repositories. Supports basic HTTP authentication as described in pip basic authentication. See the documentation for additional information related to the security risks of using this option.
        extra_index_urls: Extra package index URLs to use when building the function, allowing for packages published in private repositories. Supports basic HTTP authentication as described in pip basic authentication. See the documentation for additional information related to the security risks of using this option.
    """

    def __init__(
        self,
        name: str,
        file_id: int,
        external_id: str | None = None,
        description: str | None = None,
        owner: str | None = None,
        function_path: str = HANDLER_FILE_NAME,
        secrets: dict[str, str] | None = None,
        env_vars: dict[str, str] | None = None,
        cpu: float | None = None,
        memory: float | None = None,
        runtime: RunTime | None = None,
        metadata: dict[str, str] | None = None,
        index_url: str | None = None,
        extra_index_urls: list[str] | None = None,
    ) -> None:
        super().__init__(
            name=name,
            external_id=external_id,
            description=description,
            owner=owner,
            file_id=file_id,
            function_path=function_path,
            secrets=secrets,
            env_vars=env_vars,
            cpu=cpu,
            memory=memory,
            runtime=runtime,
            metadata=metadata,
        )
        self.index_url = index_url
        self.extra_index_urls = extra_index_urls

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> FunctionWrite:
        return cls(
            name=resource["name"],
            external_id=resource.get("externalId"),
            description=resource.get("description"),
            owner=resource.get("owner"),
            file_id=resource["fileId"],
            function_path=resource.get("functionPath", HANDLER_FILE_NAME),
            secrets=resource.get("secrets"),
            env_vars=resource.get("envVars"),
            cpu=resource.get("cpu"),
            memory=resource.get("memory"),
            runtime=resource.get("runtime"),
            metadata=resource.get("metadata"),
            index_url=resource.get("indexUrl"),
            extra_index_urls=resource.get("extraIndexUrls"),
        )

    def as_write(self) -> FunctionWrite:
        """this FunctionWrite instance."""
        return self


class FunctionFilter(CogniteFilter):
    def __init__(
        self,
        name: str | None = None,
        owner: str | None = None,
        file_id: int | None = None,
        status: FunctionStatus | None = None,
        external_id_prefix: str | None = None,
        created_time: dict[Literal["min", "max"], int] | TimestampRange | None = None,
        metadata: dict[str, str] | None = None,
    ) -> None:
        self.name = name
        self.owner = owner
        self.file_id = file_id
        self.status = status
        self.external_id_prefix = external_id_prefix
        self.created_time = created_time
        self.metadata = metadata


class FunctionCallsFilter(CogniteFilter):
    def __init__(
        self,
        status: str | None = None,
        schedule_id: int | None = None,
        start_time: dict[str, int] | TimestampRange | None = None,
        end_time: dict[str, int] | TimestampRange | None = None,
    ) -> None:
        self.status = status
        self.schedule_id = schedule_id
        self.start_time = start_time
        self.end_time = end_time


class FunctionScheduleCore(WriteableCogniteResourceWithClientRef["FunctionScheduleWrite"], ABC):
    """A representation of a Cognite Function Schedule.

    Args:
        name: Name of the function schedule.
        function_id: Id of the function.
        function_external_id: External id of the function.
        description: Description of the function schedule.
        cron_expression: Cron expression
    """

    def __init__(
        self,
        name: str,
        function_id: int | None,
        function_external_id: str | None,
        description: str | None,
        cron_expression: str,
    ) -> None:
        self.name = name
        self.function_id = function_id
        self.function_external_id = function_external_id
        self.description = description
        self.cron_expression = cron_expression


class FunctionSchedule(FunctionScheduleCore):
    """A representation of a Cognite Function Schedule.
    This is the read version, which is used when retrieving a function schedule.

    Args:
        id: ID of the schedule.
        name: Name of the function schedule.
        function_id: ID of the function.
        function_external_id: External id of the function.
        description: Description of the function schedule.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cron_expression: Cron expression
        session_id: ID of the session running with the schedule.
        when: When the schedule will trigger, in human readable text (server generated from cron_expression).
    """

    def __init__(
        self,
        id: int,
        name: str,
        function_id: int | None,
        function_external_id: str | None,
        description: str | None,
        created_time: int,
        cron_expression: str,
        session_id: int,
        when: str,
    ) -> None:
        super().__init__(
            name=name,
            function_id=function_id,
            function_external_id=function_external_id,
            description=description,
            cron_expression=cron_expression,
        )
        self.id = id
        self.created_time = created_time
        self.session_id = session_id
        self.when = when

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            id=resource["id"],
            name=resource["name"],
            function_id=resource.get("functionId"),
            function_external_id=resource.get("functionExternalId"),
            description=resource.get("description"),
            created_time=resource["createdTime"],
            cron_expression=resource["cronExpression"],
            session_id=resource["sessionId"],
            when=resource["when"],
        )

    def as_write(self) -> FunctionScheduleWrite:
        """a writeable version of this function schedule."""
        if self.cron_expression is None or self.name is None:
            raise ValueError("cron_expression or name are required to create a FunctionSchedule")

        return FunctionScheduleWrite(
            name=self.name,
            cron_expression=self.cron_expression,
            function_id=self.function_id,
            function_external_id=self.function_external_id,
            description=self.description,
            data=self.get_input_data(),
        )

    async def get_input_data_async(self) -> dict | None:
        """
        Retrieve the input data to the associated function.

        :
            Input data to the associated function or None if not set. This data is passed deserialized into the function through the data argument.
        """
        if self.id is None:
            raise ValueError("FunctionSchedule is missing 'id'")
        return await self._cognite_client.functions.schedules.get_input_data(id=self.id)

    @copy_doc_from_async(get_input_data_async)
    def get_input_data(self) -> dict | None:
        return run_sync(self.get_input_data_async())


class FunctionScheduleWrite(FunctionScheduleCore):
    """A representation of a Cognite Function Schedule.

    Args:
        name: Name of the function schedule.
        cron_expression: Cron expression
        function_id: ID of the function.
        function_external_id: External ID of the function.
        description: Description of the function schedule.
        data: Input data to the function (only present if provided on the schedule). This data is passed deserialized into the function through one of the arguments called data. WARNING: Secrets or other confidential information should not be passed via the data object. There is a dedicated secrets object in the request body to "Create functions" for this purpose.
        nonce: Nonce retrieved from sessions API when creating a session. This will be used to bind the session before executing the function. The corresponding access token will be passed to the function and used to instantiate the client of the handle() function. You can create a session via the Sessions API.
    """

    def __init__(
        self,
        name: str,
        cron_expression: str,
        function_id: int | None = None,
        function_external_id: str | None = None,
        description: str | None = None,
        data: dict | None = None,
        nonce: str | None = None,
    ) -> None:
        super().__init__(
            name=name,
            function_external_id=function_external_id,
            description=description,
            cron_expression=cron_expression,
            function_id=function_id,
        )
        self.function_id = function_id
        self.data = data
        self.nonce = nonce

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> FunctionScheduleWrite:
        return cls(
            name=resource["name"],
            function_id=resource.get("functionId"),
            function_external_id=resource.get("functionExternalId"),
            description=resource.get("description"),
            cron_expression=resource["cronExpression"],
            data=resource.get("data"),
            nonce=resource.get("nonce"),
        )

    def as_write(self) -> FunctionScheduleWrite:
        """this FunctionScheduleWrite instance."""
        return self


class FunctionSchedulesFilter(CogniteFilter):
    def __init__(
        self,
        name: str | None = None,
        function_id: int | None = None,
        function_external_id: str | None = None,
        created_time: dict[str, int] | TimestampRange | None = None,
        cron_expression: str | None = None,
    ) -> None:
        self.name = name
        self.function_id = function_id
        self.function_external_id = function_external_id
        self.created_time = created_time
        self.cron_expression = cron_expression


class FunctionScheduleWriteList(CogniteResourceList[FunctionScheduleWrite]):
    _RESOURCE = FunctionScheduleWrite


class FunctionSchedulesList(
    WriteableCogniteResourceList[FunctionScheduleWrite, FunctionSchedule],
    InternalIdTransformerMixin,
):
    _RESOURCE = FunctionSchedule

    def as_write(self) -> FunctionScheduleWriteList:
        """a writeable version of this function schedule."""
        return FunctionScheduleWriteList([f.as_write() for f in self.data])


class FunctionWriteList(CogniteResourceList[FunctionWrite], ExternalIDTransformerMixin):
    _RESOURCE = FunctionWrite


class FunctionList(WriteableCogniteResourceList[FunctionWrite, Function], IdTransformerMixin):
    _RESOURCE = Function

    def as_write(self) -> FunctionWriteList:
        """a writeable version of this function."""
        return FunctionWriteList([f.as_write() for f in self.data])


class FunctionCall(CogniteResourceWithClientRef):
    """A representation of a Cognite Function call.

    Args:
        id: A server-generated ID for the object.
        start_time: Start time of the call, measured in number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        end_time: End time of the call, measured in number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        scheduled_time: Scheduled time of the call, measured in number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        status: Status of the function call ("Running", "Completed" or "Failed").
        schedule_id: The schedule id belonging to the call.
        error: Error from the function call. It contains an error message and the stack trace.
        function_id: No description.
    """

    def __init__(
        self,
        id: int,
        start_time: int,
        end_time: int | None,
        scheduled_time: int | None,
        status: str,
        schedule_id: int | None,
        error: dict | None,
        function_id: int,
    ) -> None:
        self.id = id
        self.start_time = start_time
        self.end_time = end_time
        self.scheduled_time = scheduled_time
        self.status = status
        self.schedule_id = schedule_id
        self.error = error
        self.function_id = function_id

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            id=resource["id"],
            start_time=resource["startTime"],
            end_time=resource.get("endTime"),
            scheduled_time=resource.get("scheduledTime"),
            status=resource["status"],
            schedule_id=resource.get("scheduleId"),
            error=resource.get("error"),
            function_id=resource["functionId"],
        )

    async def get_response_async(self) -> dict[str, object] | None:
        """Retrieve the response from this function call.

        :
            Response from the function call.
        """
        call_id, function_id = self._get_identifiers_or_raise(self.id, self.function_id)
        return await self._cognite_client.functions.calls.get_response(call_id=call_id, function_id=function_id)

    @copy_doc_from_async(get_response_async)
    def get_response(self) -> dict[str, object] | None:
        return run_sync(self.get_response_async())

    async def get_logs_async(self) -> FunctionCallLog:
        """`Retrieve logs for this function call. <https://docs.cognite.com/api/v1/#operation/getFunctionCallLogs>`_

        :
            FunctionCallLog: Log for the function call.
        """
        call_id, function_id = self._get_identifiers_or_raise(self.id, self.function_id)
        return await self._cognite_client.functions.calls.get_logs(call_id=call_id, function_id=function_id)

    @copy_doc_from_async(get_logs_async)
    def get_logs(self) -> FunctionCallLog:
        return run_sync(self.get_logs_async())

    async def update_async(self) -> None:
        """Update the function call object. Can be useful if the call was made with wait=False."""
        call_id, function_id = self._get_identifiers_or_raise(self.id, self.function_id)
        latest = await self._cognite_client.functions.calls.retrieve(call_id=call_id, function_id=function_id)
        if latest is None:
            raise RuntimeError("Unable to update the function call object (it was not found)")

        self.status = latest.status
        self.end_time = latest.end_time
        self.error = latest.error

    @copy_doc_from_async(update_async)
    def update(self) -> None:
        return run_sync(self.update_async())

    @staticmethod
    def _get_identifiers_or_raise(call_id: int | None, function_id: int | None) -> tuple[int, int]:
        # Mostly a mypy thing, but for sure nice with an error message :D
        if call_id is None or function_id is None:
            raise ValueError("FunctionCall is missing one or more of: [id, function_id]")
        return call_id, function_id

    async def wait_async(self) -> None:
        backoff = Backoff(max_wait=10, base=2, multiplier=0.3)
        while self.status == "Running":
            await self.update_async()
            await asyncio.sleep(next(backoff))

    @copy_doc_from_async(wait_async)
    def wait(self) -> None:
        return run_sync(self.wait_async())


class FunctionCallList(CogniteResourceList[FunctionCall], InternalIdTransformerMixin):
    _RESOURCE = FunctionCall


class FunctionCallLogEntry(CogniteResource):
    """A log entry for a function call.

    Args:
        timestamp: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        message: Single line from stdout / stderr.
    """

    def __init__(
        self,
        timestamp: int | None,
        message: str,
    ) -> None:
        self.timestamp = timestamp
        self.message = message

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(timestamp=resource.get("timestamp"), message=resource["message"])

    def _format(self, with_timestamps: bool = False) -> str:
        ts = ""
        if with_timestamps and self.timestamp is not None:
            ts = f"[{ms_to_datetime(self.timestamp)}] "
        return f"{ts}{self.message}"


class FunctionCallLog(CogniteResourceList[FunctionCallLogEntry]):
    """A collection of function call log entries."""

    _RESOURCE = FunctionCallLogEntry

    def to_text(self, with_timestamps: bool = False) -> str:
        """Return a new-line delimited string of the log entry messages, optionally with entry timestamps.

        Args:
            with_timestamps: Whether to include entry timestamps in the output. Defaults to False.
        Returns:
            No description.
        :
            new-line delimited log entries.
        """
        return "\n".join(entry._format(with_timestamps) for entry in self)


class FunctionsLimits(CogniteResource):
    """Service limits for the associated project.

    Args:
        timeout_minutes: Timeout of each function call.
        cpu_cores: The number of CPU cores per function execution (i.e. function call).
        memory_gb: The amount of available memory in GB per function execution (i.e. function call).
        runtimes: Available runtimes. For example, "py312" translates to the latest version of the Python 3.12 series.
        response_size_mb: Maximum response size of function calls.
    """

    def __init__(
        self,
        timeout_minutes: int,
        cpu_cores: dict[str, float],
        memory_gb: dict[str, float],
        runtimes: list[RunTime],
        response_size_mb: int | None = None,
    ) -> None:
        self.timeout_minutes = timeout_minutes
        self.cpu_cores = cpu_cores
        self.memory_gb = memory_gb
        self.runtimes: list[RunTime] = runtimes
        self.response_size_mb = response_size_mb

    @classmethod
    def _load(cls, api_response: dict) -> FunctionsLimits:
        return cls(
            timeout_minutes=api_response["timeoutMinutes"],
            cpu_cores=api_response["cpuCores"],
            memory_gb=api_response["memoryGb"],
            runtimes=api_response["runtimes"],
            response_size_mb=api_response.get("responseSizeMb"),
        )


class FunctionsStatus(CogniteResource):
    """Activation Status for the associated project.

    Args:
        status: Activation Status for the associated project.
    """

    def __init__(self, status: str) -> None:
        self.status = status

    @classmethod
    def _load(cls, api_response: dict) -> FunctionsStatus:
        return cls(status=api_response["status"])
