from __future__ import annotations

import time
from abc import ABC
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, cast

from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteResource,
    CogniteResourceList,
    CogniteResponse,
    ExternalIDTransformerMixin,
    IdTransformerMixin,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.utils._time import ms_to_datetime

if TYPE_CHECKING:
    from cognite.client import CogniteClient

RunTime: TypeAlias = Literal["py38", "py39", "py310", "py311"]
FunctionStatus: TypeAlias = Literal["Queued", "Deploying", "Ready", "Failed"]
HANDLER_FILE_NAME = "handler.py"


class FunctionCore(WriteableCogniteResource["FunctionWrite"], ABC):
    """A representation of a Cognite Function.

    Args:
        name (str | None): Name of the function.
        external_id (str | None): External id of the function.
        description (str | None): Description of the function.
        owner (str | None): Owner of the function.
        file_id (int | None): File id of the code represented by this object.
        function_path (str): Relative path from the root folder to the file containing the `handle` function. Defaults to `handler.py`. Must be on posix path format.
        secrets (dict[str, str] | None): Secrets attached to the function ((key, value) pairs).
        env_vars (dict[str, str] | None): User specified environment variables on the function ((key, value) pairs).
        cpu (float | None): Number of CPU cores per function. Allowed range and default value are given by the `limits endpoint. <https://developer.cognite.com/api#tag/Functions/operation/functionsLimits>`_, and None translates to the API default. On Azure, only the default value is used.
        memory (float | None): Memory per function measured in GB. Allowed range and default value are given by the `limits endpoint. <https://developer.cognite.com/api#tag/Functions/operation/functionsLimits>`_, and None translates to the API default. On Azure, only the default value is used.
        runtime (str | None): Runtime of the function. Allowed values are ["py38", "py39","py310", "py311"]. The runtime "py38" resolves to the latest version of the Python 3.8 series.
        metadata (dict[str, str] | None): Metadata associated with a function as a set of key:value pairs.
    """

    def __init__(
        self,
        name: str | None = None,
        external_id: str | None = None,
        description: str | None = None,
        owner: str | None = None,
        file_id: int | None = None,
        function_path: str = HANDLER_FILE_NAME,
        secrets: dict[str, str] | None = None,
        env_vars: dict[str, str] | None = None,
        cpu: float | None = None,
        memory: float | None = None,
        runtime: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> None:
        # name/file_id are required when using the class to read,
        # but don't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        # TODO: In the next major version we can make these properties required in the constructor
        self.name: str = name  # type: ignore
        self.file_id: int = file_id  # type: ignore
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
    This is the reading version, which is used when retrieving a function.

    Args:
        id (int | None): ID of the function.
        name (str | None): Name of the function.
        external_id (str | None): External id of the function.
        description (str | None): Description of the function.
        owner (str | None): Owner of the function.
        status (str | None): Status of the function.
        file_id (int | None): File id of the code represented by this object.
        function_path (str): Relative path from the root folder to the file containing the `handle` function. Defaults to `handler.py`. Must be on posix path format.
        created_time (int | None): Created time in UNIX.
        secrets (dict[str, str] | None): Secrets attached to the function ((key, value) pairs).
        env_vars (dict[str, str] | None): User specified environment variables on the function ((key, value) pairs).
        cpu (float | None): Number of CPU cores per function. Allowed range and default value are given by the `limits endpoint. <https://developer.cognite.com/api#tag/Functions/operation/functionsLimits>`_, and None translates to the API default. On Azure, only the default value is used.
        memory (float | None): Memory per function measured in GB. Allowed range and default value are given by the `limits endpoint. <https://developer.cognite.com/api#tag/Functions/operation/functionsLimits>`_, and None translates to the API default. On Azure, only the default value is used.
        runtime (str | None): Runtime of the function. Allowed values are ["py38", "py39","py310", "py311"]. The runtime "py38" resolves to the latest version of the Python 3.8 series.
        runtime_version (str | None): The complete specification of the function runtime with major, minor and patch version numbers.
        metadata (dict[str, str] | None): Metadata associated with a function as a set of key:value pairs.
        error (dict | None): Dictionary with keys "message" and "trace", which is populated if deployment fails.
        cognite_client (CogniteClient | None): An optional CogniteClient to associate with this data class.
    """

    def __init__(
        self,
        id: int | None = None,
        name: str | None = None,
        external_id: str | None = None,
        description: str | None = None,
        owner: str | None = None,
        status: str | None = None,
        file_id: int | None = None,
        function_path: str = HANDLER_FILE_NAME,
        created_time: int | None = None,
        secrets: dict[str, str] | None = None,
        env_vars: dict[str, str] | None = None,
        cpu: float | None = None,
        memory: float | None = None,
        runtime: str | None = None,
        runtime_version: str | None = None,
        metadata: dict[str, str] | None = None,
        error: dict | None = None,
        cognite_client: CogniteClient | None = None,
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
        # id/created_time/status are required when using the class to read,
        # but don't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        # TODO: In the next major version we can make these properties required in the constructor
        self.id: int = id  # type: ignore
        self.created_time: int = created_time  # type: ignore
        self.status: str = status  # type: ignore
        self.runtime_version = runtime_version
        self.error = error
        self._cognite_client = cast("CogniteClient", cognite_client)

    def as_write(self) -> FunctionWrite:
        """Returns a writeable version of this function."""
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
            runtime=cast(RunTime, self.runtime),
            metadata=self.metadata,
        )

    def call(self, data: dict | None = None, wait: bool = True) -> FunctionCall:
        """`Call this particular function. <https://docs.cognite.com/api/v1/#operation/postFunctionsCall>`_

        Args:
            data (dict | None): Input data to the function (JSON serializable). This data is passed deserialized into the function through one of the arguments called data. **WARNING:** Secrets or other confidential information should not be passed via this argument. There is a dedicated `secrets` argument in FunctionsAPI.create() for this purpose.
            wait (bool): Wait until the function call is finished. Defaults to True.

        Returns:
            FunctionCall: A function call object.
        """
        return self._cognite_client.functions.call(id=self.id, data=data, wait=wait)

    def list_calls(
        self,
        status: str | None = None,
        schedule_id: int | None = None,
        start_time: dict[str, int] | None = None,
        end_time: dict[str, int] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> FunctionCallList:
        """List all calls to this function.

        Args:
            status (str | None): Status of the call. Possible values ["Running", "Failed", "Completed", "Timeout"].
            schedule_id (int | None): Schedule id from which the call belongs (if any).
            start_time (dict[str, int] | None): Start time of the call. Possible keys are `min` and `max`, with values given as time stamps in ms.
            end_time (dict[str, int] | None): End time of the call. Possible keys are `min` and `max`, with values given as time stamps in ms.
            limit (int | None): Maximum number of function calls to list. Pass in -1, float('inf') or None to list all Function Calls.

        Returns:
            FunctionCallList: List of function calls
        """
        return self._cognite_client.functions.calls.list(
            function_id=self.id,
            status=status,
            schedule_id=schedule_id,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )

    def list_schedules(self, limit: int | None = DEFAULT_LIMIT_READ) -> FunctionSchedulesList:
        """`List all schedules associated with this function. <https://docs.cognite.com/api/v1/#operation/getFunctionSchedules>`_

        Args:
            limit (int | None): Maximum number of schedules to list. Pass in -1, float('inf') or None to list all.

        Returns:
            FunctionSchedulesList: List of function schedules
        """
        return self._cognite_client.functions.schedules.list(function_id=self.id, limit=limit)

    def retrieve_call(self, id: int) -> FunctionCall | None:
        """`Retrieve call by id. <https://docs.cognite.com/api/v1/#operation/getFunctionCall>`_

        Args:
            id (int): ID of the call.

        Returns:
            FunctionCall | None: Requested function call or None if not found.
        """
        return self._cognite_client.functions.calls.retrieve(call_id=id, function_id=self.id)

    def update(self) -> None:
        """Update the function object. Can be useful to check for the latet status of the function ('Queued', 'Deploying', 'Ready' or 'Failed')."""
        latest = self._cognite_client.functions.retrieve(id=self.id)
        if latest is None:
            return None

        for attribute in self.__dict__:
            if attribute.startswith("_"):
                continue
            latest_value = getattr(latest, attribute)
            setattr(self, attribute, latest_value)


class FunctionWrite(FunctionCore):
    """A representation of a Cognite Function.
    This is the writing version, which is used when creating a function.

    Args:
        name (str): Name of the function.
        file_id (int): File id of the code represented by this object.
        external_id (str | None): External id of the function.
        description (str | None): Description of the function.
        owner (str | None): Owner of the function.
        function_path (str): Relative path from the root folder to the file containing the `handle` function. Defaults to `handler.py`. Must be on posix path format.
        secrets (dict[str, str] | None): Secrets attached to the function ((key, value) pairs).
        env_vars (dict[str, str] | None): User specified environment variables on the function ((key, value) pairs).
        cpu (float | None): Number of CPU cores per function. Allowed range and default value are given by the `limits endpoint. <https://developer.cognite.com/api#tag/Functions/operation/functionsLimits>`_, and None translates to the API default. On Azure, only the default value is used.
        memory (float | None): Memory per function measured in GB. Allowed range and default value are given by the `limits endpoint. <https://developer.cognite.com/api#tag/Functions/operation/functionsLimits>`_, and None translates to the API default. On Azure, only the default value is used.
        runtime (RunTime | None): Runtime of the function. Allowed values are ["py38", "py39","py310", "py311"]. The runtime "py38" resolves to the latest version of the Python 3.8 series.
        metadata (dict[str, str] | None): Metadata associated with a function as a set of key:value pairs.
        index_url (str | None): Specify a different python package index, allowing for packages published in private repositories. Supports basic HTTP authentication as described in pip basic authentication. See the documentation for additional information related to the security risks of using this option.
        extra_index_urls (list[str] | None): Extra package index URLs to use when building the function, allowing for packages published in private repositories. Supports basic HTTP authentication as described in pip basic authentication. See the documentation for additional information related to the security risks of using this option.
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
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> FunctionWrite:
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
        """Returns this FunctionWrite instance."""
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


class FunctionScheduleCore(WriteableCogniteResource["FunctionScheduleWrite"], ABC):
    """A representation of a Cognite Function Schedule.

    Args:
        name (str | None): Name of the function schedule.
        function_id (int | None): Id of the function.
        function_external_id (str | None): External id of the function.
        description (str | None): Description of the function schedule.
        cron_expression (str | None): Cron expression
    """

    def __init__(
        self,
        name: str | None = None,
        function_id: int | None = None,
        function_external_id: str | None = None,
        description: str | None = None,
        cron_expression: str | None = None,
    ) -> None:
        # name/function_id is required when using the class to read,
        # but doesn't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        # TODO: In the next major version we can make these properties required in the constructor
        self.name: str = name  # type: ignore
        self.function_id: int = function_id  # type: ignore
        self.function_external_id = function_external_id
        self.description = description
        self.cron_expression: str = cron_expression  # type: ignore


class FunctionSchedule(FunctionScheduleCore):
    """A representation of a Cognite Function Schedule.
    This is the reading version, which is used when retrieving a function schedule.

    Args:
        id (int | None): ID of the schedule.
        name (str | None): Name of the function schedule.
        function_id (int | None): ID of the function.
        function_external_id (str | None): External id of the function.
        description (str | None): Description of the function schedule.
        created_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cron_expression (str | None): Cron expression
        session_id (int | None): ID of the session running with the schedule.
        when (str | None): When the schedule will trigger, in human readable text (server generated from cron_expression).
        cognite_client (CogniteClient | None): An optional CogniteClient to associate with this data class.
    """

    def __init__(
        self,
        id: int | None = None,
        name: str | None = None,
        function_id: int | None = None,
        function_external_id: str | None = None,
        description: str | None = None,
        created_time: int | None = None,
        cron_expression: str | None = None,
        session_id: int | None = None,
        when: str | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(
            name=name,
            function_id=function_id,
            function_external_id=function_external_id,
            description=description,
            cron_expression=cron_expression,
        )
        # id/created_time/when are required when using the class to read,
        # but don't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        # TODO: In the next major version we can make these properties required in the constructor
        self.id: int = id  # type: ignore
        self.created_time: int = created_time  # type: ignore
        self.session_id = session_id
        self.when: str = when  # type: ignore
        self._cognite_client = cast("CogniteClient", cognite_client)

    def as_write(self) -> FunctionScheduleWrite:
        """Returns a writeable version of this function schedule."""
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

    def get_input_data(self) -> dict | None:
        """
        Retrieve the input data to the associated function.

        Returns:
            dict | None: Input data to the associated function or None if not set. This data is passed deserialized into the function through the data argument.
        """
        if self.id is None:
            raise ValueError("FunctionSchedule is missing 'id'")
        return self._cognite_client.functions.schedules.get_input_data(id=self.id)


class FunctionScheduleWrite(FunctionScheduleCore):
    """A representation of a Cognite Function Schedule.

    Args:
        name (str): Name of the function schedule.
        cron_expression (str): Cron expression
        function_id (int | None): ID of the function.
        function_external_id (str | None): External ID of the function.
        description (str | None): Description of the function schedule.
        data (dict | None): Input data to the function (only present if provided on the schedule). This data is passed deserialized into the function through one of the arguments called data. WARNING: Secrets or other confidential information should not be passed via the data object. There is a dedicated secrets object in the request body to "Create functions" for this purpose.
    """

    def __init__(
        self,
        name: str,
        cron_expression: str,
        function_id: int | None = None,
        function_external_id: str | None = None,
        description: str | None = None,
        data: dict | None = None,
    ) -> None:
        super().__init__(
            name=name,
            function_id=function_id,
            function_external_id=function_external_id,
            description=description,
            cron_expression=cron_expression,
        )
        self.data = data

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> FunctionScheduleWrite:
        return cls(
            name=resource["name"],
            function_id=resource.get("functionId"),
            function_external_id=resource.get("functionExternalId"),
            description=resource.get("description"),
            cron_expression=resource["cronExpression"],
            data=resource.get("data"),
        )

    def as_write(self) -> FunctionScheduleWrite:
        """Returns this FunctionScheduleWrite instance."""
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


class FunctionSchedulesList(WriteableCogniteResourceList[FunctionScheduleWrite, FunctionSchedule]):
    _RESOURCE = FunctionSchedule

    def as_write(self) -> FunctionScheduleWriteList:
        """Returns a writeable version of this function schedule."""
        return FunctionScheduleWriteList([f.as_write() for f in self.data], cognite_client=self._get_cognite_client())


class FunctionWriteList(CogniteResourceList[FunctionWrite], ExternalIDTransformerMixin):
    _RESOURCE = FunctionWrite


class FunctionList(WriteableCogniteResourceList[FunctionWrite, Function], IdTransformerMixin):
    _RESOURCE = Function

    def as_write(self) -> FunctionWriteList:
        """Returns a writeable version of this function."""
        return FunctionWriteList([f.as_write() for f in self.data], cognite_client=self._get_cognite_client())


class FunctionCall(CogniteResource):
    """A representation of a Cognite Function call.

    Args:
        id (int | None): A server-generated ID for the object.
        start_time (int | None): Start time of the call, measured in number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        end_time (int | None): End time of the call, measured in number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        scheduled_time (int | None): Scheduled time of the call, measured in number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        status (str | None): Status of the function call ("Running", "Completed" or "Failed").
        schedule_id (int | None): The schedule id belonging to the call.
        error (dict | None): Error from the function call. It contains an error message and the stack trace.
        function_id (int | None): No description.
        cognite_client (CogniteClient | None): An optional CogniteClient to associate with this data class.
    """

    def __init__(
        self,
        id: int | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        scheduled_time: int | None = None,
        status: str | None = None,
        schedule_id: int | None = None,
        error: dict | None = None,
        function_id: int | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        # id/start_time/status/function_id is required when using the class to read,
        # but don't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        # TODO: In the next major version we can make these properties required in the constructor
        self.id: int = id  # type: ignore
        self.start_time: int = start_time  # type: ignore
        self.end_time = end_time
        self.scheduled_time = scheduled_time
        self.status: str = status  # type: ignore
        self.schedule_id = schedule_id
        self.error = error
        self.function_id: int = function_id  # type: ignore
        self._cognite_client = cast("CogniteClient", cognite_client)

    def get_response(self) -> dict | None:
        """Retrieve the response from this function call.

        Returns:
            dict | None: Response from the function call.
        """
        call_id, function_id = self._get_identifiers_or_raise(self.id, self.function_id)
        return self._cognite_client.functions.calls.get_response(call_id=call_id, function_id=function_id)

    def get_logs(self) -> FunctionCallLog:
        """`Retrieve logs for this function call. <https://docs.cognite.com/api/v1/#operation/getFunctionCallLogs>`_

        Returns:
            FunctionCallLog: Log for the function call.
        """
        call_id, function_id = self._get_identifiers_or_raise(self.id, self.function_id)
        return self._cognite_client.functions.calls.get_logs(call_id=call_id, function_id=function_id)

    def update(self) -> None:
        """Update the function call object. Can be useful if the call was made with wait=False."""
        call_id, function_id = self._get_identifiers_or_raise(self.id, self.function_id)
        latest = self._cognite_client.functions.calls.retrieve(call_id=call_id, function_id=function_id)
        if latest is None:
            raise RuntimeError("Unable to update the function call object (it was not found)")
        self.status = latest.status
        self.end_time = latest.end_time
        self.error = latest.error

    @staticmethod
    def _get_identifiers_or_raise(call_id: int | None, function_id: int | None) -> tuple[int, int]:
        # Mostly a mypy thing, but for sure nice with an error message :D
        if call_id is None or function_id is None:
            raise ValueError("FunctionCall is missing one or more of: [id, function_id]")
        return call_id, function_id

    def wait(self) -> None:
        while self.status == "Running":
            self.update()
            time.sleep(1.0)


class FunctionCallList(CogniteResourceList[FunctionCall]):
    _RESOURCE = FunctionCall


class FunctionCallLogEntry(CogniteResource):
    """A log entry for a function call.

    Args:
        timestamp (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        message (str | None): Single line from stdout / stderr.
        cognite_client (CogniteClient | None): No description.
    """

    def __init__(
        self,
        timestamp: int | None = None,
        message: str | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.timestamp = timestamp
        self.message = message
        self._cognite_client = cast("CogniteClient", cognite_client)

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
            with_timestamps (bool): Whether to include entry timestamps in the output. Defaults to False.
        Returns:
            str: new-line delimited log entries.
        """
        return "\n".join(entry._format(with_timestamps) for entry in self)


class FunctionsLimits(CogniteResponse):
    """Service limits for the associated project.

    Args:
        timeout_minutes (int): Timeout of each function call.
        cpu_cores (dict[str, float]): The number of CPU cores per function execution (i.e. function call).
        memory_gb (dict[str, float]): The amount of available memory in GB per function execution (i.e. function call).
        runtimes (list[str]): Available runtimes. For example, "py39" translates to the latest version of the Python 3.9.x series.
        response_size_mb (int | None): Maximum response size of function calls.
    """

    def __init__(
        self,
        timeout_minutes: int,
        cpu_cores: dict[str, float],
        memory_gb: dict[str, float],
        runtimes: list[str],
        response_size_mb: int | None = None,
    ) -> None:
        self.timeout_minutes = timeout_minutes
        self.cpu_cores = cpu_cores
        self.memory_gb = memory_gb
        self.runtimes = runtimes
        self.response_size_mb = response_size_mb

    @classmethod
    def load(cls, api_response: dict) -> FunctionsLimits:
        return cls(
            timeout_minutes=api_response["timeoutMinutes"],
            cpu_cores=api_response["cpuCores"],
            memory_gb=api_response["memoryGb"],
            runtimes=api_response["runtimes"],
            response_size_mb=api_response.get("responseSizeMb"),
        )


class FunctionsStatus(CogniteResponse):
    """Activation Status for the associated project.

    Args:
        status (str): Activation Status for the associated project.
    """

    def __init__(self, status: str) -> None:
        self.status = status

    @classmethod
    def load(cls, api_response: dict) -> FunctionsStatus:
        return cls(status=api_response["status"])
