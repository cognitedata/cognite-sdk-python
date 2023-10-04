from __future__ import annotations

import time
from numbers import Number
from typing import TYPE_CHECKING, cast

from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteResource,
    CogniteResourceList,
    CogniteResponse,
    IdTransformerMixin,
)
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.utils._auxiliary import is_unlimited
from cognite.client.utils._time import ms_to_datetime

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class Function(CogniteResource):
    """A representation of a Cognite Function.

    Args:
        id (int | None): Id of the function.
        name (str | None): Name of the function.
        external_id (str | None): External id of the function.
        description (str | None): Description of the function.
        owner (str | None): Owner of the function.
        status (str | None): Status of the function.
        file_id (int | None): File id of the code represented by this object.
        function_path (str | None): Relative path from the root folder to the file containing the `handle` function. Defaults to `handler.py`. Must be on posix path format.
        created_time (int | None): Created time in UNIX.
        secrets (dict | None): Secrets attached to the function ((key, value) pairs).
        env_vars (dict | None): User specified environment variables on the function ((key, value) pairs).
        cpu (Number | None): Number of CPU cores per function. Defaults to 0.25. Allowed values are in the range [0.1, 0.6].
        memory (Number | None): Memory per function measured in GB. Defaults to 1. Allowed values are in the range [0.1, 2.5].
        runtime (str | None): Runtime of the function. Allowed values are ["py38", "py39","py310"]. The runtime "py38" resolves to the latest version of the Python 3.8 series. Will default to "py38" if not specified.
        runtime_version (str | None): The complete specification of the function runtime with major, minor and patch version numbers.
        metadata (dict | None): Metadata associated with a function as a set of key:value pairs.
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
        function_path: str | None = None,
        created_time: int | None = None,
        secrets: dict | None = None,
        env_vars: dict | None = None,
        cpu: Number | None = None,
        memory: Number | None = None,
        runtime: str | None = None,
        runtime_version: str | None = None,
        metadata: dict | None = None,
        error: dict | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.id = cast(int, id)
        self.name = cast(str, name)
        self.external_id = external_id
        self.description = description
        self.owner = owner
        self.status = status
        self.file_id = file_id
        self.function_path = function_path
        self.created_time = created_time
        self.secrets = secrets
        self.env_vars = env_vars
        self.cpu = cpu
        self.memory = memory
        self.runtime = runtime
        self.runtime_version = runtime_version
        self.metadata = metadata
        self.error = error
        self._cognite_client = cast("CogniteClient", cognite_client)

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
        schedules_by_external_id = self._cognite_client.functions.schedules.list(
            function_external_id=self.external_id, limit=limit
        )
        schedules_by_id = self._cognite_client.functions.schedules.list(function_id=self.id, limit=limit)

        if is_unlimited(limit):
            limit = self._cognite_client.functions.schedules._LIST_LIMIT_CEILING

        return (schedules_by_external_id + schedules_by_id)[:limit]

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


class FunctionFilter(CogniteFilter):
    def __init__(
        self,
        name: str | None = None,
        owner: str | None = None,
        file_id: int | None = None,
        status: str | None = None,
        external_id_prefix: str | None = None,
        created_time: dict[str, int] | TimestampRange | None = None,
    ) -> None:
        self.name = name
        self.owner = owner
        self.file_id = file_id
        self.status = status
        self.external_id_prefix = external_id_prefix
        self.created_time = created_time


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


class FunctionSchedule(CogniteResource):
    """A representation of a Cognite Function Schedule.

    Args:
        id (int | None): Id of the schedule.
        name (str | None): Name of the function schedule.
        function_id (str | None): Id of the function.
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
        function_id: str | None = None,
        function_external_id: str | None = None,
        description: str | None = None,
        created_time: int | None = None,
        cron_expression: str | None = None,
        session_id: int | None = None,
        when: str | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.id = id
        self.name = name
        self.function_id = function_id
        self.function_external_id = function_external_id
        self.description = description
        self.cron_expression = cron_expression
        self.created_time = created_time
        self.session_id = session_id
        self.when = when
        self._cognite_client = cast("CogniteClient", cognite_client)

    def get_input_data(self) -> dict | None:
        """
        Retrieve the input data to the associated function.

        Returns:
            dict | None: Input data to the associated function or None if not set. This data is passed deserialized into the function through the data argument.
        """
        if self.id is None:
            raise ValueError("FunctionSchedule is missing 'id'")
        return self._cognite_client.functions.schedules.get_input_data(id=self.id)


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


class FunctionSchedulesList(CogniteResourceList[FunctionSchedule]):
    _RESOURCE = FunctionSchedule


class FunctionList(CogniteResourceList[Function], IdTransformerMixin):
    _RESOURCE = Function


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
        self.id = id
        self.start_time = start_time
        self.end_time = end_time
        self.scheduled_time = scheduled_time
        self.status = status
        self.schedule_id = schedule_id
        self.error = error
        self.function_id = function_id
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
    def _load(cls, api_response: dict) -> FunctionsLimits:
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
    def _load(cls, api_response: dict) -> FunctionsStatus:
        return cls(status=api_response["status"])
