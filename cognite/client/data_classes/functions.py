import time
from numbers import Number
from typing import TYPE_CHECKING, Dict, List, Optional, Union, cast

from cognite.client._constants import LIST_LIMIT_CEILING, LIST_LIMIT_DEFAULT
from cognite.client.data_classes._base import CogniteFilter, CogniteResource, CogniteResourceList, CogniteResponse
from cognite.client.data_classes.shared import TimestampRange

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class Function(CogniteResource):
    """A representation of a Cognite Function.

    Args:
        id (int): Id of the function.
        name (str): Name of the function.
        external_id (str): External id of the function.
        description (str): Description of the function.
        owner (str): Owner of the function.
        status (str): Status of the function.
        file_id (int): File id of the code represented by this object.
        function_path (str): Relative path from the root folder to the file containing the `handle` function. Defaults to `handler.py`. Must be on posix path format.
        created_time (int): Created time in UNIX.
        api_key (str): Api key attached to the function.
        secrets (Dict[str, str]): Secrets attached to the function ((key, value) pairs).
        env_vars (Dict[str, str]): User specified environment variables on the function ((key, value) pairs).
        cpu (Number): Number of CPU cores per function. Defaults to 0.25. Allowed values are in the range [0.1, 0.6].
        memory (Number): Memory per function measured in GB. Defaults to 1. Allowed values are in the range [0.1, 2.5].
        runtime (str): Runtime of the function. Allowed values are ["py37", "py38", "py39"]. The runtime "py38" resolves to the latest version of the Python 3.8 series. Will default to "py38" if not specified.
        runtime_version (str): The complete specification of the function runtime with major, minor and patch version numbers.
        metadata(Dict[str, str): Metadata associated with a function as a set of key:value pairs.
        error(Dict[str, str]): Dictionary with keys "message" and "trace", which is populated if deployment fails.
        cognite_client (CogniteClient): An optional CogniteClient to associate with this data class.
    """

    def __init__(
        self,
        id: int = None,
        name: str = None,
        external_id: str = None,
        description: str = None,
        owner: str = None,
        status: str = None,
        file_id: int = None,
        function_path: str = None,
        created_time: int = None,
        api_key: str = None,
        secrets: Dict = None,
        env_vars: Dict = None,
        cpu: Number = None,
        memory: Number = None,
        runtime: str = None,
        runtime_version: str = None,
        metadata: Dict = None,
        error: Dict = None,
        cognite_client: "CogniteClient" = None,
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
        self.api_key = api_key
        self.secrets = secrets
        self.env_vars = env_vars
        self.cpu = cpu
        self.memory = memory
        self.runtime = runtime
        self.runtime_version = runtime_version
        self.metadata = metadata
        self.error = error
        self._cognite_client = cast("CogniteClient", cognite_client)

    def call(self, data: Optional[Dict] = None, wait: bool = True) -> "FunctionCall":
        """`Call this particular function. <https://docs.cognite.com/api/v1/#operation/postFunctionsCall>`_

        Args:
            data (Union[str, dict], optional): Input data to the function (JSON serializable). This data is passed deserialized into the function through one of the arguments called data. **WARNING:** Secrets or other confidential information should not be passed via this argument. There is a dedicated `secrets` argument in FunctionsAPI.create() for this purpose.
            wait (bool): Wait until the function call is finished. Defaults to True.

        Returns:
            FunctionCall: A function call object.
        """
        return self._cognite_client.functions.call(id=self.id, data=data, wait=wait)

    def list_calls(
        self,
        status: Optional[str] = None,
        schedule_id: Optional[int] = None,
        start_time: Optional[Dict[str, int]] = None,
        end_time: Optional[Dict[str, int]] = None,
        limit: Optional[int] = LIST_LIMIT_DEFAULT,
    ) -> "FunctionCallList":
        """List all calls to this function.

        Args:
            status (str, optional): Status of the call. Possible values ["Running", "Failed", "Completed", "Timeout"].
            schedule_id (int, optional): Schedule id from which the call belongs (if any).
            start_time ([Dict[str, int], optional): Start time of the call. Possible keys are `min` and `max`, with values given as time stamps in ms.
            end_time (Dict[str, int], optional): End time of the call. Possible keys are `min` and `max`, with values given as time stamps in ms.
            limit (int, optional): Maximum number of function calls to list. Pass in -1, float('inf') or None to list all Function Calls.

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

    def list_schedules(self, limit: Optional[int] = LIST_LIMIT_DEFAULT) -> "FunctionSchedulesList":
        """`List all schedules associated with this function. <https://docs.cognite.com/api/v1/#operation/getFunctionSchedules>`_

        Args:
            limit (int): Maximum number of schedules to list. Pass in -1, float('inf') or None to list all.

        Returns:
            FunctionSchedulesList: List of function schedules
        """
        schedules_by_external_id = self._cognite_client.functions.schedules.list(
            function_external_id=self.external_id, limit=limit
        )
        schedules_by_id = self._cognite_client.functions.schedules.list(function_id=self.id, limit=limit)

        if limit in [float("inf"), -1, None]:
            limit = LIST_LIMIT_CEILING

        return (schedules_by_external_id + schedules_by_id)[:limit]

    def retrieve_call(self, id: int) -> "FunctionCall":
        """`Retrieve call by id. <https://docs.cognite.com/api/v1/#operation/getFunctionCall>`_

        Args:
            id (int): ID of the call.

        Returns:
            FunctionCall: Function call.
        """
        return self._cognite_client.functions.calls.retrieve(call_id=id, function_id=self.id)

    def update(self) -> None:
        """Update the function object. Can be useful to check for the latet status of the function ('Queued', 'Deploying', 'Ready' or 'Failed').

        Returns:
            None
        """
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
        name: str = None,
        owner: str = None,
        file_id: int = None,
        status: str = None,
        external_id_prefix: str = None,
        created_time: Union[Dict[str, int], TimestampRange] = None,
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
        status: str = None,
        schedule_id: int = None,
        start_time: Union[Dict[str, int], TimestampRange] = None,
        end_time: Union[Dict[str, int], TimestampRange] = None,
    ) -> None:
        self.status = status
        self.schedule_id = schedule_id
        self.start_time = start_time
        self.end_time = end_time


class FunctionSchedule(CogniteResource):
    """A representation of a Cognite Function Schedule.

    Args:
        id (int): Id of the schedule.
        name (str): Name of the function schedule.
        function_id (int): Id of the function.
        function_external_id (str): External id of the function.
        description (str): Description of the function schedule.
        cron_expression (str): Cron expression
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        session_id (int): ID of the session running with the schedule.
        cognite_client (CogniteClient): An optional CogniteClient to associate with this data class.
    """

    def __init__(
        self,
        id: int = None,
        name: str = None,
        function_id: str = None,
        function_external_id: str = None,
        description: str = None,
        created_time: int = None,
        cron_expression: str = None,
        session_id: int = None,
        cognite_client: "CogniteClient" = None,
    ) -> None:
        self.id = id
        self.name = name
        self.function_id = function_id
        self.function_external_id = function_external_id
        self.description = description
        self.cron_expression = cron_expression
        self.created_time = created_time
        self.session_id = session_id
        self._cognite_client = cast("CogniteClient", cognite_client)

    def get_input_data(self) -> dict:
        """
        Retrieve the input data to the associated function.

        Returns:
            Input data to the associated function. This data is passed
            deserialized into the function through the data argument.
        """
        return self._cognite_client.functions.schedules.get_input_data(id=self.id)


class FunctionSchedulesFilter(CogniteFilter):
    def __init__(
        self,
        name: str = None,
        function_id: int = None,
        function_external_id: str = None,
        created_time: Union[Dict[str, int], TimestampRange] = None,
        cron_expression: str = None,
    ) -> None:
        self.name = name
        self.function_id = function_id
        self.function_external_id = function_external_id
        self.created_time = created_time
        self.cron_expression = cron_expression


class FunctionSchedulesList(CogniteResourceList):
    _RESOURCE = FunctionSchedule


class FunctionList(CogniteResourceList):
    _RESOURCE = Function


class FunctionCall(CogniteResource):
    """A representation of a Cognite Function call.

    Args:
        id (int): A server-generated ID for the object.
        start_time (int): Start time of the call, measured in number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        end_time (int): End time of the call, measured in number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        scheduled_time (int): Scheduled time of the call, measured in number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        status (str): Status of the function call ("Running", "Completed" or "Failed").
        schedule_id (int): The schedule id belonging to the call.
        error (dict): Error from the function call. It contains an error message and the stack trace.
        cognite_client (CogniteClient): An optional CogniteClient to associate with this data class.
    """

    def __init__(
        self,
        id: int = None,
        start_time: int = None,
        end_time: int = None,
        scheduled_time: int = None,
        status: str = None,
        schedule_id: int = None,
        error: dict = None,
        function_id: int = None,
        cognite_client: "CogniteClient" = None,
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

    def get_response(self) -> Dict:
        """Retrieve the response from this function call.

        Returns:
            Response from the function call.
        """
        return self._cognite_client.functions.calls.get_response(call_id=self.id, function_id=self.function_id)

    def get_logs(self) -> "FunctionCallLog":
        """`Retrieve logs for this function call. <https://docs.cognite.com/api/v1/#operation/getFunctionCallLogs>`_

        Returns:
            FunctionCallLog: Log for the function call.
        """
        return self._cognite_client.functions.calls.get_logs(call_id=self.id, function_id=self.function_id)

    def update(self) -> None:
        """Update the function call object. Can be useful if the call was made with wait=False.

        Returns:
            None
        """
        latest = self._cognite_client.functions.calls.retrieve(call_id=self.id, function_id=self.function_id)
        self.status = latest.status
        self.end_time = latest.end_time
        self.error = latest.error

    def wait(self) -> None:
        while self.status == "Running":
            self.update()
            time.sleep(1.0)


class FunctionCallList(CogniteResourceList):
    _RESOURCE = FunctionCall


class FunctionCallLogEntry(CogniteResource):
    """A log entry for a function call.

    Args:
        timestamp (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        message (str): Single line from stdout / stderr.
    """

    def __init__(
        self,
        timestamp: int = None,
        message: str = None,
        cognite_client: "CogniteClient" = None,
    ):
        self.timestamp = timestamp
        self.message = message
        self._cognite_client = cast("CogniteClient", cognite_client)


class FunctionCallLog(CogniteResourceList):
    _RESOURCE = FunctionCallLogEntry


class FunctionsLimits(CogniteResponse):
    """Service limits for the associated project.

    Args:
        timeout_minutes (int): Timeout of each function call.
        cpu_cores (Dict[str, float]): The number of CPU cores per function exectuion (i.e. function call).
        memory_gb (Dict[str, float]): The amount of available memory in GB per function execution (i.e. function call).
        runtimes (List[str]): Available runtimes. For example, "py37" translates to the latest version of the Python 3.7.x series.
        response_size_mb (Optional[int]): Maximum response size of function calls.
    """

    def __init__(
        self,
        timeout_minutes: int,
        cpu_cores: Dict[str, float],
        memory_gb: Dict[str, float],
        runtimes: List[str],
        response_size_mb: Optional[int] = None,
    ) -> None:
        self.timeout_minutes = timeout_minutes
        self.cpu_cores = cpu_cores
        self.memory_gb = memory_gb
        self.runtimes = runtimes
        self.response_size_mb = response_size_mb

    @classmethod
    def _load(cls, api_response: Dict) -> "FunctionsLimits":
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

    def __init__(
        self,
        status: str,
    ) -> None:
        self.status = status

    @classmethod
    def _load(cls, api_response: Dict) -> "FunctionsStatus":
        return cls(
            status=api_response["status"],
        )
