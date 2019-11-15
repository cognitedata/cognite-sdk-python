from typing import *

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList, CogniteResponse


class Schedule(CogniteResource):
    """A representation of a Schedule in the model hosting environment.

    Args:
        name (str): Name of the schedule.
        model_name (str): The name of the model associated with this schedule.
        description (str): Description of the schedule.
        data_spec (Union[Dict, ScheduleDataSpec]): The data spec for the schedule.
        is_deprecated (bool): Whether or not the model version is deprecated.
        created_time (int): Created time in UNIX.
        metadata (Dict): User-defined metadata about the model.
        args (Dict): Additional arguments passed to the predict routine.
        cognite_client (CogniteClient): An optional CogniteClient to associate with this data class.
    """

    def __init__(
        self,
        name: str = None,
        model_name: str = None,
        description: str = None,
        data_spec: Union["ScheduleDataSpec", Dict] = None,
        is_deprecated: bool = None,
        created_time: int = None,
        metadata: Dict = None,
        args: Dict = None,
        cognite_client=None,
    ):
        self.name = name
        self.model_name = model_name
        self.description = description
        self.data_spec = data_spec
        self.is_deprecated = is_deprecated
        self.created_time = created_time
        self.metadata = metadata
        self.args = args
        self._cognite_client = cognite_client


class ScheduleList(CogniteResourceList):
    _RESOURCE = Schedule
    _ASSERT_CLASSES = False


class LogEntry(CogniteResponse):
    """An object containing a log entry for a schedule.

    Args:
        timestamp (int): The time the log entry was recorded.
        scheduled_execution_time (int): The time the prediction was scheduled to run.
        message (str): The log message.
    """

    def __init__(self, timestamp: int = None, scheduled_execution_time: int = None, message: str = None):
        self.timestamp = timestamp
        self.scheduled_execution_time = scheduled_execution_time
        self.message = message

    @classmethod
    def _load(cls, api_response):
        return cls(
            timestamp=api_response["timestamp"],
            scheduled_execution_time=api_response["scheduledExecutionTime"],
            message=api_response["message"],
        )


class ScheduleLog(CogniteResponse):
    """An object containing the logs for a schedule.

    Args:
        failed (List[LogEntry]): A list of log entries for failed executions.
        completed (List[LogEntry]): A list of log entries for succesful executions.
    """

    def __init__(self, failed: List = None, completed: List = None):
        self.failed = failed
        self.completed = completed

    @classmethod
    def _load(cls, api_response):
        return cls(
            failed=[LogEntry._load(elem) for elem in api_response["failed"]],
            completed=[LogEntry._load(elem) for elem in api_response["completed"]],
        )
