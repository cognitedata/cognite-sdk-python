from typing import *

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList, CogniteResponse


class Schedule(CogniteResource):
    def __init__(
        self,
        id: int = None,
        model_id: int = None,
        name: str = None,
        description: str = None,
        schedule_data_spec: Union["ScheduleDataSpec", Dict] = None,
        is_deprecated: bool = None,
        created_time: int = None,
        metadata: Dict = None,
        args: Dict = None,
        cognite_client=None,
    ):
        self.id = id
        self.model_id = model_id
        self.name = name
        self.description = description
        self.schedule_data_spec = schedule_data_spec
        self.is_deprecated = is_deprecated
        self.created_time = created_time
        self.metadata = metadata
        self.args = args
        self._cognite_client = cognite_client


class ScheduleList(CogniteResourceList):
    _RESOURCE = Schedule
    _ASSERT_CLASSES = False


class LogEntry(CogniteResponse):
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
    def __init__(self, failed: List = None, completed: List = None):
        self.failed = failed
        self.completed = completed

    @classmethod
    def _load(cls, api_response):
        return cls(
            failed=[LogEntry._load(elem) for elem in api_response["data"]["failed"]],
            completed=[LogEntry._load(elem) for elem in api_response["data"]["completed"]],
        )
