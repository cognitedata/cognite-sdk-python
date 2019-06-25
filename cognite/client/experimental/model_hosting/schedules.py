from typing import Any, Dict, List, Union

from cognite.client._api_client import APIClient, CogniteCollectionResponse, CogniteResponse


class ScheduleResponse(CogniteResponse):
    def __init__(self, internal_representation):
        super().__init__(internal_representation)
        item = self.to_json()
        self.is_deprecated = item["isDeprecated"]
        self.name = item["name"]
        self.schedule_data_spec = item["dataSpec"]
        self.model_id = item["modelId"]
        self.created_time = item["createdTime"]
        self.metadata = item["metadata"]
        self.id = item["id"]
        self.args = item["args"]
        self.description = item["description"]


class ScheduleCollectionResponse(CogniteCollectionResponse):
    _RESPONSE_CLASS = ScheduleResponse


class LogEntry(CogniteResponse):
    def __init__(self, internal_representation):
        super().__init__(internal_representation)
        item = self.to_json()
        self.timestamp = item["timestamp"]
        self.scheduled_execution_time = item["scheduledExecutionTime"]
        self.message = item["message"]

    def to_json(self):
        """Returns data as a json object"""
        return self.internal_representation


class ScheduleLog(CogniteResponse):
    def __init__(self, internal_representation):
        super().__init__(internal_representation)
        item = self.to_json()
        self.failed = [LogEntry(elem) for elem in item["failed"]]
        self.completed = [LogEntry(elem) for elem in item["completed"]]

    def to_json(self):
        """Returns data as a json object"""
        return self.internal_representation["data"]


class SchedulesClient(APIClient):
    def __init__(self, **kwargs):
        super().__init__(version="0.6", **kwargs)
        self._LIMIT = 1000

    def create_schedule(
        self,
        model_id: int,
        name: str,
        schedule_data_spec: Any,
        description: str = None,
        args: Dict = None,
        metadata: Dict = None,
    ) -> ScheduleResponse:
        """Create a new schedule on a given model.

        Args:
            model_id (int): Id of model to create schedule on
            name (str): Name of schedule
            schedule_data_spec (Any): Specification of schedule input/output. Can be either a dictionary or a
                                    ScheduleDataSpec object from cognite-data-fetcher
            description (str): Description for schedule
            args (Dict): Dictionary of keyword arguments to pass to predict method.
            metadata (Dict): Dictionary of metadata about schedule

        Returns:
            experimental.model_hosting.schedules.ScheduleResponse: The created schedule.
        """
        url = "/analytics/models/schedules"

        if hasattr(schedule_data_spec, "dump"):
            schedule_data_spec = schedule_data_spec.dump()

        body = {
            "name": name,
            "description": description or "",
            "modelId": model_id,
            "args": args or {},
            "dataSpec": schedule_data_spec,
            "metadata": metadata or {},
        }
        res = self._post(url, body=body)
        return ScheduleResponse(res.json())

    def list_schedules(
        self, limit: int = None, cursor: int = None, autopaging: bool = False
    ) -> ScheduleCollectionResponse:
        """Get all schedules.

        Args:
            limit (int): Maximum number of schedules to return. Defaults to 250.
            cursor (str): Cursor to use to fetch next set of results.
            autopaging (bool): Whether or not to automatically page through all results. Will disregard limit.

        Returns:
            experimental.model_hosting.schedules.ScheduleCollectionResponse: The requested schedules.
        """
        url = "/analytics/models/schedules"
        params = {"cursor": cursor, "limit": limit if autopaging is False else self._LIMIT}
        res = self._get(url, params=params, autopaging=autopaging)
        return ScheduleCollectionResponse(res.json())

    def get_schedule(self, id: int) -> ScheduleResponse:
        """Get a schedule by id.

        Args:
            id (int): Id of schedule to get.
        Returns:
            experimental.model_hosting.schedules.ScheduleResponse: The requested schedule.
        """
        url = "/analytics/models/schedules/{}".format(id)
        res = self._get(url=url)
        return ScheduleResponse(res.json())

    def deprecate_schedule(self, id: int) -> ScheduleResponse:
        """Deprecate a schedule.

        Args:
            id (int): Id of schedule to deprecate

        Returns:
            experimental.model_hosting.schedules.ScheduleResponse: The deprecated schedule.
        """
        url = "/analytics/models/schedules/{}/deprecate".format(id)
        res = self._put(url)
        return ScheduleResponse(res.json())

    def delete_schedule(self, id: int) -> None:
        """Delete a schedule by id.

        Args:
            id (int):  The id of the schedule to delete.

        Returns:
            None
        """
        url = "/analytics/models/schedules/{}".format(id)
        self._delete(url=url)

    def get_log(self, id: int) -> ScheduleLog:
        """Return schedule log by id. The ScheduleLog object contains two logs, one for failed scheduled
        predictions and one for successful. 

        Args:
            id (int):  The id of the schedule to get logs from.

        Returns:
            experimental.model_hosting.schedules.ScheduleLog: An object containing the schedule logs.
        """
        url = "/analytics/models/schedules/{}/log".format(id)
        res = self._get(url)
        log = ScheduleLog(res.json())
        return log
