from typing import *

from cognite.client._api_client import APIClient
from cognite.client.data_classes.model_hosting.schedules import Schedule, ScheduleList, ScheduleLog


class SchedulesAPI(APIClient):
    def create_schedule(
        self,
        model_id: int,
        name: str,
        schedule_data_spec: Any,
        description: str = None,
        args: Dict = None,
        metadata: Dict = None,
    ) -> Schedule:
        """Create a new schedule on a given model.

        Args:
            model_id (int): Id of model to create schedule on
            name (str): Name of schedule
            schedule_data_spec (Any): Specification of schedule input/output. Can be either a dictionary or a
                                    ScheduleDataSpec object from the cognite-model-hosting library.
            description (str): Description for schedule
            args (Dict): Dictionary of keyword arguments to pass to predict method.
            metadata (Dict): Dictionary of metadata about schedule

        Returns:
            Schedule: The created schedule.
        """
        url_path = "/analytics/models/schedules"

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
        res = self._post(url_path, json=body)
        return Schedule._load(res.json()["data"]["items"][0])

    def list_schedules(self, limit: int = None, cursor: int = None, autopaging: bool = False) -> ScheduleList:
        """Get all schedules.

        Args:
            limit (int): Maximum number of schedules to return. Defaults to 250.
            cursor (str): Cursor to use to fetch next set of results.
            autopaging (bool): Whether or not to automatically page through all results. Will disregard limit.

        Returns:
            ScheduleList: The requested schedules.
        """
        params = {"cursor": cursor, "limit": limit if autopaging is False else self._LIST_LIMIT}
        res = self._get("/analytics/models/schedules", params=params)
        return ScheduleList._load(res.json()["data"]["items"])

    def get_schedule(self, id: int) -> Schedule:
        """Get a schedule by id.

        Args:
            id (int): Id of schedule to get.
        Returns:
            Schedule: The requested schedule.
        """
        res = self._get("/analytics/models/schedules/{}".format(id))
        return Schedule._load(res.json()["data"]["items"][0])

    def deprecate_schedule(self, id: int) -> Schedule:
        """Deprecate a schedule.

        Args:
            id (int): Id of schedule to deprecate

        Returns:
            Schedule: The deprecated schedule.
        """
        res = self._put("/analytics/models/schedules/{}/deprecate".format(id))
        return Schedule._load(res.json()["data"]["items"][0])

    def delete_schedule(self, id: int) -> None:
        """Delete a schedule by id.

        Args:
            id (int):  The id of the schedule to delete.

        Returns:
            None
        """
        self._delete("/analytics/models/schedules/{}".format(id))

    def get_log(self, id: int) -> ScheduleLog:
        """Return schedule log by id. The ScheduleLog object contains two logs, one for failed scheduled
        predictions and one for successful.

        Args:
            id (int):  The id of the schedule to get logs from.

        Returns:
            ScheduleLog: An object containing the schedule logs.
        """
        res = self._get("/analytics/models/schedules/{}/log".format(id))
        return ScheduleLog._load(res.json())
