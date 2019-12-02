from typing import *

from cognite.client._api_client import APIClient
from cognite.client.data_classes.model_hosting.schedules import Schedule, ScheduleList, ScheduleLog


class SchedulesAPI(APIClient):
    def create_schedule(
        self,
        model_name: str,
        schedule_name: str,
        schedule_data_spec: Any,
        description: str = None,
        args: Dict = None,
        metadata: Dict = None,
    ) -> Schedule:
        """Create a new schedule on a given model.

        Args:
            model_name (str): Name of the model to create schedule on
            schedule_name (str): Name of the schedule
            schedule_data_spec (Any): Specification of schedule input/output. Can be either a dictionary or a
                                    ScheduleDataSpec object from the cognite-model-hosting library.
            description (str): Description for schedule
            args (Dict): Dictionary of keyword arguments to pass to predict method.
            metadata (Dict): Dictionary of metadata about schedule

        Returns:
            Schedule: The created schedule.
        """
        url_path = "/modelhosting/models/{}/schedules".format(model_name)

        if hasattr(schedule_data_spec, "dump"):
            schedule_data_spec = schedule_data_spec.dump()

        body = {
            "name": schedule_name,
            "description": description or "",
            "args": args or {},
            "dataSpec": schedule_data_spec,
            "metadata": metadata or {},
        }
        res = self._post(url_path, json=body)
        return Schedule._load(res.json())

    def list_schedules(
        self, model_name: str, limit: int = None, cursor: int = None, autopaging: bool = False
    ) -> ScheduleList:
        """Get all schedules.

        Args:
            model_name (str): Model for which to list the schedules.
            limit (int): Maximum number of schedules to return. Defaults to 250.
            cursor (str): Cursor to use to fetch next set of results.
            autopaging (bool): Whether or not to automatically page through all results. Will disregard limit.

        Returns:
            ScheduleList: The requested schedules.
        """
        params = {"cursor": cursor, "limit": limit if autopaging is False else self._LIST_LIMIT}
        res = self._get("/modelhosting/models/{}/schedules".format(model_name), params=params)
        return ScheduleList._load(res.json()["items"])

    def get_schedule(self, model_name: str, schedule_name: str) -> Schedule:
        """Get a schedule by name.

        Args:
            model_name (str): Name of model associated with this schedule.
            schedule_name (str): Name of schedule to get.
        Returns:
            Schedule: The requested schedule.
        """
        res = self._get("/modelhosting/models/{}/schedules/{}".format(model_name, schedule_name))
        return Schedule._load(res.json())

    def deprecate_schedule(self, model_name: str, schedule_name: str) -> Schedule:
        """Deprecate a schedule.

        Args:
            model_name (str): Name of model associated with this schedule.
            schedule_name (str):  Name of schedule to deprecate.

        Returns:
            Schedule: The deprecated schedule.
        """
        res = self._put("/modelhosting/models/{}/schedules/{}/deprecate".format(model_name, schedule_name))
        return Schedule._load(res.json())

    def delete_schedule(self, model_name: str, schedule_name: str) -> None:
        """Delete a schedule by id.

        Args:
            model_name (str): Name of model associated with this schedule.
            schedule_name (str):  The name of the schedule to delete.

        Returns:
            None
        """
        self._delete("/modelhosting/models/{}/schedules/{}".format(model_name, schedule_name))

    def get_log(self, model_name: str, schedule_name: str) -> ScheduleLog:
        """Return schedule log by id. The ScheduleLog object contains two logs, one for failed scheduled
        predictions and one for successful.

        Args:
            model_name (str): Name of model associated with this schedule.
            schedule_name (str):  The name of the schedule to get logs from.

        Returns:
            ScheduleLog: An object containing the schedule logs.
        """
        res = self._get("/modelhosting/models/{}/schedules/{}/log".format(model_name, schedule_name))
        return ScheduleLog._load(res.json())
