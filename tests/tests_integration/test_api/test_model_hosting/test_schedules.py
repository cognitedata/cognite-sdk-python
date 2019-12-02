import pytest

from cognite.client.data_classes.model_hosting.schedules import Schedule, ScheduleList, ScheduleLog
from cognite.client.experimental import CogniteClient
from tests.utils import jsgz_load

SCHEDULES_API = CogniteClient().model_hosting.schedules


class TestSchedules:
    schedule_response = {
        "isDeprecated": False,
        "name": "test-schedule",
        "dataSpec": {"spec": "spec"},
        "modelName": "model1",
        "createdTime": 0,
        "metadata": {"k": "v"},
        "args": {"k": "v"},
        "description": "string",
    }

    @pytest.fixture
    def mock_post_schedule(self, rsps):
        rsps.add(
            rsps.POST,
            SCHEDULES_API._get_base_url_with_base_path() + "/modelhosting/models/model1/schedules",
            status=201,
            json=self.schedule_response,
        )
        yield rsps

    def test_create_schedule(self, mock_post_schedule):
        res = SCHEDULES_API.create_schedule(
            model_name="model1",
            schedule_name="test-schedule",
            schedule_data_spec={"spec": "spec"},
            args={"k": "v"},
            metadata={"k": "v"},
        )
        assert isinstance(res, Schedule)
        assert res.name == "test-schedule"

    def test_create_schedule_with_data_spec_objects(self, mock_post_schedule, mock_data_spec):
        res = SCHEDULES_API.create_schedule(
            model_name="model1",
            schedule_name="test-schedule",
            schedule_data_spec=mock_data_spec,
            args={"k": "v"},
            metadata={"k": "v"},
        )
        assert isinstance(res, Schedule)
        assert res.name == "test-schedule"

        data_sent_to_api = jsgz_load(mock_post_schedule.calls[0].request.body)
        actual_data_spec = data_sent_to_api["dataSpec"]

        assert {"spec": "spec"} == actual_data_spec

    @pytest.fixture
    def mock_get_schedules(self, rsps):
        response = {"items": [self.schedule_response]}
        rsps.add(
            rsps.GET,
            SCHEDULES_API._get_base_url_with_base_path() + "/modelhosting/models/model1/schedules",
            json=response,
        )
        yield rsps

    def test_list_schedules(self, mock_get_schedules):
        res = SCHEDULES_API.list_schedules(model_name="model1", limit=1)
        assert len(res) > 0
        assert isinstance(res, ScheduleList)
        assert isinstance(res[:1], ScheduleList)
        assert isinstance(res[0], Schedule)
        assert self.schedule_response["name"] == res[0].name

    @pytest.fixture
    def mock_get_schedule(self, rsps):
        rsps.add(
            rsps.GET,
            SCHEDULES_API._get_base_url_with_base_path() + "/modelhosting/models/model1/schedules/schedule1",
            json=self.schedule_response,
        )
        yield rsps

    def test_get_schedule(self, mock_get_schedule):
        res = SCHEDULES_API.get_schedule(model_name="model1", schedule_name="schedule1")
        assert isinstance(res, Schedule)
        assert self.schedule_response["name"] == res.name

    @pytest.fixture
    def mock_put_deprecate(self, rsps):
        depr_schedule_response = self.schedule_response.copy()
        depr_schedule_response["isDeprecated"] = True
        rsps.add(
            rsps.PUT,
            SCHEDULES_API._get_base_url_with_base_path() + "/modelhosting/models/model1/schedules/schedule1/deprecate",
            json=depr_schedule_response,
        )
        yield rsps

    def test_deprecate_schedule(self, mock_put_deprecate):
        res = SCHEDULES_API.deprecate_schedule(model_name="model1", schedule_name="schedule1")
        assert res.is_deprecated is True

    @pytest.fixture
    def mock_delete_schedule(self, rsps):
        rsps.add(
            rsps.DELETE,
            SCHEDULES_API._get_base_url_with_base_path() + "/modelhosting/models/model1/schedules/schedule1",
        )

    def test_delete_schedule(self, mock_delete_schedule):
        res = SCHEDULES_API.delete_schedule(model_name="model1", schedule_name="schedule1")
        assert res is None

    @pytest.fixture
    def mock_get_log(self, rsps):
        schedule_log_response = {
            "failed": [{"timestamp": 123, "scheduledExecutionTime": 345, "message": "you made mistake"}],
            "completed": [],
        }
        rsps.add(
            rsps.GET,
            SCHEDULES_API._get_base_url_with_base_path() + "/modelhosting/models/model1/schedules/schedule1/log",
            json=schedule_log_response,
        )
        yield rsps

    def test_get_log(self, mock_get_log):
        res = SCHEDULES_API.get_log(model_name="model1", schedule_name="schedule1")
        assert isinstance(res, ScheduleLog)
        assert 1 == len(res.failed)
        assert 0 == len(res.completed)
        assert 123 == res.failed[0].timestamp
        assert 345 == res.failed[0].scheduled_execution_time
        assert "you made mistake" == res.failed[0].message
