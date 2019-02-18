import gzip
import json
import os
import time
from random import randint
from unittest import mock

import pytest

from cognite.client import APIError, CogniteClient
from cognite.client.experimental.model_hosting.models import (
    ModelArtifactCollectionResponse,
    ModelArtifactResponse,
    ModelCollectionResponse,
    ModelLogResponse,
    ModelResponse,
    ModelVersionCollectionResponse,
    ModelVersionResponse,
    PredictionError,
)
from cognite.client.experimental.model_hosting.schedules import ScheduleCollectionResponse, ScheduleResponse
from cognite.client.experimental.model_hosting.source_packages import (
    SourcePackageCollectionResponse,
    SourcePackageResponse,
)
from tests.conftest import MockReturnValue

modelhosting_client = CogniteClient().experimental.model_hosting

models = modelhosting_client.models
schedules = modelhosting_client.schedules
source_packages = modelhosting_client.source_packages


@pytest.fixture
def mock_data_spec():
    class FakeScheduleDataSpec:
        def dump(self):
            return {"spec": "spec"}

    return FakeScheduleDataSpec()


class TestSourcePackages:
    @pytest.fixture(scope="class")
    def source_package_file_path(self):
        file_path = "/tmp/sp.tar.gz"
        with open(file_path, "w") as f:
            f.write("content")
        yield file_path
        os.remove(file_path)

    @pytest.fixture(scope="class")
    def created_source_package(self, source_package_file_path):
        sp_name = "test-sp-{}".format(randint(0, 1e5))
        sp = source_packages.create_source_package(
            name=sp_name,
            package_name="whatever",
            available_operations=["TRAIN", "PREDICT"],
            runtime_version="0.1",
            file_path=source_package_file_path,
        )
        yield sp
        source_packages.delete_source_package(id=sp.id)

    def test_list_source_packages(self):
        res = source_packages.list_source_packages()
        assert len(res) > 0
        assert isinstance(res, SourcePackageCollectionResponse)
        assert isinstance(res[:1], SourcePackageCollectionResponse)
        assert isinstance(res[0], SourcePackageResponse)
        for sp in res:
            assert isinstance(sp, SourcePackageResponse)

    def test_get_source_package(self, created_source_package):
        for i in range(5):
            sp = source_packages.get_source_package(created_source_package.id)
            if sp.is_uploaded:
                break
            time.sleep(1)
        assert isinstance(sp, SourcePackageResponse)
        assert sp.id == created_source_package.id
        assert sp.is_uploaded is True

    def test_deprecate_source_package(self, created_source_package):
        sp = source_packages.deprecate_source_package(created_source_package.id)
        assert sp.is_deprecated is True
        sp = source_packages.get_source_package(created_source_package.id)
        assert sp.is_deprecated is True

    def test_download_code(self, created_source_package):
        source_packages.download_source_package_code(id=created_source_package.id, directory=os.getcwd())
        sp_name = source_packages.get_source_package(id=created_source_package.id).name
        file_path = os.path.join(os.getcwd(), sp_name + ".tar.gz")
        assert os.path.isfile(file_path)
        with open(file_path) as f:
            assert "content" == f.read()
        os.remove(file_path)

    @pytest.mark.skip(
        reason="Deleting source package code currently breaks deleting the source package, "
        "and causes get_source_package_code to return 500 instead of 404."
    )
    def test_delete_code(self, created_source_package):
        source_packages.delete_source_package_code(id=created_source_package.id)
        with pytest.raises(APIError, match="deleted"):
            source_packages.download_source_package_code(id=created_source_package.id)


class TestModels:
    @pytest.fixture
    def created_model(self):
        model_name = "test-model-{}".format(randint(0, 1e5))
        model = models.create_model(name=model_name)
        yield model
        models.delete_model(model.id)

    def test_get_model(self, created_model):
        model = models.get_model(created_model.id)
        assert model.name == created_model.name
        assert model.active_version_id is None

    def test_list_models(self):
        res = models.list_models()
        assert len(res) > 0
        assert isinstance(res, ModelCollectionResponse)
        assert isinstance(res[:1], ModelCollectionResponse)
        assert isinstance(res[0], ModelResponse)
        for model in res:
            assert isinstance(model, ModelResponse)

    def test_update_model(self, created_model):
        res = models.update_model(id=created_model.id, description="bla")
        assert isinstance(res, ModelResponse)
        assert res.description == "bla"
        model = models.get_model(id=created_model.id)
        assert model.description == "bla"

    @mock.patch("requests.sessions.Session.put")
    def test_predict_on_model(self, mock_put):
        mock_put.return_value = MockReturnValue(json_data={"data": {"predictions": [1, 2, 3]}})
        predictions = models.online_predict(model_id=1)
        assert predictions == [1, 2, 3]

    @mock.patch("requests.sessions.Session.put")
    def test_predict_on_model_prediction_error(self, mock_put):
        mock_put.return_value = MockReturnValue(json_data={"error": {"message": "User error", "code": 200}})
        with pytest.raises(PredictionError, match="User error"):
            models.online_predict(model_id=1)

    def test_deprecate_model(self, created_model):
        res = models.deprecate_model(id=created_model.id)
        assert isinstance(res, ModelResponse)
        assert res.is_deprecated is True
        model = models.get_model(id=created_model.id)
        assert model.is_deprecated is True


class TestVersions:
    model_version_response = {
        "data": {
            "items": [
                {
                    "isDeprecated": True,
                    "trainingDetails": {
                        "machineType": "string",
                        "scaleTier": "string",
                        "completedTime": 0,
                        "sourcePackageId": "string",
                        "args": "string",
                    },
                    "name": "string",
                    "errorMsg": "string",
                    "modelId": 1,
                    "createdTime": 0,
                    "metadata": {"k": "v"},
                    "id": 1,
                    "sourcePackageId": 1,
                    "status": "string",
                    "description": "string",
                    "project": "string",
                }
            ]
        },
        "nextCursor": "string",
    }

    @mock.patch("requests.sessions.Session.get")
    def test_list_versions(self, get_mock):
        get_mock.return_value = MockReturnValue(json_data=self.model_version_response)
        res = models.list_model_versions(model_id=1)
        assert len(res) > 0
        assert isinstance(res, ModelVersionCollectionResponse)
        assert isinstance(res[:1], ModelVersionCollectionResponse)
        assert isinstance(res[0], ModelVersionResponse)
        for model in res:
            assert isinstance(model, ModelVersionResponse)
        assert res[0].to_json() == self.model_version_response["data"]["items"][0]
        assert res[0].id == self.model_version_response["data"]["items"][0]["id"]

    @mock.patch("requests.sessions.Session.get")
    def test_get_version(self, get_mock):
        get_mock.return_value = MockReturnValue(json_data=self.model_version_response)
        model_version = models.get_model_version(model_id=1, version_id=1)
        assert isinstance(model_version, ModelVersionResponse)
        assert model_version.id == self.model_version_response["data"]["items"][0]["id"]

    @mock.patch("requests.sessions.Session.post")
    def test_train_version(self, post_mock):
        post_mock.return_value = MockReturnValue(json_data=self.model_version_response)
        res = models.train_model_version(id=1, name="mymodel", source_package_id=1)
        assert isinstance(res, ModelVersionResponse)

    @mock.patch("requests.sessions.Session.post")
    def test_train_version_data_spec_arg(self, post_mock, mock_data_spec):
        post_mock.return_value = MockReturnValue(json_data=self.model_version_response)
        models.train_model_version(id=1, name="mymodel", source_package_id=1, args={"data_spec": mock_data_spec})

        data_sent_to_api = json.loads(gzip.decompress(post_mock.call_args[1]["data"]).decode())
        assert {"spec": "spec"} == data_sent_to_api["trainingDetails"]["args"]["data_spec"]

    @mock.patch("requests.sessions.Session.delete")
    def test_delete_version(self, delete_mock):
        delete_mock.return_value = MockReturnValue()
        res = models.delete_model_version(model_id=1, version_id=1)
        assert res is None

    @mock.patch("requests.sessions.Session.get")
    def test_list_artifacts(self, get_mock):
        get_mock.return_value = MockReturnValue(json_data={"data": {"items": [{"name": "a1", "size": 1}]}})
        res = models.list_artifacts(model_id=1, version_id=1)
        assert len(res) > 0
        assert isinstance(res, ModelArtifactCollectionResponse)
        assert isinstance(res[:1], ModelArtifactCollectionResponse)
        assert isinstance(res[0], ModelArtifactResponse)
        assert res[0].name == "a1"
        assert res[0].size == 1

    @mock.patch("requests.sessions.Session.get")
    def test_download_artifact(self, mock_get):
        mock_get.side_effect = [
            MockReturnValue(json_data={"data": {"downloadUrl": "https://download.me"}}),
            MockReturnValue(content=b"content"),
        ]
        models.download_artifact(model_id=1, version_id=1, name="a1", directory=os.getcwd())
        file_path = os.path.join(os.getcwd(), "a1")
        assert os.path.isfile(file_path)
        with open(file_path, "rb") as f:
            assert b"content" == f.read()
        os.remove(file_path)

    @mock.patch("requests.sessions.Session.put")
    def test_deprecate_model_version(self, mock_put):
        mock_put.return_value = MockReturnValue(json_data=self.model_version_response)
        res = models.deprecate_model_version(model_id=1, version_id=1)
        assert isinstance(res, ModelVersionResponse)
        assert res.is_deprecated is True

    @mock.patch("requests.sessions.Session.put")
    def test_update_model_version(self, mock_put):
        updated_model_version = self.model_version_response.copy()
        updated_model_version["data"]["items"][0]["description"] = "blabla"
        mock_put.return_value = MockReturnValue(json_data=updated_model_version)
        res = models.update_model_version(model_id=1, version_id=1, description="blabla")
        assert isinstance(res, ModelVersionResponse)
        assert res.description == "blabla"

    @mock.patch("requests.sessions.Session.get")
    def test_get_model_version_log(self, mock_get):
        mock_get.return_value = MockReturnValue(
            json_data={"data": {"predict": ["l1", "l2", "l3"], "train": ["l1", "l2", "l3"]}}
        )
        res = models.get_logs(model_id=1, version_id=1, log_type="both")
        assert isinstance(res, ModelLogResponse)
        assert res.prediction_logs == ["l1", "l2", "l3"]
        assert res.training_logs == ["l1", "l2", "l3"]
        assert (
            res.__str__() == '{\n    "predict": [\n        "l1",\n        "l2",\n        "l3"\n    ],\n    '
            '"train": [\n        "l1",\n        "l2",\n        "l3"\n    ]\n}'
        )

    @mock.patch("requests.sessions.Session.put")
    def test_predict_on_model_version(self, mock_put):
        mock_put.return_value = MockReturnValue(json_data={"data": {"predictions": [1, 2, 3]}})
        predictions = models.online_predict(model_id=1, version_id=1)
        assert predictions == [1, 2, 3]

    @mock.patch("requests.sessions.Session.put")
    def test_predict_instance_is_data_spec(self, mock_put, mock_data_spec):
        mock_put.return_value = MockReturnValue(json_data={"data": {"predictions": [1, 2, 3]}})
        models.online_predict(model_id=1, version_id=1, instances=[mock_data_spec, mock_data_spec])
        data_sent_to_api = json.loads(mock_put.call_args[1]["data"])
        for instance in data_sent_to_api["instances"]:
            assert {"spec": "spec"} == instance

    @mock.patch("requests.sessions.Session.put")
    def test_predict_on_model_version_prediction_error(self, mock_put):
        mock_put.return_value = MockReturnValue(json_data={"error": {"message": "User error", "code": 200}})
        with pytest.raises(PredictionError, match="User error"):
            models.online_predict(model_id=1, version_id=1)


class TestSchedules:
    schedule_response = {
        "data": {
            "items": [
                {
                    "isDeprecated": False,
                    "name": "test-schedule",
                    "outputDataSpec": {"spec": "spec"},
                    "modelId": 123,
                    "createdTime": 0,
                    "metadata": {"k": "v"},
                    "id": 123,
                    "inputDataSpec": {"spec": "spec"},
                    "args": {"k": "v"},
                    "description": "string",
                    "lastProcessedTimestamp": 0,
                }
            ]
        },
        "nextCursor": "string",
    }

    @mock.patch("requests.sessions.Session.post")
    def test_create_schedule(self, mock_post):
        mock_post.return_value = MockReturnValue(json_data=self.schedule_response)
        res = schedules.create_schedule(
            model_id=123,
            name="myschedule",
            input_data_spec={"spec": "spec"},
            output_data_spec={"spec": "spec"},
            args={"k": "v"},
            metadata={"k": "v"},
        )
        assert isinstance(res, ScheduleResponse)
        assert res.id == 123

    @mock.patch("requests.sessions.Session.post")
    def test_create_schedule_with_data_spec_objects(self, mock_post, mock_data_spec):
        mock_post.return_value = MockReturnValue(json_data=self.schedule_response)
        res = schedules.create_schedule(
            model_id=123,
            name="myschedule",
            input_data_spec=mock_data_spec,
            output_data_spec=mock_data_spec,
            args={"k": "v"},
            metadata={"k": "v"},
        )
        assert isinstance(res, ScheduleResponse)
        assert res.id == 123

        data_sent_to_api = json.loads(gzip.decompress(mock_post.call_args[1]["data"]).decode())
        actual_input_data_spec = data_sent_to_api["inputDataSpec"]
        actual_output_data_spec = data_sent_to_api["outputDataSpec"]

        assert {"spec": "spec"} == actual_input_data_spec
        assert {"spec": "spec"} == actual_output_data_spec

    @mock.patch("requests.sessions.Session.get")
    def test_list_schedules(self, mock_get):
        mock_get.return_value = MockReturnValue(json_data=self.schedule_response)
        res = schedules.list_schedules(limit=1)
        assert len(res) > 0
        assert isinstance(res, ScheduleCollectionResponse)
        assert isinstance(res[:1], ScheduleCollectionResponse)
        assert isinstance(res[0], ScheduleResponse)
        assert self.schedule_response["data"]["items"][0]["name"] == res[0].name

    @mock.patch("requests.sessions.Session.get")
    def test_get_schedule(self, mock_get):
        mock_get.return_value = MockReturnValue(json_data=self.schedule_response)
        res = schedules.get_schedule(id=1)
        assert isinstance(res, ScheduleResponse)
        assert self.schedule_response["data"]["items"][0]["name"] == res.name

    @mock.patch("requests.sessions.Session.put")
    def test_deprecate_schedule(self, mock_put):
        depr_schedule_response = self.schedule_response.copy()
        depr_schedule_response["data"]["items"][0]["isDeprecated"] = True
        mock_put.return_value = MockReturnValue(json_data=depr_schedule_response)

        res = schedules.deprecate_schedule(id=1)
        assert res.is_deprecated is True

    @mock.patch("requests.sessions.Session.delete")
    def test_delete_schedule(self, mock_delete):
        mock_delete.return_value = MockReturnValue()
        res = schedules.delete_schedule(id=1)
        assert res is None
