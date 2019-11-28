import os
from copy import deepcopy
from tempfile import TemporaryDirectory

import pytest

from cognite.client._api.model_hosting.models import PredictionError
from cognite.client._api.model_hosting.versions import EmptyArtifactsDirectory
from cognite.client.data_classes.model_hosting.versions import (
    ModelArtifact,
    ModelArtifactList,
    ModelVersion,
    ModelVersionList,
    ModelVersionLog,
)
from cognite.client.experimental import CogniteClient
from tests.utils import jsgz_load

MODELS_API = CogniteClient().model_hosting.models
VERSIONS_API = CogniteClient().model_hosting.versions


@pytest.mark.skip("model hosting development")
class TestVersions:
    model_version_response = {
        "isDeprecated": True,
        "trainingDetails": {
            "machineType": "string",
            "scaleTier": "string",
            "completedTime": 0,
            "sourcePackageId": "string",
            "args": "string",
        },
        "name": "version1",
        "errorMsg": "string",
        "modelName": "model1",
        "createdTime": 0,
        "metadata": {"k": "v"},
        "sourcePackageId": 1,
        "status": "string",
        "description": "string",
    }

    @pytest.fixture
    def mock_post_model_version(self, rsps):
        model_version_response = deepcopy(self.model_version_response)
        model_version_response["trainingDetails"] = None
        rsps.add(
            rsps.POST,
            VERSIONS_API._get_base_url_with_base_path() + "/analytics/models/model1/versions",
            status=201,
            json=model_version_response,
        )
        yield rsps

    def test_create_version(self, mock_post_model_version):
        res = VERSIONS_API.create_model_version(model_name="model1", version_name="version1", source_package_id=1)
        assert isinstance(res, ModelVersion)
        assert "version1" == res.name

    @pytest.fixture
    def mock_post_deploy_model_version(self, rsps):
        model_version_response = deepcopy(self.model_version_response)
        model_version_response["trainingDetails"] = None
        model_version_response["status"] = "DEPLOYING"
        rsps.add(
            rsps.POST,
            VERSIONS_API._get_base_url_with_base_path() + "/analytics/models/model1/versions/version1/deploy",
            status=200,
            json=model_version_response,
        )
        yield rsps

    def test_deploy_version(self, mock_post_deploy_model_version):
        res = VERSIONS_API.deploy_awaiting_model_version(model_name="model1", version_name="version1")
        assert isinstance(res, ModelVersion)
        assert "DEPLOYING" == res.status

    @pytest.fixture
    def mock_create_and_deploy_model_version(self, mock_post_model_version, mock_post_deploy_model_version):
        mock_post_model_version.add(
            mock_post_model_version.POST,
            VERSIONS_API._get_base_url_with_base_path() + "/analytics/models/model1/versions/version1/artifacts/upload",
            status=201,
            json={"uploadUrl": "https://upload.here"},
        )
        mock_post_model_version.add(mock_post_model_version.PUT, "https://upload.here", status=200, json={})
        yield mock_post_model_version

    def test_create_and_deploy_model_version(self, mock_create_and_deploy_model_version):
        artifacts_directory = os.path.join(os.path.dirname(__file__), "source_package_for_tests/artifacts")
        model_version = VERSIONS_API.deploy_model_version(
            model_name="model1", version_name="version1", source_package_id=1, artifacts_directory=artifacts_directory
        )
        calls = mock_create_and_deploy_model_version.calls
        assert model_version.name == "version1"
        assert {"description": "", "metadata": {}, "name": "version1", "sourcePackageId": 1} == jsgz_load(
            calls[0].request.body
        )
        for call in calls[1:5]:
            try:
                res = jsgz_load(call.request.body)
                assert res in [{"name": "artifact1.txt"}, {"name": os.path.join("sub_dir", "artifact2.txt")}]
            except OSError:
                assert call.request.body in [b"content\n", b"content\r\n"]
        assert b"{}" == calls[5].request.body

    @pytest.fixture
    def mock_get_model_versions(self, rsps):
        model_version_response = {"items": [deepcopy(self.model_version_response)]}
        rsps.add(
            rsps.GET,
            VERSIONS_API._get_base_url_with_base_path() + "/analytics/models/model1/versions",
            status=200,
            json=model_version_response,
        )
        yield rsps

    def test_list_versions(self, mock_get_model_versions):
        res = VERSIONS_API.list_model_versions(model_name="model1")
        assert len(res) > 0
        assert isinstance(res, ModelVersionList)
        assert isinstance(res[:1], ModelVersionList)
        assert isinstance(res[0], ModelVersion)
        for model in res:
            assert isinstance(model, ModelVersion)
        expected = deepcopy(self.model_version_response)
        print(expected)
        print(res[0].dump(camel_case=True))
        assert expected == res[0].dump(camel_case=True)

    @pytest.fixture
    def mock_get_version(self, rsps):
        rsps.add(
            rsps.GET,
            VERSIONS_API._get_base_url_with_base_path() + "/analytics/models/model1/versions/version1",
            status=200,
            json=self.model_version_response,
        )
        yield rsps

    def test_get_version(self, mock_get_version):
        model_version = VERSIONS_API.get_model_version(model_name="model1", version_name="version1")
        assert isinstance(model_version, ModelVersion)
        assert model_version.name == self.model_version_response["name"]

    @pytest.fixture
    def mock_delete_version(self, rsps):
        rsps.add(
            rsps.DELETE,
            VERSIONS_API._get_base_url_with_base_path() + "/analytics/models/model1/versions/version1",
            status=200,
            json=self.model_version_response,
        )
        yield rsps

    def test_delete_version(self, mock_delete_version):
        res = VERSIONS_API.delete_model_version(model_name="model1", version_name="version1")
        assert res is None

    @pytest.fixture
    def mock_get_artifacts(self, rsps):
        rsps.add(
            rsps.GET,
            VERSIONS_API._get_base_url_with_base_path() + "/analytics/models/model1/versions/version1/artifacts",
            status=200,
            json={"items": [{"name": "a1", "size": 1}]},
        )
        yield rsps

    def test_list_artifacts(self, mock_get_artifacts):
        res = VERSIONS_API.list_artifacts(model_name="model1", version_name="version1")
        assert len(res) > 0
        assert isinstance(res, ModelArtifactList)
        assert isinstance(res[:1], ModelArtifactList)
        assert isinstance(res[0], ModelArtifact)
        assert res[0].name == "a1"
        assert res[0].size == 1

    @pytest.fixture
    def mock_download_artifact(self, rsps):
        rsps.add(
            rsps.GET,
            VERSIONS_API._get_base_url_with_base_path() + "/analytics/models/model1/versions/version1/artifacts/a1",
            status=200,
            json={"downloadUrl": "https://download.me"},
        )
        rsps.add(rsps.GET, "https://download.me", status=200, body=b"content")
        yield rsps

    def test_download_artifact(self, mock_download_artifact):
        VERSIONS_API.download_artifact(
            model_name="model1", version_name="version1", artifact_name="a1", directory=os.getcwd()
        )
        file_path = os.path.join(os.getcwd(), "a1")
        assert os.path.isfile(file_path)
        with open(file_path, "rb") as f:
            assert b"content" == f.read()
        os.remove(file_path)

    @pytest.fixture(scope="class")
    def artifact_file_path(self):
        with TemporaryDirectory() as tmp_dir:
            file_path = os.path.join(tmp_dir, "my_artifact.txt")
            with open(file_path, "w") as f:
                f.write("content")
            yield file_path

    @pytest.fixture
    def mock_upload_artifact(self, rsps):
        rsps.add(
            rsps.POST,
            VERSIONS_API._get_base_url_with_base_path() + "/analytics/models/model1/versions/version1/artifacts/upload",
            json={"uploadUrl": "https://upload.here"},
        )
        rsps.add(rsps.PUT, "https://upload.here")
        yield rsps

    def test_upload_artifact_from_file(self, mock_upload_artifact, artifact_file_path):
        VERSIONS_API.upload_artifact_from_file(
            model_name="model1", version_name="version1", artifact_name="my_artifact.txt", file_path=artifact_file_path
        )
        assert b"content" == mock_upload_artifact.calls[1].request.body

    def test_upload_artifacts_from_directory(self, mock_upload_artifact):
        artifacts_directory = os.path.join(os.path.dirname(__file__), "source_package_for_tests/artifacts")
        VERSIONS_API.upload_artifacts_from_directory(
            model_name="model1", version_name="version1", directory=artifacts_directory
        )
        for call in mock_upload_artifact.calls:
            try:
                res = jsgz_load(call.request.body)
                assert res in [{"name": "artifact1.txt"}, {"name": os.path.join("sub_dir", "artifact2.txt")}]
            except OSError:
                assert call.request.body in [b"content\n", b"content\r\n"]

    def test_upload_artifacts_from_directory_no_artifacts(self):
        with TemporaryDirectory() as tmp:
            with pytest.raises(EmptyArtifactsDirectory, match="directory is empty"):
                VERSIONS_API.upload_artifacts_from_directory(
                    model_name="model1", version_name="version1", directory=tmp
                )

    @pytest.fixture
    def mock_post_deprecate(self, rsps):
        rsps.add(
            rsps.POST,
            VERSIONS_API._get_base_url_with_base_path() + "/analytics/models/model1/versions/version1/deprecate",
            json=self.model_version_response,
        )
        yield rsps

    def test_deprecate_model_version(self, mock_post_deprecate):
        res = VERSIONS_API.deprecate_model_version(model_name="model1", version_name="version1")
        assert isinstance(res, ModelVersion)
        assert res.is_deprecated is True

    @pytest.fixture
    def mock_post_update(self, rsps):
        updated_model_version = deepcopy(self.model_version_response)
        updated_model_version["description"] = "blabla"
        rsps.add(
            rsps.POST,
            VERSIONS_API._get_base_url_with_base_path() + "/analytics/models/model1/versions/version1/update",
            json=updated_model_version,
        )
        yield rsps

    def test_update_model_version(self, mock_post_update):
        res = VERSIONS_API.update_model_version(model_name="model1", version_name="version1", description="blabla")
        assert isinstance(res, ModelVersion)
        assert res.description == "blabla"

    @pytest.fixture
    def mock_get_log(self, rsps):
        rsps.add(
            rsps.GET,
            VERSIONS_API._get_base_url_with_base_path() + "/analytics/models/model1/versions/version1/log",
            json={"predict": ["l1", "l2", "l3"], "train": ["l1", "l2", "l3"]},
        )
        yield rsps

    def test_get_model_version_log(self, mock_get_log):
        res = VERSIONS_API.get_logs(model_name="model1", version_name="version1", log_type="both")
        assert isinstance(res, ModelVersionLog)
        assert res.prediction_logs == ["l1", "l2", "l3"]
        assert res.training_logs == ["l1", "l2", "l3"]
        assert {"prediction_logs": ["l1", "l2", "l3"], "training_logs": ["l1", "l2", "l3"]} == res.dump()

    @pytest.fixture
    def mock_post_predict(self, rsps):
        rsps.add(
            rsps.POST,
            VERSIONS_API._get_base_url_with_base_path() + "/analytics/models/model1/versions/version1/predict",
            json={"predictions": [1, 2, 3]},
        )
        yield rsps

    def test_predict_on_model_version(self, mock_post_predict):
        predictions = MODELS_API.online_predict(model_name="model1", version_name="version1")
        assert predictions == [1, 2, 3]

    def test_predict_instance_is_data_spec(self, mock_post_predict, mock_data_spec):
        MODELS_API.online_predict(
            model_name="model1", version_name="version1", instances=[mock_data_spec, mock_data_spec]
        )
        data_sent_to_api = jsgz_load(mock_post_predict.calls[0].request.body)
        for instance in data_sent_to_api["instances"]:
            assert {"spec": "spec"} == instance

    @pytest.fixture
    def mock_put_predict_fail(self, rsps):
        rsps.add(
            rsps.POST,
            VERSIONS_API._get_base_url_with_base_path() + "/analytics/models/model1/versions/version1/predict",
            json={"error": {"message": "User error", "code": 200}},
        )
        yield rsps

    def test_predict_on_model_version_prediction_error(self, mock_put_predict_fail):
        with pytest.raises(PredictionError, match="User error"):
            MODELS_API.online_predict(model_name="model1", version_name="version1")
