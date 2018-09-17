import sys
from unittest import mock

import pytest

import cognite.v06.models
from cognite.cli.cli import CogniteCLI
from cognite.cli.cli_models import CogniteModelsCLI


@pytest.fixture(autouse=True)
def cognite_utils_request_mock(monkeypatch):
    response_mock = mock.MagicMock()

    def mockreturn(*args, **kwargs):
        return response_mock

    monkeypatch.setattr(cognite.v06.models.utils, "get_request", mockreturn)
    monkeypatch.setattr(cognite.v06.models.utils, "delete_request", mockreturn)
    monkeypatch.setattr(cognite.v06.models.utils, "post_request", mockreturn)
    monkeypatch.setattr(cognite.v06.models.utils, "put_request", mockreturn)
    yield response_mock


class TestModelsCLI:
    def test_get_models(self, cognite_utils_request_mock, mocker):
        sys.argv = ["cognite", "models", "get"]
        mocker.spy(cognite.v06.models, "get_models")
        CogniteCLI()
        assert 1 == cognite.v06.models.get_models.call_count
        assert 1 == cognite_utils_request_mock.json.call_count

    def test_get_versions(self, cognite_utils_request_mock, mocker):
        sys.argv = ["cognite", "models", "get", "123"]
        mocker.spy(cognite.v06.models, "get_model_versions")
        CogniteCLI()
        assert 1 == cognite_utils_request_mock.json.call_count
        assert 1 == cognite.v06.models.get_model_versions.call_count

    def test_get_source_packages(self, cognite_utils_request_mock, mocker):
        sys.argv = ["cognite", "models", "get", "-s"]
        mocker.spy(cognite.v06.models, "get_model_source_packages")
        CogniteCLI()
        assert 1 == cognite_utils_request_mock.json.call_count
        assert 1 == cognite.v06.models.get_model_source_packages.call_count

    def test_source_command(self, mocker):
        copytree_mock = mocker.patch("shutil.copytree")
        sys.argv = ["cognite", "models", "source", "mysourcepackage"]
        CogniteCLI()
        assert 1 == copytree_mock.call_count

    def test_verify_source_package_no_setup_file(self, mocker):
        mocker.patch("os.walk", return_value=[])
        models_cli = CogniteModelsCLI()
        with pytest.raises(AssertionError) as e:
            models_cli._verify_source_package("a/fake/directory/")
        assert e.value.args[0] == "Your package must contain a setup.py file"

    def test_verify_source_package_no_model_file(self, mocker):
        mocker.patch("os.walk", return_value=[("root_dir", [], ["setup.py"])])
        models_cli = CogniteModelsCLI()
        with pytest.raises(AssertionError) as e:
            models_cli._verify_source_package("a/fake/directory/")
        assert e.value.args[0] == "Your package must contain a model.py file"

    def test_verify_source_package_no_model_class(self, mocker):
        mocker.patch("importlib.util.spec_from_file_location", return_value=mocker.MagicMock())
        mocker.patch("importlib.util.module_from_spec")
        mocker.patch("cognite.cli.cli_models.hasattr", return_value=False)
        mocker.patch(
            "os.walk", return_value=[("root_dir", ["model"], ["setup.py"]), ("root_dir/model", [], ["model.py"])]
        )
        models_cli = CogniteModelsCLI()
        with pytest.raises(AssertionError) as e:
            models_cli._verify_source_package("a/fake/directory/")
        assert e.value.args[0] == "model.py must contain a class named Model"

    def test_verify_source_package_no_available_ops(self, mocker):
        def hasattr_mock(object, attr):
            return attr == "Model"

        mocker.patch("importlib.util.spec_from_file_location", return_value=mocker.MagicMock())
        mocker.patch("importlib.util.module_from_spec")
        mocker.patch("cognite.cli.cli_models.hasattr", side_effect=hasattr_mock)
        mocker.patch(
            "os.walk", return_value=[("root_dir", ["model"], ["setup.py"]), ("root_dir/model", [], ["model.py"])]
        )
        models_cli = CogniteModelsCLI()
        with pytest.raises(AssertionError) as e:
            models_cli._verify_source_package("a/fake/directory/")
        assert e.value.args[0] == "Your model class must define at least a train or predict routine."

    def test_verify_source_package_has_predict_no_load(self, mocker):
        def hasattr_mock(object, attr):
            return attr in ["Model", "train", "predict"]

        mocker.patch("importlib.util.spec_from_file_location", return_value=mocker.MagicMock())
        mocker.patch("importlib.util.module_from_spec")
        mocker.patch("cognite.cli.cli_models.hasattr", side_effect=hasattr_mock)
        mocker.patch(
            "os.walk", return_value=[("root_dir", ["model"], ["setup.py"]), ("root_dir/model", [], ["model.py"])]
        )
        models_cli = CogniteModelsCLI()
        with pytest.raises(AssertionError) as e:
            models_cli._verify_source_package("a/fake/directory/")
        assert e.value.args[0] == "Your model class must define a 'load' method in order to perform predictions"

    def test_verify_source_package_available_ops(self, mocker):
        def hasattr_mock(object, attr):
            return attr in ["Model", "predict", "load"]

        mocker.patch("importlib.util.spec_from_file_location", return_value=mocker.MagicMock())
        mocker.patch("importlib.util.module_from_spec")
        mocker.patch("cognite.cli.cli_models.hasattr", side_effect=hasattr_mock)
        mocker.patch(
            "os.walk", return_value=[("root_dir", ["model"], ["setup.py"]), ("root_dir/model", [], ["model.py"])]
        )
        models_cli = CogniteModelsCLI()
        info = models_cli._verify_source_package("a/fake/directory/")

        assert info.get("available_operations") == ["predict"]

    def test_deploy(self, mocker):
        models_cli = CogniteModelsCLI()
        mocker.patch.object(models_cli, "_verify_source_package", autospec=True)
        mocker.patch("cognite.cli.cli_models.run_setup")
        upload_sp_mock = mocker.patch("cognite.v06.models.upload_source_package")
        create_model_mock = mocker.patch("cognite.v06.models.create_model")

        models_cli.deploy(["-m", "a_model"])
        assert 1 == upload_sp_mock.call_count
        assert 1 == create_model_mock.call_count
