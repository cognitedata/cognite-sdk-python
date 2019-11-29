from random import randint

import pytest

from cognite.client._api.model_hosting.models import PredictionError
from cognite.client.data_classes.model_hosting.models import Model, ModelList
from cognite.client.experimental import CogniteClient

MODELS_API = CogniteClient().model_hosting.models


class TestModels:
    @pytest.fixture
    def created_model(self):
        model_name = "test-model-{}".format(randint(0, 1e5))
        model = MODELS_API.create_model(name=model_name, webhook_url="https://bla.bla")
        yield model
        MODELS_API.delete_model(model_name)

    @pytest.fixture
    def mock_online_predict_ok(self, rsps):
        rsps.add(
            rsps.POST,
            MODELS_API._get_base_url_with_base_path() + "/modelhosting/models/model1/predict",
            status=200,
            json={"predictions": [1, 2, 3]},
        )
        yield rsps

    @pytest.fixture
    def mock_online_predict_fail(self, rsps):
        rsps.add(
            rsps.POST,
            MODELS_API._get_base_url_with_base_path() + "/modelhosting/models/model1/predict",
            status=200,
            json={"error": {"message": "User error", "code": 200}},
        )
        yield rsps

    def test_get_model(self, created_model):
        model = MODELS_API.get_model(created_model.name)
        assert model.name == created_model.name
        assert model.active_version_name is None

    def test_list_models(self, created_model):
        res = MODELS_API.list_models()
        assert len(res) > 0
        assert isinstance(res, ModelList)
        assert isinstance(res[:1], ModelList)
        assert isinstance(res[0], Model)
        for model in res:
            assert isinstance(model, Model)

    def test_update_model(self, created_model):
        res = MODELS_API.update_model(name=created_model.name, description="bla")
        assert isinstance(res, Model)
        assert res.description == "bla"
        model = MODELS_API.get_model(name=created_model.name)
        assert model.description == "bla"

    def test_predict_on_model(self, mock_online_predict_ok):
        predictions = MODELS_API.online_predict(model_name="model1")
        assert predictions == [1, 2, 3]

    def test_predict_on_model_prediction_error(self, mock_online_predict_fail):
        with pytest.raises(PredictionError, match="User error"):
            MODELS_API.online_predict(model_name="model1")

    def test_deprecate_model(self, created_model):
        res = MODELS_API.deprecate_model(name=created_model.name)
        assert isinstance(res, Model)
        assert res.is_deprecated is True
        model = MODELS_API.get_model(name=created_model.name)
        assert model.is_deprecated is True
