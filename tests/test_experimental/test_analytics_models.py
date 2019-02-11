from random import randint

import pytest

from cognite import CogniteClient

models = CogniteClient().experimental.analytics.models


@pytest.fixture
def created_model():
    model_name = "test-model-{}".format(randint(0, 1e5))
    model = models.create_model(name=model_name)
    yield model["items"][0]
    models.delete_model(model["items"][0]["id"])


@pytest.fixture
def created_source_package():
    sp_name = "test-sp-{}".format(randint(0, 1e5))
    sp = models.create_source_package(
        name=sp_name, package_name="whatever", available_operations=["TRAIN", "PREDICT"], runtime_version="0.1"
    )
    yield sp
    models.delete_source_package(source_package_id=sp["id"])


class TestModels:
    def test_get_model(self, created_model):
        model = models.get_model(created_model["id"])["items"][0]
        assert model["name"] == created_model["name"]

    def test_get_source_package(self, created_source_package):
        sp = models.get_source_package(created_source_package["id"])["items"][0]
        assert sp["id"] == created_source_package["id"]
