import re

import pytest
import requests

from openapi.openapi import Components, OpenAPISpec
from tests.tests_unit.tests_openapi.utils import SPEC_URL


@pytest.fixture(scope="session")
def download_spec():
    spec = requests.get("https://storage.googleapis.com/cognitedata-api-docs/dist/0.6.json").json()
    return spec


SPEC = OpenAPISpec(SPEC_URL)


class TestOpenAPISpec:
    def test_download_spec(self, download_spec):
        assert download_spec == SPEC._spec
        assert download_spec == SPEC.download_spec()

    def test_get_paths(self, download_spec):
        assert list(download_spec["paths"].values()) == [path._operations for path in SPEC.paths]

    def test_get_operation(self, download_spec):
        operation = SPEC.get_operation("getAssets")
        assert download_spec["paths"]["/api/0.6/projects/{project}/assets"]["get"] == operation._operation
        assert "get" == operation.method
        assert "getAssets" == operation.operation_id

    def test_get_operation_does_not_exist(self):
        with pytest.raises(ValueError):
            SPEC.get_operation("blabla")


class TestComponents:
    def test_get_schemas(self, download_spec):
        assert isinstance(SPEC.components, Components)

    def test_get_schema(self, download_spec):
        schema = SPEC.components.get_schema("AssetV2")
        assert download_spec["components"]["schemas"]["AssetV2"] == schema._schema
        assert "AssetV2" == schema.name


OPERATION = SPEC.get_operation("getAssets")


class TestOperation:
    def test_get_description(self):
        assert re.match(re.compile(".+\n\n.+", re.MULTILINE), OPERATION.description)

    def test_get_parameters(self):
        assert OPERATION._operation["parameters"] == [p._parameter for p in OPERATION.params]


SCHEMA = SPEC.components.get_schema("AssetV2")


class TestSchema:
    def test_get_properties(self):
        assert SCHEMA._schema["properties"] == {p.name: p._property for p in SCHEMA.properties}

    def test_get_property(self):
        assert SCHEMA._schema["properties"]["id"] == SCHEMA.get_property("id")._property


class TestProperty:
    def test_get_type__int(self):
        property = SCHEMA.get_property("id")
        assert "integer" == property.type
        assert "int" == property.python_type

    def test_get_type__object_with_ref(self):
        property = SPEC.components.get_schema("ApiKeyResponse").get_property("data")
        assert "DataApiKeyResponseDTO" == property.type
        assert "DataApiKeyResponseDTO" == property.python_type

    def test_get_type__array_int(self):
        property = SPEC.components.get_schema("Asset").get_property("path")
        assert "array" == property.type
        assert "List[int]" == property.python_type

    def test_get_type__object(self):
        property = SPEC.components.get_schema("AssetV2").get_property("metadata")
        assert "object" == property.type
        assert "Dict[str, Any]" == property.python_type

    def test_get_type__array_of_nested_objects(self):
        property = SPEC.components.get_schema("AssetFilterDTO").get_property("types")
        assert "array" == property.type
        assert "List[TypeFilter]" == property.python_type
