import os

import pytest

from openapi.generator import ClassGenerator, CodeGenerator, UpdateClassGenerator

input_path = os.path.join(os.path.dirname(__file__), "input_output/input.py")
output_path = os.path.join(os.path.dirname(__file__), "input_output/output.py")
output_test_path = os.path.join(os.path.dirname(__file__), "input_output/output_test.py")
with open(input_path) as f:
    INPUT = f.read()
with open(output_path) as f:
    OUTPUT = f.read()

if os.getenv("CI") == "1":
    CODE_GENERATOR = CodeGenerator(spec_path="deref-spec.json")
else:
    CODE_GENERATOR = CodeGenerator(spec_url="https://storage.googleapis.com/cognitedata-api-docs/dist/v1.json")


class TestCodeGenerator:
    def test_generated_output(self):
        CODE_GENERATOR.generate(input_path, output_test_path)
        with open(output_test_path) as f:
            output = f.read()
        assert OUTPUT == output

    def test_output_unchanged_if_regenerated(self):
        output = CODE_GENERATOR.generate_to_str(output_path)
        assert OUTPUT == output


CLASS_GENERATOR = ClassGenerator(CODE_GENERATOR.open_api_spec, INPUT)

UPDATE_CLASS_GENERATOR = UpdateClassGenerator(CODE_GENERATOR.open_api_spec, INPUT)


class TestUpdateClassGenerator:
    def test_get_gen_method_segments(self):
        segments = UPDATE_CLASS_GENERATOR.gen_update_class_segments
        assert ("AssetChange", "AssetUpdate") == segments[0]
        assert "AssetUpdate" == segments[0].class_name
        assert "AssetChange" == segments[0].schema_name

    def test_gen_docstring(self):
        docstring = UPDATE_CLASS_GENERATOR.generate_docstring(
            CLASS_GENERATOR._spec.components.schemas.get("AssetChange"), indentation=4
        )
        assert (
            """    \"\"\"Changes applied to asset

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
    \"\"\""""
            == docstring
        )

    def test_gen_setters(self):
        setters = UPDATE_CLASS_GENERATOR.generate_setters(
            CLASS_GENERATOR._spec.components.schemas.get("EventChange"), "EventUpdate", indentation=4
        )
        assert (
            """    @property
    def external_id(self):
        return EventUpdate._PrimitiveEventUpdate(self, 'externalId')

    @property
    def data_set_id(self):
        return EventUpdate._PrimitiveEventUpdate(self, 'dataSetId')

    @property
    def start_time(self):
        return EventUpdate._PrimitiveEventUpdate(self, 'startTime')

    @property
    def end_time(self):
        return EventUpdate._PrimitiveEventUpdate(self, 'endTime')

    @property
    def description(self):
        return EventUpdate._PrimitiveEventUpdate(self, 'description')

    @property
    def metadata(self):
        return EventUpdate._ObjectEventUpdate(self, 'metadata')

    @property
    def asset_ids(self):
        return EventUpdate._ListEventUpdate(self, 'assetIds')

    @property
    def source(self):
        return EventUpdate._PrimitiveEventUpdate(self, 'source')

    @property
    def type(self):
        return EventUpdate._PrimitiveEventUpdate(self, 'type')

    @property
    def subtype(self):
        return EventUpdate._PrimitiveEventUpdate(self, 'subtype')"""
            == setters
        )

    def test_generate_attr_update_classes(self):
        attr_update_classes = UPDATE_CLASS_GENERATOR.generate_attr_update_classes("AssetUpdate", 0)
        assert (
            """class _PrimitiveAssetUpdate(CognitePrimitiveUpdate):
    def set(self, value: Any) -> "AssetUpdate":
        return self._set(value)

class _ObjectAssetUpdate(CogniteObjectUpdate):
    def set(self, value: Dict) -> "AssetUpdate":
        return self._set(value)

    def add(self, value: Dict) -> "AssetUpdate":
        return self._add(value)

    def remove(self, value: List) -> "AssetUpdate":
        return self._remove(value)

class _ListAssetUpdate(CogniteListUpdate):
    def set(self, value: List) -> "AssetUpdate":
        return self._set(value)

    def add(self, value: List) -> "AssetUpdate":
        return self._add(value)

    def remove(self, value: List) -> "AssetUpdate":
        return self._remove(value)

class _LabelAssetUpdate(CogniteLabelUpdate):
    def add(self, value: List) -> "AssetUpdate":
        return self._add(value)

    def remove(self, value: List) -> "AssetUpdate":
        return self._remove(value)"""
            == attr_update_classes
        )

    def test_generate_code(self):
        schema = UPDATE_CLASS_GENERATOR._spec.components.schemas.get("AssetChange")
        docstring = UPDATE_CLASS_GENERATOR.generate_docstring(schema, indentation=4)
        setters = UPDATE_CLASS_GENERATOR.generate_setters(schema, "AssetUpdate", indentation=4)
        attr_update_classes = UPDATE_CLASS_GENERATOR.generate_attr_update_classes("AssetUpdate", 4)

        generated = UPDATE_CLASS_GENERATOR.generate_code_for_class_segments()["AssetUpdate"]
        assert generated == docstring + "\n" + attr_update_classes + "\n" + setters
