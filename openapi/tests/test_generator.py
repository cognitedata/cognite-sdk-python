import os

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


class TestClassGenerator:
    def test_get_gen_class_segments(self):
        segments = CLASS_GENERATOR.gen_class_segments
        assert ("Asset", "Asset") == segments[0]
        assert ("AssetFilter.filter", "AssetFilter") == segments[1]

        assert "Asset" == segments[0].class_name
        assert "Asset" == segments[0].schema_names

    def test_generate_docstring_from_schema(self):
        schemas = [CLASS_GENERATOR._spec.components.schemas.get("Asset")]
        docstring = CLASS_GENERATOR.generate_docstring(schemas, 4)
        assert (
            """    \"\"\"A representation of a physical asset, for example a factory or a piece of equipment.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): The name of the asset.
        parent_id (int): A server-generated ID for the object.
        description (str): The description of the asset.
        metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        source (str): The source of the asset.
        id (int): A server-generated ID for the object.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        root_id (int): A server-generated ID for the object.
        aggregates (Dict[str, Any]): Aggregated metrics of the asset
        cognite_client (CogniteClient): The client to associate with this object.
    \"\"\""""
            == docstring
        )

    def test_generate_constructor_from_schema(self):
        schemas = [CLASS_GENERATOR._spec.components.schemas.get("Asset")]
        constructor = CLASS_GENERATOR.generate_constructor(schemas, indentation=4)
        assert (
            """    def __init__(self, external_id: str = None, name: str = None, parent_id: int = None, description: str = None, metadata: Dict[str, str] = None, source: str = None, id: int = None, created_time: int = None, last_updated_time: int = None, root_id: int = None, aggregates: Dict[str, Any] = None, cognite_client = None):
        self.external_id = external_id
        self.name = name
        self.parent_id = parent_id
        self.description = description
        self.metadata = metadata
        self.source = source
        self.id = id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.root_id = root_id
        self.aggregates = aggregates
        self._cognite_client = cognite_client"""
            == constructor
        )

    def test_generate_loader_from_schema(self):
        schemas = [CLASS_GENERATOR._spec.components.schemas.get("AssetFilter")["properties"]["filter"]]
        loader = CLASS_GENERATOR.generate_loader(schemas, "AssetFilter", indentation=4)
        assert (
            """
    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        instance = super(AssetFilter, cls)._load(resource, cognite_client)
        if isinstance(resource, Dict):
            if instance.created_time is not None:
                setattr(instance, "created_time", EpochTimestampRange(**instance.created_time))
            if instance.last_updated_time is not None:
                setattr(instance, "last_updated_time", EpochTimestampRange(**instance.last_updated_time))
        return instance
"""
            == loader
        )

    def test_generate_code_for_class_segments(self):
        class_segments = CLASS_GENERATOR.generate_code_for_class_segments()
        schemas = [CLASS_GENERATOR._spec.components.schemas.get("Asset")]
        docstring = CLASS_GENERATOR.generate_docstring(schemas, indentation=4)
        constructor = CLASS_GENERATOR.generate_constructor(schemas, indentation=4)
        loader = CLASS_GENERATOR.generate_loader(schemas, "Asset", indentation=4)
        assert class_segments["Asset"] == docstring + "\n" + constructor + loader


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
        return _PrimitiveEventUpdate(self, 'externalId')

    @property
    def start_time(self):
        return _PrimitiveEventUpdate(self, 'startTime')

    @property
    def end_time(self):
        return _PrimitiveEventUpdate(self, 'endTime')

    @property
    def description(self):
        return _PrimitiveEventUpdate(self, 'description')

    @property
    def metadata(self):
        return _ObjectEventUpdate(self, 'metadata')

    @property
    def asset_ids(self):
        return _ListEventUpdate(self, 'assetIds')

    @property
    def source(self):
        return _PrimitiveEventUpdate(self, 'source')

    @property
    def type(self):
        return _PrimitiveEventUpdate(self, 'type')

    @property
    def subtype(self):
        return _PrimitiveEventUpdate(self, 'subtype')"""
            == setters
        )

    def test_generate_attr_update_classes(self):
        attr_update_classes = UPDATE_CLASS_GENERATOR.generate_attr_update_classes("AssetUpdate")
        assert (
            """class _PrimitiveAssetUpdate(CognitePrimitiveUpdate):
    def set(self, value: Any) -> AssetUpdate:
        return self._set(value)

class _ObjectAssetUpdate(CogniteObjectUpdate):
    def set(self, value: Dict) -> AssetUpdate:
        return self._set(value)

    def add(self, value: Dict) -> AssetUpdate:
        return self._add(value)

    def remove(self, value: List) -> AssetUpdate:
        return self._remove(value)

class _ListAssetUpdate(CogniteListUpdate):
    def set(self, value: List) -> AssetUpdate:
        return self._set(value)

    def add(self, value: List) -> AssetUpdate:
        return self._add(value)

    def remove(self, value: List) -> AssetUpdate:
        return self._remove(value)"""
            == attr_update_classes
        )

    def test_generate_code(self):
        schema = UPDATE_CLASS_GENERATOR._spec.components.schemas.get("AssetChange")
        docstring = UPDATE_CLASS_GENERATOR.generate_docstring(schema, indentation=4)
        setters = UPDATE_CLASS_GENERATOR.generate_setters(schema, "AssetUpdate", indentation=4)
        attr_update_classes = UPDATE_CLASS_GENERATOR.generate_attr_update_classes("AssetUpdate")

        generated = UPDATE_CLASS_GENERATOR.generate_code_for_class_segments()["AssetUpdate"]
        assert generated == docstring + "\n" + setters + "\n" + attr_update_classes
