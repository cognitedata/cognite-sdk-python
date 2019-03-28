import os

from openapi.generator import ClassGenerator, CodeGenerator, UpdateClassGenerator

input_path = os.path.join(os.path.dirname(__file__), "input_output/input.py")
output_path = os.path.join(os.path.dirname(__file__), "input_output/output.py")
output_test_path = os.path.join(os.path.dirname(__file__), "input_output/output_test.py")
with open(input_path) as f:
    INPUT = f.read()
with open(output_path) as f:
    OUTPUT = f.read()

spec_path = os.path.join(os.path.dirname(__file__), "input_output/test_spec_dereferenced.json")
CODE_GENERATOR = CodeGenerator(spec_path=spec_path)


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
        assert ("Asset, AssetReferences", "Asset") == segments[0]
        assert ("AssetFilter.filter", "AssetFilter") == segments[1]

        assert "Asset" == segments[0].class_name
        assert "Asset, AssetReferences" == segments[0].schema_names

    def test_generate_docstring_from_schema(self):
        schemas = [
            CLASS_GENERATOR._spec.components.schemas.get("Asset"),
            CLASS_GENERATOR._spec.components.schemas.get("AssetReferences"),
        ]
        docstring = CLASS_GENERATOR.generate_docstring(schemas, 4)
        assert (
            """    \"\"\"Representation of a physical asset, e.g plant or piece of equipment

    Args:
        external_id (str): External Id provided by client. Should be unique within the project
        name (str): Name of asset. Often referred to as tag.
        parent_id (int): Javascript friendly internal ID given to the object.
        description (str): Description of asset.
        metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value
        source (str): The source of this asset
        id (int): Javascript friendly internal ID given to the object.
        last_updated_time (int): It is the number of seconds that have elapsed since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        path (List[int]): IDs of assets on the path to the asset.
        depth (int): Asset path depth (number of levels below root node).
        ref_id (str): Reference ID used only in post request to disambiguate references to duplicate names.
        parent_ref_id (str): Reference ID of parent, to disambiguate if multiple nodes have the same name
    \"\"\""""
            == docstring
        )

    def test_generate_constructor_from_schema(self):
        schemas = [
            CLASS_GENERATOR._spec.components.schemas.get("Asset"),
            CLASS_GENERATOR._spec.components.schemas.get("AssetReferences"),
        ]
        constructor = CLASS_GENERATOR.generate_constructor(schemas, indentation=4)
        assert (
            """    def __init__(self, external_id: str = None, name: str = None, parent_id: int = None, description: str = None, metadata: Dict[str, Any] = None, source: str = None, id: int = None, last_updated_time: int = None, path: List[int] = None, depth: int = None, ref_id: str = None, parent_ref_id: str = None):
        self.external_id = external_id
        self.name = name
        self.parent_id = parent_id
        self.description = description
        self.metadata = metadata
        self.source = source
        self.id = id
        self.last_updated_time = last_updated_time
        self.path = path
        self.depth = depth
        self.ref_id = ref_id
        self.parent_ref_id = parent_ref_id"""
            == constructor
        )

    def test_generate_code_for_class_segments(self):
        class_segments = CLASS_GENERATOR.generate_code_for_class_segments()
        schemas = [
            CLASS_GENERATOR._spec.components.schemas.get("Asset"),
            CLASS_GENERATOR._spec.components.schemas.get("AssetReferences"),
        ]
        docstring = CLASS_GENERATOR.generate_docstring(schemas, indentation=4)
        constructor = CLASS_GENERATOR.generate_constructor(schemas, indentation=4)
        assert class_segments["Asset"] == docstring + "\n" + constructor


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
            """    \"\"\"Changes will be applied to event.

    Args:
        id (int): Javascript friendly internal ID given to the object.
        external_id (str): External Id provided by client. Should be unique within the project
    \"\"\""""
            == docstring
        )

    def test_gen_constructor(self):
        constructor = UPDATE_CLASS_GENERATOR.generate_constructor(
            CLASS_GENERATOR._spec.components.schemas.get("AssetChange"), indentation=4
        )
        assert (
            """    def __init__(self, id: int = None, external_id: str = None):
        self.id = id
        self.external_id = external_id
        self._update_object = {}"""
            == constructor
        )

    def test_gen_setters(self):
        setters = UPDATE_CLASS_GENERATOR.generate_setters(
            CLASS_GENERATOR._spec.components.schemas.get("EventChange"), indentation=4
        )
        assert (
            """    def external_id_set(self, value: Union[str, None]):
        if value is None:
            self._update_object['externalId'] = {'setNull': True}
            return self
        self._update_object['externalId'] = {'set': value}
        return self


    def start_time_set(self, value: Union[int, None]):
        if value is None:
            self._update_object['startTime'] = {'setNull': True}
            return self
        self._update_object['startTime'] = {'set': value}
        return self


    def end_time_set(self, value: Union[int, None]):
        if value is None:
            self._update_object['endTime'] = {'setNull': True}
            return self
        self._update_object['endTime'] = {'set': value}
        return self


    def description_set(self, value: Union[str, None]):
        if value is None:
            self._update_object['description'] = {'setNull': True}
            return self
        self._update_object['description'] = {'set': value}
        return self


    def metadata_set(self, value: Union[Dict[str, Any], None]):
        if value is None:
            self._update_object['metadata'] = {'setNull': True}
            return self
        self._update_object['metadata'] = {'set': value}
        return self


    def asset_ids_add(self, value: List):
        self._update_object['assetIds'] = {'add': value}
        return self


    def asset_ids_remove(self, value: List):
        self._update_object['assetIds'] = {'remove': value}
        return self


    def asset_ids_set(self, value: Union[List, None]):
        if value is None:
            self._update_object['assetIds'] = {'setNull': True}
            return self
        self._update_object['assetIds'] = {'set': value}
        return self


    def source_set(self, value: Union[str, None]):
        if value is None:
            self._update_object['source'] = {'setNull': True}
            return self
        self._update_object['source'] = {'set': value}
        return self
"""
            == setters
        )

    def test_generate_code(self):
        schema = UPDATE_CLASS_GENERATOR._spec.components.schemas.get("AssetChange")
        docstring = UPDATE_CLASS_GENERATOR.generate_docstring(schema, indentation=4)
        constructor = UPDATE_CLASS_GENERATOR.generate_constructor(schema, indentation=4)
        setters = UPDATE_CLASS_GENERATOR.generate_setters(schema, indentation=4)

        generated = UPDATE_CLASS_GENERATOR.generate_code_for_class_segments()["AssetUpdate"]
        assert generated == docstring + "\n" + constructor + "\n" + setters
