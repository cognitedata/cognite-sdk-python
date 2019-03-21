import os

from openapi.generator import ClassGenerator, CodeGenerator, MethodGenerator
from tests.tests_unit.tests_openapi.utils import SPEC_URL

input_path = os.path.join(os.path.dirname(__file__), "input_output/input.py")
output_path = os.path.join(os.path.dirname(__file__), "input_output/output.py")
output_test_path = os.path.join(os.path.dirname(__file__), "input_output/output_test.py")
with open(input_path) as f:
    INPUT = f.read()
with open(output_path) as f:
    OUTPUT = f.read()

CODE_GENERATOR = CodeGenerator(SPEC_URL)


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
        assert ("GetFieldValuesDTO", "Field") == segments[0]
        assert ("AssetV2", "Asset") == segments[1]

        assert "Asset" == segments[1].class_name
        assert "AssetV2" == segments[1].schema_name

    def test_generate_docstring_from_schema(self):
        docstring = CLASS_GENERATOR.generate_docstring(CLASS_GENERATOR._spec.components.get_schema("AssetV2"), 4)
        assert (
            """    \"\"\"Representation of a physical asset, e.g plant or piece of equipment\n
    Args:
        id (int): ID of the asset.
        path (List[int]): IDs of assets on the path to the asset.
        depth (int): Asset path depth (number of levels below root node).
        name (str): Name of asset. Often referred to as tag.
        parent_id (int): ID of parent asset, if any
        description (str): Description of asset.
        types (List[Field]): The field specific values of the asset.
        metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value
        source (str): The source of this asset
        source_id (str): ID of the asset in the source. Only applicable if source is specified. The combination of source and sourceId must be unique.
        created_time (int): Time when this asset was created in CDP in milliseconds since Jan 1, 1970.
        last_updated_time (int): The last time this asset was updated in CDP, in milliseconds since Jan 1, 1970.
    \"\"\""""
            == docstring
        )

    def test_generate_constructor_from_schema(self):
        constructor = CLASS_GENERATOR.generate_constructor(
            CLASS_GENERATOR._spec.components.get_schema("AssetV2"), indentation=4
        )
        assert (
            """    def __init__(self, id: int = None, path: List[int] = None, depth: int = None, name: str = None, parent_id: int = None, description: str = None, types: List[Field] = None, metadata: Dict[str, Any] = None, source: str = None, source_id: str = None, created_time: int = None, last_updated_time: int = None):
        self.id = id
        self.path = path
        self.depth = depth
        self.name = name
        self.parent_id = parent_id
        self.description = description
        self.types = types
        self.metadata = metadata
        self.source = source
        self.source_id = source_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time"""
            == constructor
        )

    def test_generate_code_for_class_segments(self):
        class_segments = CLASS_GENERATOR.generate_code_for_class_segments()
        docstring = CLASS_GENERATOR.generate_docstring(
            CLASS_GENERATOR._spec.components.get_schema("AssetV2"), indentation=4
        )
        constructor = CLASS_GENERATOR.generate_constructor(
            CLASS_GENERATOR._spec.components.get_schema("AssetV2"), indentation=4
        )
        assert class_segments["Asset"] == docstring + "\n" + constructor


METHOD_GENERATOR = MethodGenerator(CODE_GENERATOR.open_api_spec, INPUT)


class TestMethodGenerator:
    def test_get_gen_method_segments(self):
        segments = METHOD_GENERATOR.gen_method_segments
        assert ("getAssets", "Asset", "get", "self") == segments[0]
        assert ("postAssets", "Union[Asset, List[Asset]]", "post", "self, nana") == segments[1]

        assert "getAssets" == segments[0].operation_id
        assert "Asset" == segments[0].return_type
        assert "get" == segments[0].method
        assert "self" == segments[0].params

    def test_generate_docstring_from_operation_get(self):
        docstring = METHOD_GENERATOR.generate_docstring(
            METHOD_GENERATOR._spec.get_operation("getAssets"), indentation=8
        )
        assert (
            """        \"\"\"List all assets

Retrieve a list of all assets in the given project. The list is sorted alphabetically
by name. This operation supports pagination. You can retrieve a subset of assets
by supplying additional fields; Only assets satisfying all criteria will be returned.
Names and descriptions are fuzzy searched using [edit distance](https://en.wikipedia.org/wiki/Edit_distance).
The fuzziness parameter controls the maximum edit distance when considering matches
for the name and description fields.

        Args:
            name (str): The name of the asset(s) to get.
            depth (int): Get sub assets up to this many levels below the specified path.
            metadata (str): The metadata values used to filter the results. Format is {"key1": "value1", "key2": "value2"}. The maximum number of entries (pairs of key+value) is 64. The maximum length in characters of the sum of all keys and values is 10240. There is also a maximum length of 128 characters per key and 512 per value.
            description (str): Only return assets that contain this description
            source (str): The source of the assets used to filter the results
            limit (int): Limits the number of results to be returned. The maximum results returned by the server is 1000 even if the limit specified is larger.

        Returns:
            Asset:
        \"\"\""""
            == docstring
        )

    def test_generate_method_signature_operation_get(self):
        signature = METHOD_GENERATOR.generate_method_signature(
            METHOD_GENERATOR._spec.get_operation("getAssets"), "get", 4
        )
        assert (
            "    def get(self, name: str = None, depth: int = None, metadata: str = None, description: str = None, "
            "source: str = None, limit: int = None) -> Asset:" == signature
        )

    def test_generate_code_for_method_segments_operation_get(self):
        signature = METHOD_GENERATOR.generate_method_signature(
            METHOD_GENERATOR._spec.get_operation("getAssets"), "get", 4
        )
        docstring = METHOD_GENERATOR.generate_docstring(
            METHOD_GENERATOR._spec.get_operation("getAssets"), indentation=8
        )
        method_segments = METHOD_GENERATOR.generate_code_for_method_segments()
        assert method_segments["getAssets"] == signature + "\n" + docstring

    def test_generate_docstring_from_operation_with_request_body(self):
        docstring = METHOD_GENERATOR.generate_docstring(
            METHOD_GENERATOR._spec.get_operation("postAssets"), indentation=8
        )
        assert (
            """        \"\"\"Create new assets

Create new assets. It is possible to post a maximum of 1000 assets per request

        Args:
            name (str): Name of asset. Often referred to as tag.
            ref_id (str): Reference ID used only in post request to disambiguate references to duplicate names.
            parent_name (str): Name of parent. This parent must exist in the same POST request.
            parent_ref_id (str): Reference ID of parent, to disambiguate if multiple nodes have the same name.
            parent_id (int): ID of parent asset in CDP, if any. If parentName or parentRefId are also specified, this will be ignored.
            description (str): Description of asset.
            source (str): The source of this asset
            source_id (str): ID of the asset in the source. Only applicable if source is specified. The combination of source and sourceId must be unique.
            types (List[Dict[str, Any]]): The field specific values of the asset.
            metadata (Dict[str, Any]): Custom, application specific metadata. Format is {"key1": "value1", "key2": "value2"}. The maximum number of entries (pairs of key+value) is 64. The maximum length in characters of the sum of all keys and values is 10240. There is also a maximum length of 128 characters per key and 512 per value.
            created_time (int): Time when this asset was created in CDP in milliseconds since Jan 1, 1970.
            last_updated_time (int): The last time this asset was updated in CDP, in milliseconds since Jan 1, 1970.

        Returns:
            Union[Asset, List[Asset]]:
        \"\"\""""
            == docstring
        )

    def test_generate_method_signature_from_operation_with_request_body(self):
        signature = METHOD_GENERATOR.generate_method_signature(
            METHOD_GENERATOR._spec.get_operation("postAssets"), "get", 4
        )
        assert (
            "    def get(self, name: str = None, ref_id: str = None, parent_name: str = None, "
            "parent_ref_id: str = None, parent_id: int = None, description: str = None, source: str = None, "
            "source_id: str = None, types: List[Dict[str, Any]] = None, metadata: Dict[str, Any] = None, "
            "created_time: int = None, last_updated_time: int = None) -> Union[Asset, List[Asset]]:" == signature
        )

    def test_generate_code_for_method_segments_operation_with_request_body(self):
        signature = METHOD_GENERATOR.generate_method_signature(
            METHOD_GENERATOR._spec.get_operation("postAssets"), "post", 4
        )
        docstring = METHOD_GENERATOR.generate_docstring(
            METHOD_GENERATOR._spec.get_operation("postAssets"), indentation=8
        )
        method_segments = METHOD_GENERATOR.generate_code_for_method_segments()
        assert method_segments["postAssets"] == signature + "\n" + docstring
