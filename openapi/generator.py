import os
import re
from collections import namedtuple

from black import FileMode, format_str

from openapi import utils
from openapi.openapi import OpenAPISpec
from openapi.utils import TYPE_MAPPING

TO_EXCLUDE = ["project", "cursor"]
GEN_CLASS_PATTERN = "# GenClass: ([\S ]+)\s+class (\S+)\(.+\):(?:(?!# GenStop)[\s\S])+# GenStop"
GEN_UPDATE_CLASS_PATTERN = "# GenUpdateClass: (\S+)\s+class (\S+)\(.+\):(?:(?!# GenStop)[\s\S])+# GenStop"

GenClassSegment = namedtuple("GenClassSegment", ["schema_names", "class_name"])
GenUpdateClassSegment = namedtuple("GenUpdateClassSegment", ["schema_name", "class_name"])


class ClassGenerator:
    def __init__(self, spec, input):
        self._spec = spec
        self._input = input
        self.gen_class_segments = [GenClassSegment(*args) for args in re.findall(GEN_CLASS_PATTERN, self._input)]

    def generate_code_for_class_segments(self):
        generated_segments = {}
        for class_segment in self.gen_class_segments:
            schema_names = class_segment.schema_names.split(", ")
            schemas = []
            for schema_name in schema_names:
                res = re.match("(.*)\.(.*)", schema_name)
                if res is not None:
                    schema_name = res.group(1)
                    property_name = res.group(2)
                    schema = self._spec.components.schemas.get(schema_name)
                    property = self._get_schema_properties(schema)[property_name]
                    schemas.append(property)
                else:
                    schemas.append(self._spec.components.schemas.get(schema_name))
            docstring = self.generate_docstring(schemas, indentation=4)
            constructor_args = self.generate_constructor(schemas, indentation=4)
            generated_segment = docstring + "\n" + constructor_args
            generated_segments[class_segment.class_name] = generated_segment
        return generated_segments

    def generate_docstring(self, schemas, indentation):
        docstring = " " * indentation + '"""{}\n\n'.format(self._get_schema_description(schemas[0]))
        docstring += " " * indentation + "Args:\n"
        ignore = [p for p in TO_EXCLUDE]
        for schema in schemas:
            for prop_name, prop in self._get_schema_properties(schema).items():
                if prop_name not in ignore:
                    docstring += " " * (indentation + 4) + "{} ({}): {}\n".format(
                        utils.to_snake_case(prop_name), utils.get_type_hint(prop), self._get_schema_description(prop)
                    )
                    ignore.append(prop_name)
        docstring += " " * indentation + '"""'
        return docstring

    def generate_constructor(self, schemas, indentation):
        constructor_params = [" " * indentation + "def __init__(self"]
        ignore = [p for p in TO_EXCLUDE]
        for schema in schemas:
            for prop_name, prop in self._get_schema_properties(schema).items():
                prop_name = utils.to_snake_case(prop_name)
                req = "" if prop.get("required", False) else " = None"
                if prop_name not in ignore:
                    constructor_params.append("{}: {}{}".format(prop_name, utils.get_type_hint(prop), req))
                    ignore.append(prop_name)
        constructor_params = ", ".join(constructor_params) + ", **kwargs):"
        constructor_body = ""
        ignore = [p for p in TO_EXCLUDE]
        for schema in schemas:
            for prop_name, prop in self._get_schema_properties(schema).items():
                prop_name = utils.to_snake_case(prop_name)
                if prop_name not in ignore:
                    constructor_body += " " * (indentation + 4) + "self.{} = {}\n".format(prop_name, prop_name)
                    ignore.append(prop_name)
        return constructor_params + "\n" + constructor_body[:-1]

    @staticmethod
    def _get_schema_description(schema):
        if "allOf" in schema:
            schema = schema["allOf"][0]
        return schema.get("description", "No description.")

    @staticmethod
    def _get_schema_properties(schema):
        if "allOf" in schema:
            properties = {}
            for s in schema["allOf"]:
                for prop_name, prop in s["properties"].items():
                    properties[prop_name] = prop
            return properties
        return schema["properties"]


class UpdateClassGenerator:
    def __init__(self, spec, input):
        self._spec = spec
        self._input = input
        self.gen_update_class_segments = [
            GenUpdateClassSegment(*args) for args in re.findall(GEN_UPDATE_CLASS_PATTERN, self._input)
        ]

    def generate_code_for_class_segments(self):
        generated_segments = {}
        for class_segment in self.gen_update_class_segments:
            schema = self._spec.components.schemas.get(class_segment.schema_name)
            docstring = self.generate_docstring(schema, indentation=4)
            setters = self.generate_setters(schema, indentation=4)
            attr_update_classes = self.generate_attr_update_classes(class_segment.class_name)
            generated_segment = docstring + "\n" + setters + "\n" + attr_update_classes
            generated_segments[class_segment.class_name] = generated_segment
        return generated_segments

    def generate_docstring(self, schema, indentation):
        docstring = " " * indentation + '"""{}\n\n'.format(self._get_schema_description(schema))
        docstring += " " * indentation + "Args:\n"
        for prop_name, prop in self._get_schema_properties(schema).items():
            indent = " " * (indentation + 4)
            if prop_name == "id":
                docstring += indent + "id (int): {}\n".format(self._get_schema_description(prop))
            elif prop_name == "externalId":
                docstring += indent + "external_id (str): {}\n".format(self._get_schema_description(prop))
        docstring += " " * indentation + '"""'
        return docstring

    def generate_setters(self, schema, indentation):
        setters = []
        schema_properties = self._get_schema_properties(schema)
        if "update" in schema_properties:
            update_schema = schema_properties["update"]
        else:
            update_schema = schema
        indent = " " * indentation
        for prop_name, prop in self._get_schema_properties(update_schema).items():
            if prop_name == "id":
                continue
            update_prop_type_hints = {p: type_hint for p, type_hint in self._get_update_properties(prop)}
            if "set" in update_prop_type_hints:
                setter = indent + "@property\n"
                setter += indent + "def {}(self):\n".format(utils.to_snake_case(prop_name))
                if update_prop_type_hints["set"] == "List":
                    setter += indent + indent + "return _ListUpdate(self, '{}')".format(prop_name)
                elif update_prop_type_hints["set"] == "Dict[str, Any]":
                    setter += indent + indent + "return _ObjectUpdate(self, '{}')".format(prop_name)
                else:
                    setter += indent + indent + "return _PrimitiveUpdate(self, '{}')".format(prop_name)
                setters.append(setter)
        return "\n\n".join(setters)

    def generate_attr_update_classes(self, class_name):
        update_classes = []
        update_class_methods = {
            "_PrimitiveUpdate": [("set", "Any")],
            "_ObjectUpdate": [("set", "Dict"), ("add", "Dict"), ("remove", "List")],
            "_ListUpdate": [("set", "List"), ("add", "List"), ("remove", "List")],
        }
        indent = " " * 4
        for update_class_name, methods in update_class_methods.items():
            update_methods = []
            for method, value_type in methods:
                update_method = indent + "def {}(self, value: {}) -> {}:\n".format(method, value_type, class_name)
                update_method += indent + indent + "return self._{}(value)".format(method)
                update_methods.append(update_method)
            update_class = "class {}(Cognite{}):\n{}".format(
                update_class_name, update_class_name.lstrip("_"), "\n\n".join(update_methods)
            )
            update_classes.append(update_class)
        return "\n\n".join(update_classes)

    @staticmethod
    def _get_update_properties(property_update_schema):
        update_properties = []
        if "oneOf" in property_update_schema:
            update_objects = property_update_schema["oneOf"]
            for update_obj in update_objects:
                update_properties.extend(UpdateClassGenerator._get_update_properties(update_obj))
            return update_properties
        for property_name, property in property_update_schema["properties"].items():
            if property_name != "setNull":
                update_properties.append((property_name, TYPE_MAPPING[property["type"]]))
        return update_properties

    @staticmethod
    def _get_schema_description(schema):
        if "allOf" in schema:
            schema = schema["allOf"][0]
        elif "oneOf" in schema:
            return UpdateClassGenerator._get_schema_description(schema["oneOf"][0])
        return schema.get("description", "No description.")

    @staticmethod
    def _get_schema_properties(schema):
        if "allOf" in schema:
            properties = {}
            for s in schema["allOf"]:
                for prop_name, prop in s["properties"].items():
                    properties[prop_name] = prop
            return properties
        if "oneOf" in schema:
            assert len(schema["oneOf"]) == 2, "oneOf contains {} schemas, expected 2".format(len(schema["oneOf"]))
            first_schema_properties = UpdateClassGenerator._get_schema_properties(schema["oneOf"][0])
            second_schema_properties = UpdateClassGenerator._get_schema_properties(schema["oneOf"][1])
            diff = list(set(first_schema_properties) - set(second_schema_properties))
            assert diff == ["id"] or diff == ["externalId"]
            properties = {}
            properties.update(first_schema_properties)
            properties.update(second_schema_properties)
            return properties
        return schema["properties"]


class CodeGenerator:
    def __init__(self, spec_url: str = None, spec_path: str = None):
        self.open_api_spec = OpenAPISpec(url=spec_url, path=spec_path)

    def generate(self, input: str, output):
        generated = self.generate_to_str(input)
        with open(output, "w") as f:
            f.write(generated)

    def generate_to_str(self, input: str):
        input = self._parse_input(input)
        class_generator = ClassGenerator(self.open_api_spec, input)
        update_class_generator = UpdateClassGenerator(self.open_api_spec, input)

        content_with_generated_classes = self._generate_classes(input, class_generator)
        content_with_generated_update_classes = self._generate_update_classes(
            content_with_generated_classes, update_class_generator
        )
        content_with_imports = self._generate_imports(content_with_generated_update_classes)
        content_formatted = self._format_with_black(content_with_imports)

        return content_formatted

    def _generate_classes(self, content, class_generator):
        generated_class_segments = class_generator.generate_code_for_class_segments()
        for cls_name, code_segment in generated_class_segments.items():
            pattern = self._get_gen_class_replace_pattern(cls_name)
            replace_with = self._get_gen_class_replace_string(cls_name, code_segment)
            content = re.sub(pattern, replace_with, content)
        return content

    def _generate_update_classes(self, content, method_generator):
        generated_class_segments = method_generator.generate_code_for_class_segments()
        for operation_id, code_segment in generated_class_segments.items():
            pattern = self._get_gen_update_class_replace_pattern(operation_id)
            replace_with = self._get_gen_update_class_replace_string(operation_id, code_segment)
            content = re.sub(pattern, replace_with, content)
        return content

    def _generate_imports(self, content):
        if re.search("from typing import \*", content) is None:
            content = "from typing import *\n\n" + content
        return content

    def _format_with_black(self, content):
        return format_str(src_contents=content, mode=FileMode(line_length=120))

    def _get_gen_class_replace_pattern(self, class_name):
        return "# GenClass: ([\S ]+)\s+class {}\((.+)\):(?:(?!# GenStop)[\s\S])+# GenStop".format(class_name)

    def _get_gen_class_replace_string(self, class_name, code_segment):
        return r"# GenClass: \1\nclass {}(\2):\n{}\n    # GenStop".format(class_name, code_segment)

    def _get_gen_update_class_replace_pattern(self, class_name):
        return "# GenUpdateClass: (\S+)\s+class {}\((.+)\):(?:(?!# GenStop)[\s\S])+# GenStop".format(class_name)

    def _get_gen_update_class_replace_string(self, class_name, code_segment):
        return r"# GenUpdateClass: \1\nclass {}(\2):\n{}\n    # GenStop".format(class_name, code_segment)

    @staticmethod
    def _parse_input(input):
        if re.match("^([^/]*/)*[^/]+\.py$", input):
            if not os.path.isfile(input):
                raise AssertionError("{} is not a python file or does not exist.".format(input))
            return CodeGenerator._read_file(input)
        return input

    @staticmethod
    def _read_file(path):
        with open(path) as f:
            return f.read()
