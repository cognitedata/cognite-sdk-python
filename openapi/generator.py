import os
import re
from collections import namedtuple

from black import FileMode, format_str

from openapi import utils
from openapi.openapi import OpenAPISpec, Schema

TO_EXCLUDE = ["project", "cursor"]
GEN_CLASS_PATTERN = "# GenClass: (\S+)\s+class (\S+)\(.+\):(?:(?!# GenStop)[\s\S])+# GenStop"
GEN_METHOD_PATTERN = "# GenMethod: (\S+) -> ([\S ]+)\s+def (\S+)\((.+)\):(?:(?!# GenStop)[\s\S])+# GenStop"

GenClassSegment = namedtuple("GenClassSegment", ["schema_name", "class_name"])
GenMethodSegment = namedtuple("GenMethodSegment", ["operation_id", "return_type", "method", "params"])


class ClassGenerator:
    def __init__(self, spec, input):
        self._spec = spec
        self._input = input
        self.gen_class_segments = [GenClassSegment(*args) for args in re.findall(GEN_CLASS_PATTERN, self._input)]

    def generate_code_for_class_segments(self):
        generated_segments = {}
        for class_segment in self.gen_class_segments:
            schema = self._spec.components.get_schema(class_segment.schema_name)
            docstring = self.generate_docstring(schema, indentation=4)
            constructor_args = self.generate_constructor(schema, indentation=4)
            generated_segment = docstring + "\n" + constructor_args
            generated_segments[class_segment.class_name] = generated_segment
        return generated_segments

    def generate_docstring(self, schema: Schema, indentation):
        docstring = " " * indentation + '"""{}\n\n'.format(schema.description)
        docstring += " " * indentation + "Args:\n"
        for prop in schema.properties:
            if prop.name not in TO_EXCLUDE:
                docstring += " " * (indentation + 4) + "{} ({}): {}\n".format(
                    utils.to_snake_case(prop.name),
                    utils.get_full_type_hint(prop.python_type, self.gen_class_segments),
                    prop.description,
                )
        docstring += " " * indentation + '"""'
        return docstring

    def generate_constructor(self, schema: Schema, indentation):
        constructor_params = [" " * indentation + "def __init__(self"]
        for prop in schema.properties:
            prop_name = utils.to_snake_case(prop.name)
            req = "" if prop.required else " = None"
            constructor_params.append(
                "{}: {}{}".format(prop_name, utils.get_full_type_hint(prop.python_type, self.gen_class_segments), req)
            )
        constructor_params = ", ".join(constructor_params) + "):"
        constructor_body = ""
        for prop in schema.properties:
            prop_name = utils.to_snake_case(prop.name)
            constructor_body += " " * (indentation + 4) + "self.{} = {}\n".format(prop_name, prop_name)
        return constructor_params + "\n" + constructor_body[:-1]


class MethodGenerator:
    def __init__(self, spec, input):
        self._spec = spec
        self._input = input
        self.gen_class_segments = [GenClassSegment(*args) for args in re.findall(GEN_CLASS_PATTERN, self._input)]
        self.gen_method_segments = [GenMethodSegment(*args) for args in re.findall(GEN_METHOD_PATTERN, self._input)]

    def generate_code_for_method_segments(self):
        generated_segments = {}
        for segment in self.gen_method_segments:
            operation = self._spec.get_operation(segment.operation_id)
            docstring = self.generate_docstring(operation, indentation=8)
            signature = self.generate_method_signature(operation, segment.method, indentation=4)
            generated_segments[segment.operation_id] = signature + "\n" + docstring
        return generated_segments

    def generate_docstring(self, operation, indentation):
        docstring = " " * indentation + '"""{}\n\n'.format(operation.description)
        docstring += " " * indentation + "Args:\n"

        for param in operation.params:
            if param.name not in TO_EXCLUDE:
                docstring += " " * (indentation + 4) + "{} ({}): {}\n".format(
                    utils.to_snake_case(param.name), utils.get_python_type(param.type), param.description
                )

        if operation.request_schema:
            for prop in operation.request_schema.properties:
                if prop.name not in TO_EXCLUDE:
                    docstring += " " * (indentation + 4) + "{} ({}): {}\n".format(
                        utils.to_snake_case(prop.name),
                        utils.get_full_type_hint(prop.python_type, self.gen_class_segments),
                        prop.description,
                    )

        docstring += "\n" + " " * indentation + "Returns:\n"
        docstring += " " * (indentation + 4) + self._get_return_type_hint(operation) + ":"
        docstring += "\n" + " " * indentation + '"""'
        return docstring

    def generate_method_signature(self, operation, method_name, indentation):
        return_value = self._get_return_type_hint(operation)
        signature = [" " * indentation + "def {}(self".format(method_name)]

        for param in operation.params:
            if param.name not in TO_EXCLUDE:
                req = "" if param.required else " = None"
                signature.append(
                    "{}: {}{}".format(utils.to_snake_case(param.name), utils.get_python_type(param.type), req)
                )

        if operation.request_schema:
            for prop in operation.request_schema.properties:
                if prop.name not in TO_EXCLUDE:
                    prop_name = utils.to_snake_case(prop.name)
                    req = "" if prop.required else " = None"
                    signature.append(
                        "{}: {}{}".format(
                            prop_name, utils.get_full_type_hint(prop.python_type, self.gen_class_segments), req
                        )
                    )

        return ", ".join(signature) + ") -> {}:".format(return_value)

    def _get_return_type_hint(self, operation):
        for segment in self.gen_method_segments:
            if segment.operation_id == operation.operation_id:
                return segment.return_type
        raise ValueError("Operation '{}' does not exist".format(operation.operation_id))


class CodeGenerator:
    def __init__(self, spec_url: str):
        self.open_api_spec = OpenAPISpec(spec_url)

    def generate(self, input: str, output):
        generated = self.generate_to_str(input)
        with open(output, "w") as f:
            f.write(generated)

    def generate_to_str(self, input: str):
        input = self._parse_input(input)
        self.class_generator = ClassGenerator(self.open_api_spec, input)
        self.method_generator = MethodGenerator(self.open_api_spec, input)

        content_with_generated_classes = self._generate_classes(input)
        content_with_generated_methods = self._generate_methods(content_with_generated_classes)
        content_with_imports = self._generate_imports(content_with_generated_methods)
        content_formatted = self._format_with_black(content_with_imports)

        return content_formatted

    def _generate_classes(self, content):
        generated_class_segments = self.class_generator.generate_code_for_class_segments()
        for cls_name, code_segment in generated_class_segments.items():
            pattern = self._get_gen_class_replace_pattern(cls_name)
            replace_with = self._get_gen_class_replace_string(cls_name, code_segment)
            content = re.sub(pattern, replace_with, content)
        return content

    def _generate_methods(self, content):
        generated_method_segments = self.method_generator.generate_code_for_method_segments()
        for operation_id, code_segment in generated_method_segments.items():
            pattern = self._get_gen_method_replace_pattern(operation_id)
            replace_with = self._get_gen_method_replace_string(operation_id, code_segment)
            content = re.sub(pattern, replace_with, content)
        return content

    def _generate_imports(self, content):
        if re.search("from typing import \*", content) is None:
            content = "from typing import *\n\n" + content
        return content

    def _format_with_black(self, content):
        return format_str(src_contents=content, mode=FileMode(line_length=120))

    def _get_gen_class_replace_pattern(self, class_name):
        return "# GenClass: (\S+)\s+class {}\((.+)\):(?:(?!# GenStop)[\s\S])+# GenStop".format(class_name)

    def _get_gen_class_replace_string(self, class_name, code_segment):
        return r"# GenClass: \1\nclass {}(\2):\n{}\n    # GenStop".format(class_name, code_segment)

    def _get_gen_method_replace_pattern(self, operation_id):
        return "# GenMethod: {} -> ([\S ]+)\s+def \S+\(.+\):(?:(?!# GenStop)[\s\S])+# GenStop".format(operation_id)

    def _get_gen_method_replace_string(self, operation_id, code_segment):
        return r"# GenMethod: {} -> \1\n{}\n        # GenStop".format(operation_id, code_segment)

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
