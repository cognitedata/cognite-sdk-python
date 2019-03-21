import json
from typing import *

import requests

from openapi import utils


class Property:
    def __init__(self, components, name: str, property: Dict, required: bool):
        self._components = components
        self._property = property.copy()
        self.name = name
        self.required = required

    @property
    def description(self):
        if "description" in self._property:
            return self._property["description"]
        elif "$ref" in self._property:
            nested_schema = self._get_schema(self._property["$ref"])
            return nested_schema.description
        return ""

    @property
    def type(self):
        if "type" in self._property:
            return self._property["type"]
        elif "$ref" in self._property:
            nested_schema = self._get_schema(self._property["$ref"])
            return nested_schema.name

    @property
    def python_type(self):
        if self.type == "object":
            if "additionalProperties" in self._property:
                return self._property["additionalProperties"]["type"]
            return "Dict[str, Any]"
        if self.type == "array":
            items = self._property["items"]
            if "type" in items:
                return "List[{}]".format(utils.get_python_type(items["type"]))
            return "List[{}]".format(self._components.get_schema_from_ref(items["$ref"]).name)
        if self.type in utils.FLAT_TYPE_MAPPING:
            return utils.get_python_type(self.type)
        return self.type

    def _get_schema(self, ref):
        return self._components.get_schema_from_ref(ref)


class Schema:
    def __init__(self, components, name: str, schema: Dict):
        self._components = components
        self._schema = schema.copy()
        self.name = name
        self._properties = self._schema.get("properties", {})
        self._required = self._schema.get("required", [])
        self.properties = [
            Property(self._components, name, property, name in self._required)
            for name, property in self._properties.items()
        ]
        self.description = self._schema.get("description", "")

    def get_property(self, name):
        for property in self.properties:
            if property.name == name:
                return property
        raise ValueError("Property `{}` does not exist on schema `{}`".format(name, self.name))

    def __str__(self):
        return json.dumps(self._schema, indent=4)


class Parameter:
    def __init__(self, parameter):
        self._parameter = parameter.copy()
        self.type = self._parameter.get("schema", {}).get("type")
        self.required = self._parameter.get("required", False)
        self.in_ = self._parameter["in"]
        self.name = self._parameter["name"]
        self.description = self._parameter.get("description", "")


class Operation:
    def __init__(self, components, method: str, operation: Dict, path_parameters: List):
        self._operation = operation.copy()
        self._components = components
        self._path_params = path_parameters
        self.method = method
        self.operation_id = self._operation["operationId"]
        self.params = [Parameter(param) for param in self._path_params + self._operation.get("parameters", [])]

    @property
    def description(self):
        summary = self._operation.get("summary", "")
        description = utils.truncate_description(self._operation.get("description", ""))
        if len(summary) > 0 and len(description) > 0:
            return summary + "\n\n" + description
        return "{}{}".format(summary, description)

    @property
    def request_schema(self):
        if "requestBody" in self._operation:
            schema = self._components.get_schema_from_ref(
                self._operation["requestBody"]["content"]["application/json"]["schema"]["$ref"]
            )
            for prop in schema.properties:
                if prop.type == "array" and prop.name == "items" and "$ref" in prop._property["items"]:
                    return self._components.get_schema_from_ref(prop._property["items"]["$ref"])
            return schema


class Path:
    def __init__(self, components, path, operations):
        self.path = path
        self._components = components
        self._operations = operations

    @property
    def operations(self):
        operations_raw = self._operations.copy()
        path_parameters = []
        if "parameters" in operations_raw:
            path_parameters = operations_raw.pop("parameters", [])

        operations = []
        for method, operation in operations_raw.items():
            operations.append(Operation(self._components, method, operation, path_parameters))
        return operations


class Components:
    def __init__(self, component):
        self._component = component.copy()
        self.schemas = [Schema(self, name, schema) for name, schema in self._component["schemas"].items()]

    def get_schema(self, name: str):
        for schema in self.schemas:
            if schema.name == name:
                return schema
        raise ValueError("Schema `{}` does not exist".format(name))

    def get_schema_from_ref(self, ref):
        name = ref.split("/")[-1]
        return self.get_schema(name)


class Info:
    def __init__(self, info):
        self.title = info["title"]
        self.version = info["version"]
        self.description = utils.truncate_description(info["description"])


class OpenAPISpec:
    def __init__(self, url: str):
        self._spec_url = url
        self._spec = self.download_spec()
        self.info = Info(self._spec["info"])
        self.components = Components(self._spec["components"])
        self.paths = [Path(self.components, path, operations) for path, operations in self._spec["paths"].items()]

    def download_spec(self):
        return requests.get(self._spec_url).json()

    def get_operation(self, operation_id: str):
        for path in self.paths:
            for operation in path.operations:
                if operation.operation_id == operation_id:
                    return operation
        raise ValueError("Operation with that id ´{}´ does not exist".format(operation_id))
