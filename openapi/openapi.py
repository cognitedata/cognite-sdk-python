import json
import os
from subprocess import check_output
from tempfile import TemporaryDirectory

import requests

from openapi import utils


class Paths:
    def __init__(self, paths):
        self._paths = paths

    def get(self, path=None):
        if path:
            for curr_path, path_item in self._paths.items():
                if path == curr_path:
                    return path_item
            ValueError("PathItem with path ´{}´ does not exist".format(path))
        return [path_item for _, path_item in self._paths.items()]

    def get_operation(self, operation_id: str):
        for path_item in self.get():
            for method, operation in path_item.items():
                if "operationId" in operation and operation["operationId"] == operation_id:
                    return operation
        raise ValueError("Operation with that id ´{}´ does not exist".format(operation_id))


class Schemas:
    def __init__(self, schemas):
        self._schemas = schemas
        self._rev_schemas = dict()
        for k, v in schemas.items():
            self._rev_schemas[json.dumps(v)] = k

    def get(self, name=None):
        if name:
            for schema_name, schema in self._schemas.items():
                if schema_name == name:
                    return schema
            raise ValueError("Schema `{}` does not exist".format(name))
        return [schema for _, schema in self._schemas.items()]

    def rev_get(self, struct):
        key = json.dumps(struct)
        if key in self._rev_schemas:
            return self._rev_schemas[key]
        return None


class Components:
    def __init__(self, components):
        self._components = components
        self.schemas = Schemas(self._components["schemas"])


class Info:
    def __init__(self, info):
        self.title = info["title"]
        self.version = info["version"]
        self.description = utils.truncate_description(info["description"])


class OpenAPISpec:
    def __init__(self, url: str = None, path: str = None, exclude_schemas=()):
        if url:
            self._spec_url = url
            self._spec = self.download_spec(exclude_schemas=exclude_schemas)
        elif path:
            with open(path) as f:
                self._spec = json.load(f)
        self.info = Info(self._spec["info"])
        self.components = Components(self._spec["components"])
        self.paths = Paths(self._spec["paths"])

    def download_spec(self, exclude_schemas):
        with TemporaryDirectory() as dir:
            spec = requests.get(self._spec_url).json()
            for key in exclude_schemas:
                del spec["components"]["schemas"][key]
            spec_path = os.path.join(dir, "spec.json")
            with open(spec_path, "w") as f:
                json.dump(spec, f, indent=4)
            res = check_output("swagger-cli bundle -r {}".format(spec_path), shell=True)
        return json.loads(res)
