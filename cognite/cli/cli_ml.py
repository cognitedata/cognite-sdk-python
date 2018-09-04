from cognite import config
from cognite.v05 import hosting


class CogniteMLCLI:
    def get(self, resource):
        models = hosting.get_models()
        print(f"{resource}s:")
        print(models)

    def create(self, resource):
        print(f"creating {resource}")
