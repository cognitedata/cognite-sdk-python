import argparse
import sys
from datetime import datetime

from tabulate import tabulate

from cognite.v05 import hosting


class CogniteMLCLI:
    def get(self, resource, next_arg_index):
        if resource == "model":
            models = hosting.get_models()
        else:
            raise AssertionError
        if not models:
            print("No models yet :(")
            return
        tabular_data = [
            [
                m.get("id"),
                m.get("name"),
                m.get("description"),
                m.get("active_version_id"),
                datetime.fromtimestamp(int(m.get("created_time")) / 1000),
                m.get("is_deprecated") == "1",
            ]
            for m in models
        ]
        headers = ["ID", "Name", "Description", "Active Version", "Created Time", "Is Deprecated"]
        print(tabulate(tabular_data, headers=headers))

    def create(self, resource, next_arg_index):
        usage_msg = """cognite ml create {} [<args>]""".format(resource)
        parser = argparse.ArgumentParser(description="Create a new {}.".format(resource), usage=usage_msg)
        parser.add_argument("name", help="Name of the model")
        parser.add_argument("-d", "--description", help="A description of what the model does", default="")
        args = parser.parse_args(sys.argv[next_arg_index:])
        hosting.create_model(name=args.name, description=args.description)
