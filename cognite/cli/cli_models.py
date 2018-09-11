import argparse
import sys
from datetime import datetime

from tabulate import tabulate

from cognite.v06 import models


class CogniteMLCLI:
    def get(self, args):
        """cognite models get [<args>]

        Get all models or pass a model id and get all model versions.
        """
        parser = argparse.ArgumentParser(description="Access CDP model hosting services.", usage=self.get.__doc__)
        parser.add_argument("-m", "--model-id", help="Model to get versions for")
        parser.add_argument("-s", "--source-packages", help="Get source-packages", action="store_true")
        parsed_args = parser.parse_args(args)
        if parsed_args.source_packages:
            if parsed_args.model_id:
                # TODO: Get source packages associated with this model
                pass
            else:
                # TODO: Get all source packages
                pass
        elif parsed_args.model_id:
            # TODO: Get all model versions
            pass
        else:
            # TODO: Get all models
            self.print_models()

    def print_models(self):
        models_list = models.get_models()

        if not models:
            print("No models yet :(")
            return

        models_tabular = [
            [
                m.get("id"),
                m.get("name"),
                m.get("description"),
                m.get("active_version_id"),
                datetime.fromtimestamp(int(m.get("created_time")) / 1000),
                m.get("is_deprecated") == "1",
            ]
            for m in models_list
        ]
        headers = ["ID", "Name", "Description", "Active Version", "Created Time", "Is Deprecated"]
        print(tabulate(models_tabular, headers=headers))

    def create(self, resource, next_arg_index):
        usage_msg = """cognite ml create {} [<args>]""".format(resource)
        parser = argparse.ArgumentParser(description="Create a new {}.".format(resource), usage=usage_msg)
        parser.add_argument("name", help="Name of the model")
        parser.add_argument("-d", "--description", help="A description of what the model does", default="")
        args = parser.parse_args(sys.argv[next_arg_index:])
        models.create_model(name=args.name, description=args.description)
