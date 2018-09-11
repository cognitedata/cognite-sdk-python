import argparse
import os
import shutil
import sys
from datetime import datetime

from tabulate import tabulate

from cognite.v06 import models

MODEL_SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir, "auxiliary/_hosting/model"))


class CogniteMLCLI:
    def get(self, args):
        """cognite models get [<args>]

        Get all models or pass a model id and get all model versions.
        """
        parser = argparse.ArgumentParser(description="Get information about models.", usage=self.get.__doc__)
        parser.add_argument("-m", "--model-id", help="Model to get versions for", type=int)
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
            versions_list = models.get_versions(parsed_args.model_id)
            self._print_versions(versions_list)
        else:
            models_list = models.get_models()
            self._print_models(models_list)

    def new(self, args):
        """cognite models new

        Create a new model source package in the current working directory
        """
        parser = argparse.ArgumentParser(description="Create new model source package.", usage=self.new.__doc__)
        parsed_args = parser.parse_args(args)

        model_name = input("Model name: ")

        dest_dir = os.path.join(os.getcwd(), model_name)
        # if not os.path.isdir(dest_dir):
        #     os.mkdir(dest_dir)
        shutil.copytree(MODEL_SRC_DIR, dest_dir)
        print(
            "\nYour source package has been prepared!\nEnter the following command to take a look\n\n   cd {}\n".format(
                model_name
            )
        )

    def create(self, args):
        """cognite models create [<args>]"""
        parser = argparse.ArgumentParser(description="Create a new model.", usage=self.create.__doc__)
        parser.add_argument("name", help="Name of the model")
        parser.add_argument("-d", "--description", help="A description of what the model does", default="")
        args = parser.parse_args(args)
        models.create_model(name=args.name, description=args.description)

    def _print_models(self, models_list):
        if not models_list:
            print("No models yet.")
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

    def _print_versions(self, versions_list):
        if not versions_list:
            print("No versions yet.")
            return
        versions_tabular = [
            [
                m.get("id"),
                m.get("name"),
                m.get("status"),
                m.get("description"),
                m.get("source_package_id"),
                datetime.fromtimestamp(int(m.get("created_time")) / 1000),
                m.get("is_deprecated") == "1",
            ]
            for m in versions_list
        ]
        headers = ["ID", "Name", "Status", "Description", "Source Package ID", "Created Time", "Is Deprecated"]
        print(tabulate(versions_tabular, headers=headers))

    def _copy_source_package(self, path):
        pass
