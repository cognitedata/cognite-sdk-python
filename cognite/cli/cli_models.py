import argparse
import importlib.util
import os
import shutil
import sys
from datetime import datetime
from distutils.core import run_setup

from tabulate import tabulate

from cognite.v06 import models

MODEL_SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir, "auxiliary/_hosting/model"))


class CogniteModelsCLI:
    def get(self, args):
        """cognite models get [<args>]

        Get all models or pass a model id and get all model versions.
        """
        parser = argparse.ArgumentParser(description="Get information about models.", usage=self.get.__doc__)
        parser.add_argument(
            "model_id", metavar="model-id", help="ID of model to get versions for", type=int, nargs="?", default=None
        )
        parser.add_argument("-s", "--source-packages", help="Get source-packages", action="store_true")
        parsed_args = parser.parse_args(args)
        if parsed_args.source_packages:
            source_package_list = models.get_model_source_packages()
            self._print_source_packages(source_package_list)
        elif parsed_args.model_id:
            versions_list = models.get_model_versions(parsed_args.model_id)
            self._print_versions(versions_list)
        else:
            models_list = models.get_models()
            self._print_models(models_list)

    def source(self, args):
        """cognite models source <source-package-name>

        Create a new model source package in the current working directory
        """
        parser = argparse.ArgumentParser(description="Create new model source package.", usage=self.source.__doc__)
        parser.add_argument("name", help="Name of the source package.")
        parsed_args = parser.parse_args(args)
        dest_dir = os.path.join(os.getcwd(), parsed_args.name)
        shutil.copytree(MODEL_SRC_DIR, dest_dir)
        print(
            "Your source package has been prepared!\nEnter the following command to take a look\n\n   cd {}\n".format(
                parsed_args.name
            )
        )

    def deploy(self, args):
        """cognite models deploy [<args>]

        1) Runs tests
        2) Builds sourcepackage
        3) Uploads sourcepackage
        4) Creates model (if --model-name is passed)
        """
        parser = argparse.ArgumentParser(description="Upload your source package", usage=self.deploy.__doc__)
        parser.add_argument("-m", "--model-name", help="Name of the model")
        parser.add_argument("-d", "--description", help="A description of what the model does", default="")
        parsed_args = parser.parse_args(args)

        print("* Verifying package...")
        info = self._verify_source_package(os.getcwd())

        print("* Building distribution...")
        run_setup(os.path.join(os.getcwd(), "setup.py"), script_args=["sdist"])

        print("* Uploading Source Package...")
        dist_dir = os.path.join(os.getcwd(), "dist")
        tarball_path = None
        for root, subdirs, files in os.walk(dist_dir):
            for f in files:
                if f.endswith(".tar.gz"):
                    tarball_path = os.path.join(dist_dir, f)

        source_package_id = models.upload_source_package(
            info.get("source_package_name"),
            parsed_args.description,
            info.get("package_name"),
            info.get("available_operations"),
            file_path=tarball_path,
        )

        if parsed_args.model_name is not None:
            print("* Creating model...")
            model = models.create_model(name=parsed_args.model_name, description=parsed_args.description)

        print(
            "\nSuccesfully uploaded source package{}!".format(
                " and created {}".format(parsed_args.model_name) if parsed_args.model_name is not None else ""
            )
        )

        print("Source package ID: {}".format(source_package_id))
        if parsed_args.model_name is not None:
            print("Model ID: {}".format(model.get("id")))

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
                m.get("is_deprecated"),
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
                m.get("is_deprecated"),
            ]
            for m in versions_list
        ]
        headers = ["ID", "Name", "Status", "Description", "Source Package ID", "Created Time", "Is Deprecated"]
        print(tabulate(versions_tabular, headers=headers))

    def _print_source_packages(self, source_package_list):
        if not source_package_list:
            print("No source packages yet.")
            return
        versions_tabular = [
            [
                m.get("id"),
                m.get("name"),
                m.get("description"),
                ", ".join(m.get("available_operations")),
                datetime.fromtimestamp(int(m.get("created_time")) / 1000),
                m.get("is_deprecated"),
                m.get("is_uploaded"),
            ]
            for m in source_package_list
        ]
        headers = ["ID", "Name", "Description", "Available Operations", "Created Time", "Is Deprecated", "Is Uploaded"]
        print(tabulate(versions_tabular, headers=headers))

    def _verify_source_package(self, directory):
        sys.path.append(os.getcwd())
        info = {"source_package_name": None, "package_name": None, "available_operations": []}

        has_model_module = False
        has_setup__file = False
        for root, subdirs, files in os.walk(directory):
            for f in files:
                if f == "model.py":
                    info["package_name"] = root.split("/")[-1]
                    has_model_module = True
                if f == "setup.py":
                    info["source_package_name"] = root.split("/")[-1]
                    has_setup__file = True

        if not has_setup__file:
            raise AssertionError("Your package must contain a setup.py file")
        if not has_model_module:
            raise AssertionError("Your package must contain a model.py file")

        spec = importlib.util.spec_from_file_location(
            "{}.model".format(info.get("package_name")), "./{}/model.py".format(info.get("package_name"))
        )
        model_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(model_module)

        if not hasattr(model_module, "Model"):
            raise AssertionError("model.py must contain a class named Model")

        model_class = model_module.Model

        has_train_routine = hasattr(model_class, "train")
        has_predict_routine = hasattr(model_class, "predict")
        has_load_routine = hasattr(model_class, "load")

        if not has_train_routine and not has_predict_routine:
            raise AssertionError("Your model class must define at least a train or predict routine.")

        if has_predict_routine and not has_load_routine:
            raise AssertionError("Your model class must define a 'load' method in order to perform predictions")

        if not has_train_routine:
            print(
                "* Your source package does not have a training routine and can not be used to train a new model version."
            )

        if not has_predict_routine:
            print(
                "* Your source package does not have a prediction routine and can not be used to make predictions on model versions."
            )
        if has_predict_routine:
            info.get("available_operations").append("predict")
        if has_train_routine:
            info.get("available_operations").append("train")

        return info
