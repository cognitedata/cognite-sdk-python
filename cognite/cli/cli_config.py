import argparse
import json
import os
import sys

from cognite._constants import CONFIG_PATH


class CogniteConfigCLI:
    def __init__(self):
        pass

    def set_default(self, next_arg_index):
        usage_msg = """cognite config set-default [<args>]"""
        parser = argparse.ArgumentParser(description="Configure your CDP client.", usage=usage_msg)
        parser.add_argument("-p", "--project", help="CDP Project to access")
        parser.add_argument("-k", "--api-key", help="Resource to run command on")
        parser.add_argument("-b", "--base-url", help="Base url to send client requests to.")

        args = parser.parse_args(sys.argv[next_arg_index:])
        config = {"cognite": {}}

        if os.path.isfile(CONFIG_PATH):
            with open(CONFIG_PATH, "r") as f:
                config = json.load(f)

        for arg, val in args.__dict__.items():
            if val is not None:
                config["cognite"][arg] = val

        with open(CONFIG_PATH, "w") as f:
            json.dump(config, f)

        print(config)

    def view(self, next_arg_index):
        usage_msg = """cognite config view [<args>]"""
        parser = argparse.ArgumentParser(description="View your CDP client configuration.", usage=usage_msg)
        _ = parser.parse_args(sys.argv[next_arg_index:])

        with open(CONFIG_PATH, "r") as f:
            print(json.load(f))
