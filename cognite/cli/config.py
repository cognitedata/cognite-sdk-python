import argparse
import os
import sys

import yaml


class CogniteConfigCLI:
    def __init__(self):
        pass

    def set_default(self):
        usage_msg = """cognite config set [<args>]"""
        parser = argparse.ArgumentParser(description="Configure your CDP client.", usage=usage_msg)
        parser.add_argument("-p", "--project", help="CDP Project to access", default=os.getenv("COGNITE_PROJECT", ""))
        parser.add_argument(
            "-k", "--api-key", help="Resource to run command on", default=os.getenv("COGNITE_API_KEY", "")
        )

        args = parser.parse_args(sys.argv[3:])
        config = {"cognite": {"api-key": args.api_key, "project": args.project}}
        with open(os.getenv("HOME") + "/.cognite.conf", "w") as f:
            dump = yaml.dump(config)
            f.write(dump)

    def view(self):
        usage_msg = """cognite config view [<args>]"""
        parser = argparse.ArgumentParser(description="View your CDP client configuration.", usage=usage_msg)
        _ = parser.parse_args(sys.argv[3:])

        with open(os.getenv("HOME") + "/.cognite.conf", "r") as f:
            print(f.read())
