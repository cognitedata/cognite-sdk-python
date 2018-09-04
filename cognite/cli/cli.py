#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import sys

from cognite import config
from cognite.cli.cli_config import CogniteConfigCLI
from cognite.cli.cli_ml import CogniteMLCLI


class CogniteCLI:
    def __init__(self):
        usage_msg = """cognite <service> [<args>]

The available Cognite services are:
   config       Configure your CDP client.
   ml           Cognite machine learning services
"""
        parser = argparse.ArgumentParser(description="Command line interface for CDP.", usage=usage_msg)
        parser.add_argument("service", help="Service to run", choices=["config", "ml"])
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.service.replace("-", "_")):
            print("Unrecognized service")
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        config.load_from_file()
        getattr(self, args.service)(next_arg_index=2)

    def config(self, next_arg_index):
        usage_msg = """cognite config <command> [<args>]

The available Cognite config commands are:
    set-default         Set default config
    view        View config
"""
        parser = argparse.ArgumentParser(description="Configure your CDP client.", usage=usage_msg)
        parser.add_argument("command", help="Command to run", choices=["set-default", "view"])
        args = parser.parse_args(sys.argv[next_arg_index : next_arg_index + 1])
        cmd = args.command.replace("-", "_")
        config_cli = CogniteConfigCLI()
        getattr(config_cli, cmd)(next_arg_index + 1)

    def ml(self, next_arg_index):
        usage_msg = """cognite ml <command> <resource> [<args>]

The available Cognite ML commands are:
    get         Get resource
    create      Create resource
"""
        parser = argparse.ArgumentParser(description="Access CDP machine learning services.", usage=usage_msg)
        parser.add_argument("command", help="Command to run", choices=["create", "get"])
        parser.add_argument("resource", help="Resource to run command on", choices=["model", "version", "source"])
        # now that we're inside a subcommand, ignore the first
        # TWO argvs, ie the command (cognite) and the subcommand (ml)
        args = parser.parse_args(sys.argv[next_arg_index : next_arg_index + 2])
        ml_cli = CogniteMLCLI()
        getattr(ml_cli, args.command)(args.resource, next_arg_index + 2)


def main():
    CogniteCLI()
