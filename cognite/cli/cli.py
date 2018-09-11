#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import sys

from cognite.cli.cli_models import CogniteMLCLI


class CogniteCLI:
    def __init__(self):
        """cognite <service> [<args>]

The available Cognite services are:
name        Description
----        -----------
models      Model hosting
"""
        parser = argparse.ArgumentParser(description="Command line interface for CDP.", usage=self.__init__.__doc__)
        parser.add_argument("service", help="Service to run", choices=["models"])
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.service.replace("-", "_")):
            print("Unrecognized service")
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        getattr(self, args.service)(sys.argv[2:])

    def models(self, args):
        """cognite models <command> [<args>]

The available commands are:
    new         Create a new model source package in the current directory
    get         Get all models, versions, or source packages.
    create      Create a new model
    train       Train a new model version
"""
        parser = argparse.ArgumentParser(description="Access CDP model hosting services.", usage=self.models.__doc__)
        parser.add_argument("command", help="Command to run", choices=["new", "train", "create", "get"])
        parsed_args = parser.parse_args(args[:1])
        ml_cli = CogniteMLCLI()
        getattr(ml_cli, parsed_args.command)(args[1:])


def main():
    CogniteCLI()
