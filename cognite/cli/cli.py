#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import os
import sys

from cognite.cli.cli_models import CogniteModelsCLI


class CogniteCLI:
    def __init__(self):
        """cognite <service> [<args>]

The available Cognite services are:
    models      Model hosting
"""
        parser = argparse.ArgumentParser(description="Command line interface for CDP.", usage=self.__init__.__doc__)
        parser.add_argument("service", help="Service to run")
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(sys.argv[1:2])
        if not os.getenv("COGNITE_API_KEY") or not os.getenv("COGNITE_PROJECT"):
            print("You must set COGNITE_API_KEY and COGNITE_PROJECT environment variables.")
            return
        if not hasattr(self, args.service.replace("-", "_")):
            print("Unrecognized service")
            parser.print_help()
            return
        # use dispatch pattern to invoke method with same name
        getattr(self, args.service)(sys.argv[2:])

    def models(self, args):
        """cognite models <command> [<args>]

The available commands are:
    source      Create a new model source package in the current directory.
    deploy          Run tests, upload a source package and optionally create a model with a specified name.
    get         Get all models, source packages, or versions for a model.
"""
        parser = argparse.ArgumentParser(description="Access CDP model hosting services.", usage=self.models.__doc__)
        parser.add_argument("command", help="Command to run", choices=["source", "get", "deploy"])
        parsed_args = parser.parse_args(args[:1])
        ml_cli = CogniteModelsCLI()
        getattr(ml_cli, parsed_args.command)(args[1:])


def main():
    CogniteCLI()
