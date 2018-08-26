#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import os
import sys

from cognite.cli.config import CogniteConfigCLI
from cognite.cli.ml import CogniteMLCLI


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
        getattr(self, args.service)()

    def config(self):
        usage_msg = """cognite config <command> [<args>]

The available Cognite config commands are:
    set         Set config variables
    view        View config
"""
        parser = argparse.ArgumentParser(description="Configure your CDP client.", usage=usage_msg)
        parser.add_argument("command", help="Command to run", choices=["set-default", "view"])
        args = parser.parse_args(sys.argv[2:3])
        cmd = args.command.replace("-", "_")
        config_cli = CogniteConfigCLI()
        getattr(config_cli, cmd)()

    def ml(self):
        usage_msg = """cognite ml <command> <resource> [<args>]

The available Cognite ML commands are:
    get         Get resource
    create      Create resource
"""
        parser = argparse.ArgumentParser(description="Access CDP machine learning services.", usage=usage_msg)
        # prefixing the argument with -- means it's optional
        parser.add_argument("command", help="Command to run", choices=["create", "get"])
        parser.add_argument("resource", help="Resource to run command on", choices=["model", "version" "source"])
        # now that we're inside a subcommand, ignore the first
        # TWO argvs, ie the command (cognite) and the subcommand (ml)
        args = parser.parse_args(sys.argv[2:4])
        ml_cli = CogniteMLCLI()
        getattr(ml_cli, args.command)(args.resource)


def main():
    CogniteCLI()
