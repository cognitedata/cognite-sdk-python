#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import sys

USAGE_MSG = """cognite <service> [<args>]

The most commonly used cognite services are:
   ml     Cognite machine learning services
"""


class CogniteCLI:
    def __init__(self):
        parser = argparse.ArgumentParser(description="Command line interface for CDP.", usage=USAGE_MSG)
        parser.add_argument("service", help="Service to run")
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.service):
            print("Unrecognized service")
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        getattr(self, args.service)()

    def ml(self):
        parser = argparse.ArgumentParser(description="Access CDP machine learning services.")
        # prefixing the argument with -- means it's optional
        parser.add_argument("-v", "--verbose", action="store_true")
        # now that we're inside a subcommand, ignore the first
        # TWO argvs, ie the command (cognite) and the subcommand (ml)
        args = parser.parse_args(sys.argv[2:])
        print("Running cognite ml, verbose=%s" % args.verbose)


def main():
    CogniteCLI()
