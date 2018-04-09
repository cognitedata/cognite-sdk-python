'''This module is used to remove typehints from source code to ensure python 2.7 compatibility.'''
import argparse
import ast
import os
import re
import sys

import astunparse


class TypeHintRemover(ast.NodeTransformer):
    def visit_FunctionDef(self, node):
        # remove the return type defintion
        node.returns = None
        # remove all argument annotations
        if node.args.args:
            for arg in node.args.args:
                arg.annotation = None
        return node

    def visit_Import(self, node):
        node.names = [n for n in node.names if n.name != 'typing']
        return node if node.names else None

    def visit_ImportFrom(self, node):
        return node if node.module != 'typing' else None


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-th', '--remove-type-hints', action='store_true')
    parser.add_argument('--suppress-warning', action='store_true')

    args = parser.parse_args()
    print(args)

    dirs = [os.path.abspath(os.path.join(os.path.dirname(__file__), 'cognite', file)) for file in
            os.listdir('./cognite') if
            re.match('.+\.py$', file)]

    if not args.suppress_warning:
        if input("This will alter the source code of your project and should not be run without knowing what you're "
                 "doing. Enter 'PEACOCK' to continue: ") != 'PEACOCK':
            sys.exit(0)

    for dir in dirs:
        if args.remove_type_hints:
            # parse the source code into an AST
            with open(dir, 'r') as f:
                parsed_source = ast.parse(f.read())
            # remove all type annotations, function return type definitions
            # and import statements from 'typing'
            transformed = TypeHintRemover().visit(parsed_source)

            with open(dir, 'w') as f:
                f.write(astunparse.unparse(transformed))
