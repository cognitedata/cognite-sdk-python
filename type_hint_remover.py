"""This module is used to remove typehints from source code to ensure python 3.5 compatibility."""
import argparse
import os
import re
import subprocess

import strip_hints

SKIP_DIRECTORIES = ["_auxiliary", "__pycache__"]
PYTHON_FILE_PATTERN = r"^.+\.py$"


def strip_hints_and_overwrite(file_path):
    transformed = strip_hints.strip_file_to_string(file_path, to_empty=True, only_assigns_and_defs=True)
    with open(file_path, "w") as f:
        f.write(transformed)


def remove_type_hints_recursively(directory: str):
    print("*****Removing type hints in '{}' directory".format(directory))
    for root, _, files in os.walk(directory):
        if not any(dir_name in root for dir_name in SKIP_DIRECTORIES):
            for file in files:
                if re.match(PYTHON_FILE_PATTERN, file):
                    file_path = "{}/{}".format(root, file)
                    strip_hints_and_overwrite(file_path)


def check_for_changes(directory: str):
    print("*****Checking for type hints in '{}' directory".format(directory))
    has_changed = []
    for root, _, files in os.walk(directory):
        if not any(dir_name in root for dir_name in SKIP_DIRECTORIES):
            for file in files:
                if re.match(PYTHON_FILE_PATTERN, file):
                    file_path = "{}/{}".format(root, file)
                    try:
                        subprocess.check_call(
                            ["strip-hints", file_path, "--only-test-for-changes", "--only-assigns-and-defs"],
                            stdout=subprocess.PIPE,
                        )
                    except subprocess.CalledProcessError:
                        continue
                    has_changed.append(file_path)
    if has_changed:
        print("Found typehints incomaptible with python 3.5 in the following files: \n")
        for file in has_changed:
            print("- " + file)
        print("\nRemove them by running ´python3 type_hint_remover.py´ in the root directory.")
        exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="Check for incompatible typehints. Exit code 1 if true.")
    args = parser.parse_args()

    directories = ["cognite", "tests"]
    for dir in directories:
        if args.check:
            check_for_changes(dir)
        else:
            remove_type_hints_recursively(dir)
