"""This module is used to remove typehints from source code to ensure python 3.5 compatibility."""
import os
import re

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


if __name__ == "__main__":
    directories = ["cognite", "tests"]
    for dir in directories:
        remove_type_hints_recursively(dir)
