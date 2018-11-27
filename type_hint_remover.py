"""This module is used to remove typehints from source code to ensure python 2.7 and 3.5 compatibility."""
import os
import re

import strip_hints

if __name__ == "__main__":
    files = [
        os.path.abspath(os.path.join(os.path.dirname(__file__), "cognite", file))
        for file in os.listdir("./cognite")
        if re.match(".+\.py$", file)
    ]

    version_dirs = [
        os.path.abspath(os.path.join(os.path.dirname(__file__), "cognite", file))
        for file in os.listdir("./cognite")
        if re.match("^v\d\d$", file)
    ]
    for dir in version_dirs:
        version_files = [
            os.path.abspath(os.path.join(dir, file)) for file in os.listdir(dir) if re.match(".+\.py$", file)
        ]
        files.extend(version_files)

    for file_path in files:
        print("*****Removing type hints in {}".format(file_path))

        transformed = strip_hints.strip_file_to_string(file_path, to_empty=True, only_assigns_and_defs=True)

        with open(file_path, "w") as f:
            f.write(transformed)
        print()
