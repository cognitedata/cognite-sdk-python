import os
import shlex
import subprocess
import tempfile
from pathlib import Path

import requests

"""
Creates/updates the proto files in cognite/client/_proto/ using definitions from:
https://github.com/cognitedata/protobuf-files

Requires `protoc` to be installed. On MacOS, you can install it with Homebrew:
$ brew install protobuf
"""

URL_BASE = "https://raw.githubusercontent.com/cognitedata/protobuf-files/master/v1/timeseries/"
FILES = "data_point_list_response.proto", "data_points.proto", "data_point_insertion_request.proto"
PROTO_DIR = str(Path("cognite/client/_proto").resolve())


def download_proto_files_and_compile():
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        for file in map(Path, FILES):
            file.touch()
            file.write_bytes(requests.get(f"{URL_BASE}{file}").content)
        protoc_command = " ".join(("protoc", *FILES, f"--python_out={PROTO_DIR}", f"--pyi_out={PROTO_DIR}"))
        subprocess.run(shlex.split(protoc_command), check=True)


def patch_bad_imports():
    for file in Path().glob("*.py"):
        file.write_text(
            file.read_text().replace(
                "import data_points_pb2 as data__points__pb2",
                "import cognite.client._proto.data_points_pb2 as data__points__pb2",
            )
        )
    for file in Path().glob("*.pyi"):
        file.write_text(
            file.read_text().replace(
                "import data_points_pb2 as _data_points_pb2",
                "import cognite.client._proto.data_points_pb2 as _data_points_pb2",
            )
        )


if __name__ == "__main__":
    download_proto_files_and_compile()
    os.chdir(PROTO_DIR)
    patch_bad_imports()
