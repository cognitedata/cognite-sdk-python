"""
Creates/updates the proto files in cognite/client/_proto/ using definitions from:
https://github.com/cognitedata/protobuf-files

Note:
As long as we support `protobuf >= 5`, we need to use the earliest `protoc` version from
the v5 release train, since gencode embeds a minimum-runtime-version guard tied to whatever
protoc produced it. That's release 26.0, which can be downloaded here:
https://github.com/protocolbuffers/protobuf/releases/tag/v26.0

This script expects that exact version at the repo root as `./protoc`.

Run this script from the repo root: `python scripts/update_proto_files.py`
"""

import os
import subprocess
import tempfile
from pathlib import Path

import requests

URL_BASE = "https://raw.githubusercontent.com/cognitedata/protobuf-files/master/v1/timeseries/"
# In case you need to target a branch/specific commit:
# URL_BASE = "https://raw.githubusercontent.com/cognitedata/protobuf-files/a542c592c9646068b167abd13df16204216ce00f/v1/timeseries/"
FILES = "data_point_list_response.proto", "data_points.proto", "data_point_insertion_request.proto"
PROTO_DIR = str(Path("cognite/client/_proto").resolve())
PROTOC = str(Path("protoc").resolve())


def download_proto_files_and_compile():
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        for file in map(Path, FILES):
            file.touch()
            file.write_bytes(requests.get(f"{URL_BASE}{file}").content)
        subprocess.run([PROTOC, *FILES, f"--python_out={PROTO_DIR}", f"--pyi_out={PROTO_DIR}"], check=True)


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
