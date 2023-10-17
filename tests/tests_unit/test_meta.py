import inspect
from pathlib import Path

import pytest

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from tests.utils import all_subclasses

ALL_FILEPATHS = Path("cognite/client/").rglob("*.py")


def test_assert_no_root_init_file():
    # We have an implicit namespace package under the namespace package directory: 'cognite'.

    # From: https://packaging.python.org/en/latest/guides/packaging-namespace-packages/#native-namespace-packages
    # "It is extremely important that every distribution that uses the namespace package omits the __init__.py
    # or uses a pkgutil-style __init__.py. If any distribution does not, it will cause the namespace logic to
    # fail and the other sub-packages will not be importable"
    assert not Path("cognite/__init__.py").exists()


def test_ensure_all_files_use_future_annots():
    def keep(path):
        skip_list = [
            "_pb2.py",  # Auto-generated, dislikes changes ;)
        ]
        return all(skip not in str(path.as_posix()) for skip in skip_list)

    err_msg = "File: '{}' is missing 'from __future__ import annotations' at line=0"
    for filepath in filter(keep, ALL_FILEPATHS):
        with filepath.open("r") as file:
            # We just read the first line from each file:
            assert file.readline() == "from __future__ import annotations\n", err_msg.format(filepath)


@pytest.mark.parametrize("cls", [CogniteResource, CogniteResourceList])
def test_ensure_all_to_pandas_methods_use_snake_case(cls):
    err_msg = "Class: '{}' for method to_pandas does not default camel_case parameter to False."
    for sub_cls in all_subclasses(cls):
        if not (cls_method := getattr(sub_cls, "to_pandas", False)):
            continue
        if param := inspect.signature(cls_method).parameters.get("camel_case"):
            assert param.default is False, err_msg.format(sub_cls.__name__)
