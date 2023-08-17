from pathlib import Path


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
            "cognite/client/utils/_priority_tpe.py",  # Module docstring at the top takes priority
        ]
        return all(skip not in str(path) for skip in skip_list)

    all_filepaths = [Path(p) for p in Path("cognite/client/").glob("**/*.py") if keep(p)]
    err_msg = "File: '{}' is missing 'from __future__ import annotations' at line=0"

    for filepath in all_filepaths:
        with filepath.open("r") as file:
            # We just read the first line from each file:
            assert file.readline() == "from __future__ import annotations\n", err_msg.format(filepath)
