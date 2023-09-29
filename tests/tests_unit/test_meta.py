from pathlib import Path

ALL_FILEPATHS = Path("cognite/client/").glob("**/*.py")


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
        return all(skip not in str(path.as_posix()) for skip in skip_list)

    err_msg = "File: '{}' is missing 'from __future__ import annotations' at line=0"
    for filepath in filter(keep, ALL_FILEPATHS):
        with filepath.open("r") as file:
            # We just read the first line from each file:
            assert file.readline() == "from __future__ import annotations\n", err_msg.format(filepath)


def test_ensure_all_tests_use_camel_case_except_dump():
    err_msg = "File: '{}' contains camel_case=True and not a dump at line {}."
    for filepath in ALL_FILEPATHS:
        if "tests/" in str(filepath):
            with filepath.open("r") as file:
                for line_num, line in enumerate(file.readlines()):
                    if ".dump(camel_case" in line:
                        continue

                    assert "camel_case=True" not in line, err_msg.format(filepath, line_num)
