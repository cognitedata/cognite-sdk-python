import ast
import hashlib
import inspect
import re
import shlex
import subprocess
from collections.abc import Iterator
from functools import cache
from pathlib import Path

from cognite.client import AsyncCogniteClient
from cognite.client._api_client import APIClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import Token
from scripts.sync_client_codegen.constants import ASYNC_METHODS_TO_KEEP, MAYBE_IMPORTS, SYNC_METHODS_TO_KEEP


def get_api_class_by_attribute(cls_: object, parent_name: tuple[str, ...] = ()) -> dict[str, type[APIClient]]:
    available_apis: dict[str, type[APIClient]] = {}
    for attr, obj in cls_.__dict__.items():
        if attr.startswith("_") or not isinstance(obj, APIClient):
            continue
        obj_attr = (*parent_name, attr)
        available_apis[".".join(obj_attr)] = obj.__class__
        available_apis.update(get_api_class_by_attribute(obj, parent_name=obj_attr))
    return available_apis


def find_api_class_name(source_code: str, file: Path, raise_on_missing: bool = True) -> str | None:
    match re.findall(r"class (\w+API)\((?:Org)?APIClient\):", source_code):
        case []:
            return None
        case [cls_name]:
            return cls_name
        case [*multiple]:
            # Dev. note: so you've hit this error and wonder where to -> please split your APIs into separate files
            # (sorry if this feels like Java all of a sudden). It makes the codebase much easier to auto-convert
            # from async to sync.
            raise RuntimeError(f"Found multiple API classes in file='{file}': {multiple}")


def hash_file(path: Path) -> str:
    return hashlib.new("md5", path.read_bytes()).hexdigest()


def is_pyfile(file: Path) -> bool:
    return file.suffix == ".py"


def list_apis() -> Iterator[Path]:
    return Path("cognite/client/_api").rglob("*")


def list_sync_apis() -> Iterator[Path]:
    return Path("cognite/client/_sync_api").rglob("*")


def path_as_importable(path: Path) -> str:
    return ".".join(path.with_suffix("").parts)


def is_md5_hash(s: str) -> bool:
    return bool(re.match(r"^[a-f0-9]{32}$", s))


def read_hash_from_file(path: Path) -> tuple[bool, str]:
    with path.open("r") as f:
        f.readline()
        f.readline()
        maybe_hash = f.readline().strip()

    return is_md5_hash(maybe_hash), maybe_hash


def file_has_changed(write_file: Path, read_file_hash: str) -> bool:
    # Skip a file if it exists and the stored hash matches:
    if write_file.exists():
        is_valid, existing_hash = read_hash_from_file(write_file)
        if is_valid and existing_hash == read_file_hash:
            return False
    return True


def get_module_level_imports(tree: ast.Module):
    import_nodes = [node for node in tree.body if isinstance(node, (ast.Import, ast.ImportFrom))]
    return "\n".join(ast.unparse(node) for node in import_nodes)


def get_module_level_type_checking_imports(tree: ast.Module) -> str:
    imports: list[str] = []
    for node in tree.body:
        if not isinstance(node, ast.If):
            continue

        match node.test:
            case ast.Name(id="TYPE_CHECKING"):
                pass  # we found: `if TYPE_CHECKING:`
            case ast.Attribute(value=ast.Name(id="typing"), attr="TYPE_CHECKING"):
                pass  # we found: `if typing.TYPE_CHECKING:`
            case _:
                continue

        for sub in node.body:
            if isinstance(sub, (ast.Import, ast.ImportFrom)):
                imports.append(ast.unparse(sub))
    return "\n".join(imports)


def get_all_imports(tree: ast.Module, source_code: str, source_path: Path) -> tuple[str, str]:
    all_imports = get_module_level_imports(tree)
    type_checking_imports = get_module_level_type_checking_imports(tree)
    extras = []
    parent = source_path.parent
    parent_api_in_init = parent != Path("cognite/client/_api") and (parent / "__init__.py").exists()
    parent_source = get_source_code(parent / "__init__.py") if parent_api_in_init else ""

    for maybe in MAYBE_IMPORTS:
        # Typically there are type aliases defined in the async API module that we need to import:
        if maybe in source_code:
            to_import = maybe.split(": ")[0]
            import_path = path_as_importable(source_path).replace(".__init__", "")
            extras.append(f"from {import_path} import {to_import}")
        # For a lot of 'nested' APIs, these are in parent / init:
        elif maybe in parent_source:
            to_import = maybe.split(": ")[0]
            extras.append(f"from {path_as_importable(parent)} import {to_import}")

    if extras:
        all_imports += "\n"
    return all_imports + "\n".join(extras), type_checking_imports


def find_class_node(tree: ast.Module, class_name: str) -> ast.ClassDef:
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return node
    raise ValueError(f"Could not find class '{class_name}' in AST")


def find_self_assignments(class_node: ast.ClassDef) -> tuple[list[str], list[str]]:
    init_node = None
    for node in class_node.body:
        if isinstance(node, ast.FunctionDef) and node.name == "__init__":
            init_node = node
            break

    if not init_node:
        return [], []

    names, nested_apis = [], []
    for stmt in init_node.body:
        if not isinstance(stmt, ast.Assign):
            continue

        for t in stmt.targets:
            if (
                isinstance(t, ast.Attribute)
                and isinstance(t.value, ast.Name)
                and t.value.id == "self"
                and isinstance(stmt.value, ast.Call)
                and stmt.value.func.id.endswith("API")
            ):
                names.append(cls_name := foolish_cls_name_rewrite(stmt.value.func.id))
                nested_apis.append(
                    # As opposed to the regular APIs, we only need to pass the async client here:
                    f"self.{t.attr} = Sync{cls_name}(async_client)"
                )
    return names, nested_apis


def foolish_cls_name_rewrite(class_name: str) -> str:
    # Yes, YESSSS
    return class_name.replace("ThreeD", "3D")


def inverse_foolish_cls_name_rewrite(class_name: str) -> str:
    return class_name.replace("3D", "ThreeD")  # Needed when searching


def method_should_be_converted(node: ast.AST) -> bool:
    match node:
        case ast.AsyncFunctionDef(name=n) if not n.startswith("_") or n in ASYNC_METHODS_TO_KEEP:
            return True
        case ast.FunctionDef(name=n) if n in SYNC_METHODS_TO_KEEP:
            return True
        case _:
            return False


def fix_imports_for_sync_apis(all_imports: str, lst_of_api_names: list[str]) -> str:
    """
    This function performs two main changes for each API name provided:
    1. It changes the import path from `cognite.client._api...` to `cognite.client._sync_api...`.
    2. It prepends "Sync" to the imported class name (e.g., "DatapointsAPI" -> "SyncDatapointsAPI").
    """
    if not lst_of_api_names:
        return all_imports

    api_name_options = "|".join(map(inverse_foolish_cls_name_rewrite, lst_of_api_names))
    pattern = re.compile(rf"^from cognite\.client\._api(\..*? import\s+)(.*?)({api_name_options})(.*)$", re.MULTILINE)

    def replacer(match: re.Match) -> str:
        """This function is called for each match found by re.sub."""
        module_and_import, leading_imports, matched_api_name, trailing_imports = match.groups()
        if leading_imports or trailing_imports:
            # To the poor dev who needs to fix this in the future, here is a long and nice error message.
            # Having one API class per source file seems like good practice anyhow, and if you don't import
            # directly from the defining module, the fix is easy.
            raise ValueError(
                f"Cannot handle multiple imports in the same line for API '{matched_api_name}'. "
                "Example: This functions changes:\n"
                "from cognite.client._api.time_series import DatapointsAPI\n"
                "to:\n"
                "from cognite.client._sync_api.time_series import SyncDatapointsAPI\n"
            )
        return f"from cognite.client._sync_api{module_and_import}Sync{foolish_cls_name_rewrite(matched_api_name)}"

    # Perform the substitution in a single pass:
    return pattern.sub(replacer, all_imports)


def run_ruff(file_paths: list[Path], verbose: bool = False) -> None:
    if not file_paths:
        return
    # We exit nonzero if ruff fixes anything, so we run with check=False to not raise:
    base = f"poetry run pre-commit run ruff-{{}} --files {shlex.join(map(str, file_paths))}"
    command = shlex.split(base.format("check"))
    if verbose:
        print("Now running command\n", shlex.join(command))
    subprocess.run(command, check=False, capture_output=True)

    command = shlex.split(base.format("format"))
    if verbose:
        print("Now running command\n", shlex.join(command))
    subprocess.run(command, check=False, capture_output=True)


def get_dot_path_lookup(async_client: AsyncCogniteClient) -> tuple[dict[str, str], dict[str, str]]:
    api_cls_lookup = get_api_class_by_attribute(async_client)
    dot_path_lookup = {v.__name__: k for k, v in api_cls_lookup.items()}
    if len(dot_path_lookup) != len(api_cls_lookup):
        raise ValueError("API class names not unique, cannot continue")
    file_path_lookup = {v.__name__: inspect.getfile(v) for v in api_cls_lookup.values()}
    return dot_path_lookup, file_path_lookup


def ensure_parent_dir(file: Path) -> None:
    if not file.parent.is_dir():
        file.parent.mkdir(parents=True, exist_ok=True)


@cache
def get_source_code(file: Path) -> str:
    return file.read_text()


def filter_base_apis_and_sort_alphabetically(dct: dict[str, str]) -> list[tuple[str, str]]:
    return sorted((k, v) for k, v in dct.items() if "." not in v)


def clean_up_files(all_expected_files: list[Path], verbose: bool = False) -> None:
    clean_up = set(filter(is_pyfile, list_sync_apis())).difference(all_expected_files)
    if not clean_up:
        if verbose:
            print("- No stale files to clean up 🧹")
        return
    print(f"Cleaning up {len(clean_up)} files no longer needed:")
    for f in clean_up:
        print(f"- Deleting: '{f}'")
        f.unlink()


def setup_async_mock_client() -> AsyncCogniteClient:
    return AsyncCogniteClient(ClientConfig(client_name="name", project="proj", credentials=Token("not-a-token")))


def get_canonical_source(source: Path | str) -> str:
    """
    Reads a Python file from disk or a string, parses it into an AST, and returns
    the canonical source code representation with all comments removed. Docstrings
    however, are not removed, which is exactly what we want.

    This is used to compare a newly generated file against the current version
    without being affected by comment differences (e.g. type-ignore comments),
    allowing us to detect only meaningful code changes that require regeneration.
    """
    if isinstance(source, Path):
        try:
            source = get_source_code(source)
        except FileNotFoundError:
            return ""

    tree = ast.parse(source)
    return ast.unparse(tree)
