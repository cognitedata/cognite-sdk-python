from __future__ import annotations

import ast
import hashlib
import inspect
import re
import shlex
import subprocess
import textwrap
from collections.abc import Iterator
from functools import cache
from pathlib import Path

SYNC_CLIENT_PATH = Path("cognite/client/_sync_cognite_client.py")

try:
    # Future devs will most likely delete a file in order to get it regenerated. This will always
    # end up with a ModuleNotFoundError, so we catch that and proceed with a dummy CogniteClient
    # (class which we always regenerate anyway).
    from cognite.client import AsyncCogniteClient
except ImportError as e:
    if "cognite.client._sync_api" in str(e):
        SYNC_CLIENT_PATH.write_text("class CogniteClient:\n    ...\n")
    else:
        raise

from cognite.client import AsyncCogniteClient  # noqa: E402
from cognite.client._api_client import APIClient  # noqa: E402
from cognite.client.config import ClientConfig, global_config  # noqa: E402
from cognite.client.credentials import Token  # noqa: E402

EIGHT_SPACES = " " * 8
SKIP_API_NAMES = {
    "PrincipalsAPI",
}
KNOWN_FILES_SKIP_LIST = {
    Path("cognite/client/_api/datapoint_tasks.py"),
    Path("cognite/client/_api/functions/utils.py"),
    Path("cognite/client/_api/org_apis/principals.py"),  # TODO?
}
MAYBE_IMPORTS = (
    "SortSpec: TypeAlias",
    "_FILTERS_SUPPORTED: frozenset[type[Filter]]",
    "AggregateAssetProperty: TypeAlias",
    "Source: TypeAlias",
    "RunStatus: TypeAlias",
    "WorkflowIdentifier: TypeAlias",
    "WorkflowVersionIdentifier: TypeAlias",
    "ComparableCapability: TypeAlias",
)
ASYNC_API_DIR = Path("cognite/client/_api")
SYNC_API_DIR = Path("cognite/client/_sync_api")

# Template for the generated sync client code:
# - we rely on other tools to clean up imports
SYNC_API_TEMPLATE = '''\
"""
===============================================================================
{file_hash}
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

{existing_imports}
from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils._concurrency import ConcurrencySettings
from typing import Any, Iterator, TypeVar, TYPE_CHECKING, overload
from collections.abc import Coroutine

if TYPE_CHECKING:
    import pandas as pd
    {type_checking_imports}

_T = TypeVar("_T")


class Sync{class_name}(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient):
        self.__async_client = async_client
        {nested_apis_init}

'''


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
    match re.findall(r"class (\w+API)\(APIClient\):", source_code):
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


def read_hash_from_file(path: Path) -> tuple[bool, str | None]:
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
            # check for "if TYPE_CHECKING:"
            case ast.Name(id="TYPE_CHECKING"):
                pass
            # or "if typing.TYPE_CHECKING:"
            case ast.Attribute(value=ast.Name(id="typing"), attr="TYPE_CHECKING"):
                pass
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
                and stmt.value.func.id not in SKIP_API_NAMES
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
    # There's more to it than just an isinstance check: 'async def __call__' does not return
    # a coroutine, but an async generator. This in turn means that mypy forces the overloads
    # to NOT be 'async def' but just 'def'. Wait what?! I for sure had to Google it. So we need
    # to treat __call__ as a special case in order to not lose all that typing goodies...
    return isinstance(node, ast.AsyncFunctionDef) or getattr(node, "name", None) == "__call__"


def generate_sync_client_code(
    class_name: str,
    source_code: str,
    source_path: Path,
    dot_path_lookup: dict[str, str],
    file_hash: str,
) -> str | None:
    if class_name is None:
        return None
    try:
        dotted_path = dot_path_lookup[class_name]
    except KeyError:
        raise RuntimeError(
            f"Could not find the dotted path for class='{class_name}', e.g.:"
            "EventsAPI -> 'events', DatapointsAPI -> 'timer_series.data'."
        )

    tree = ast.parse(source_code)
    class_def = find_class_node(tree, class_name)

    # Group methods by name to handle overloads correctly.
    generated_methods = []
    methods_by_name = {}
    all_method_nodes = [m for m in class_def.body if method_should_be_converted(m)]
    for method_node in all_method_nodes:
        methods_by_name.setdefault(method_node.name, []).append(method_node)

    for name, method_nodes in methods_by_name.items():
        if name.startswith("_") and name != "__call__":
            continue

        # The last definition is the implementation, the rest are overloads
        overloads = method_nodes[:-1]
        implementation = method_nodes[-1]

        for overload_node in overloads:
            sync_def = "@overload\n    def {name}({args}) -> {return_type}: ...".format(
                name=name,
                args=ast.unparse(overload_node.args),
                return_type=ast.unparse(overload_node.returns).replace("AsyncIterator", "Iterator"),
            )
            generated_methods.append(sync_def)

        docstring = ast.get_docstring(implementation)

        # Create the list of arguments to pass to the async call
        call_parts = []
        # 1. Handle positional-only arguments (e.g., func(a, /))
        call_parts.extend([arg.arg for arg in implementation.args.posonlyargs])
        # 2. Handle regular arguments (can be pos or keyword)
        # We will pass these by keyword for safety.
        regular_args = [f"{arg.arg}={arg.arg}" for arg in implementation.args.args if arg.arg != "self"]
        call_parts.extend(regular_args)
        # 3. Handle variadic positional arguments (*args)
        if implementation.args.vararg:
            call_parts.append(f"*{implementation.args.vararg.arg}")
        # 4. Handle keyword-only arguments (e.g., func(*, a))
        kw_only_args = [f"{arg.arg}={arg.arg}" for arg in implementation.args.kwonlyargs]
        call_parts.extend(kw_only_args)
        # 5. Handle variadic keyword arguments (**kwargs)
        if implementation.args.kwarg:
            call_parts.append(f"**{implementation.args.kwarg.arg}")

        # Check return type for AsyncIterator
        return_type_str = ast.unparse(implementation.returns)
        is_iterator = "AsyncIterator" in return_type_str
        sync_return_type = return_type_str.replace("AsyncIterator", "Iterator")

        method_body = ""
        if is_iterator:
            # Skip name here (__call__):
            method_body = f"yield from SyncIterator(self.__async_client.{dotted_path}({', '.join(call_parts)}))"
        else:
            method_body = f"return run_sync(self.__async_client.{dotted_path}.{name}({', '.join(call_parts)}))"

        indented_docstring = ""
        if docstring:
            indented_docstring = f'{EIGHT_SPACES}"""\n{textwrap.indent(docstring, EIGHT_SPACES)}\n{EIGHT_SPACES}"""\n'
        impl_def = (
            f"def {name}({ast.unparse(implementation.args)}) -> {sync_return_type}:\n"
            f"{indented_docstring}{EIGHT_SPACES}{method_body}"
        )
        generated_methods.append(impl_def)

    all_imports, type_checking_imports = get_all_imports(tree, source_code, source_path)
    # In init, we find nested APIs - we also may need to modify existing imports:
    api_names, nested_apis = find_self_assignments(class_def)
    all_imports = fix_imports_for_sync_apis(all_imports, api_names)

    # Combine everything 🤞
    return (
        textwrap.dedent(
            SYNC_API_TEMPLATE.format(
                file_hash=file_hash,
                class_name=foolish_cls_name_rewrite(class_name),
                existing_imports=all_imports,
                type_checking_imports=type_checking_imports,
                nested_apis_init="\n        ".join(nested_apis),
            )
        )
        + "    "
        + "\n\n    ".join(generated_methods)
        + "\n"
    )


def fix_imports_for_sync_apis(all_imports: str, lst_of_api_names: list[str]) -> str:
    """
    This function performs two main changes for each API name provided:
    1. It changes the import path from `cognite.client._api...` to `cognite.client._sync_api...`.
    2. It prepends "Sync" to the imported class name (e.g., "DatapointsAPI" -> "SyncDatapointsAPI").
    """
    if not lst_of_api_names:
        return all_imports

    api_name_options = "|".join(map(re.escape, lst_of_api_names))  # escape is prob overkill
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
        return f"from cognite.client._sync_api{module_and_import}Sync{matched_api_name}"

    # Perform the substitution in a single pass:
    return pattern.sub(replacer, all_imports)


def run_ruff(file_paths: list[Path]) -> None:
    if not file_paths:
        return
    # We exit nonzero if ruff fixes anything, so we run with check=False to not raise:
    base = f"poetry run pre-commit run ruff-{{}} --files {shlex.join(map(str, file_paths))}"
    command = shlex.split(base.format("check"))
    print(shlex.join(command))
    subprocess.run(command, check=False, capture_output=True)
    command = shlex.split(base.format("format"))
    print(shlex.join(command))
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


def main(read_file: Path, dot_path_lookup: dict[str, str]) -> tuple[Path | None, bool]:
    """Generate sync client code for a given async API file then return the path to the generated file."""
    if read_file in KNOWN_FILES_SKIP_LIST:
        # print(f"- Skipping codegen for '{read_file}': on skip list ⏭️")
        return None, False

    source_code = get_source_code(read_file)
    class_name = find_api_class_name(source_code, read_file)
    write_file = SYNC_API_DIR / read_file.relative_to(ASYNC_API_DIR)
    if read_file.name == "__init__.py" and class_name is None:
        ensure_parent_dir(write_file)
        write_file.touch(exist_ok=True)
        # print(f"- Skipping codegen for '{read_file}': empty __init__.py file ⏭️")
        return write_file, False

    if class_name is None:
        raise RuntimeError(f"Could not find API class name in file='{read_file}'")
    read_file_hash = hash_file(read_file)
    if not file_has_changed(write_file, read_file_hash):
        # print(f"- Skipping codegen for '{read_file}': no changes detected ⏭️")
        return write_file, False

    generated_code = generate_sync_client_code(class_name, source_code, read_file, dot_path_lookup, read_file_hash)
    if generated_code is None:
        # print(f"- Skipping codegen for '{read_file}': on skip list ⏭️")
        return None, False

    ensure_parent_dir(write_file)
    write_file.write_text(generated_code)
    print(f"- Generated sync client code for: '{read_file}' ✅")
    return write_file, True


COGNITE_CLIENT_TEMPLATE = '''\
"""
===================================================
This file is auto-generated - do not edit manually!
===================================================
"""
from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import AsyncCogniteClient
{all_api_imports}

if TYPE_CHECKING:
    from cognite.client import ClientConfig


class CogniteClient:
    """Main entrypoint into the Cognite Python SDK.

    All Cognite Data Fusion APIs are accessible through this synchronous client.
    For the asynchronous client, see :class:`~cognite.client._cognite_client.AsyncCogniteClient`.

    Args:
        config (ClientConfig | None): The configuration for this client.
    """

    def __init__(self, config: ClientConfig | None = None) -> None:
        self.__async_client = async_client = AsyncCogniteClient(config)

        # Initialize all sync. APIs:
        {nested_apis_init}
'''


def filter_base_apis_and_sort_alphabetically(dct: dict[str, str]) -> list[tuple[str, str]]:
    return sorted((k, v) for k, v in dct.items() if "." not in v)


def create_sync_cognite_client(
    dot_path_lookup: dict[str, str],
    file_path_lookup: dict[str, str],
) -> None:
    all_apis = []
    all_imports = []
    for api, attr in filter_base_apis_and_sort_alphabetically(dot_path_lookup):
        override_api_name = foolish_cls_name_rewrite(api)
        all_apis.append(f"self.{attr} = Sync{override_api_name}(async_client)\n")

        import_path = path_as_importable(
            SYNC_API_DIR / Path(file_path_lookup[api]).relative_to(ASYNC_API_DIR.resolve())
        ).replace(".__init__", "")
        all_imports.append(f"from {import_path} import Sync{override_api_name}")

    content = COGNITE_CLIENT_TEMPLATE.format(
        file_hash="TODO", all_api_imports="\n".join(all_imports), nested_apis_init="        ".join(all_apis)
    )
    SYNC_CLIENT_PATH.write_text(content)
    print(f"- Generated sync CogniteClient in: '{SYNC_CLIENT_PATH}' ✅")


def clean_up_files(all_expected_files: list[Path]) -> None:
    clean_up = set(filter(is_pyfile, list_sync_apis())).difference(all_expected_files)
    if not clean_up:
        # print("No files to clean up!")
        return
    print(f"Cleaning up {len(clean_up)} files no longer needed:")
    for f in clean_up:
        print(f"- Deleting: '{f}'")
        f.unlink()


def setup_async_mock_client() -> AsyncCogniteClient:
    return AsyncCogniteClient(ClientConfig(client_name="name", project="proj", credentials=Token("not-a-token")))


if __name__ == "__main__":
    global_config.disable_pypi_version_check = True

    # We need a client to inspect, it does not need to be functional:
    async_client = setup_async_mock_client()

    # Let's say I have the SimulatorRoutineRevisionsAPI, and want to know the `simulators.routines.revisions`
    # (aka dotted) path to it so that I can magically do `self.__async_client.<dotted_path>.<method>`:
    dot_path_lookup, file_path_lookup = get_dot_path_lookup(async_client)

    # Run convert on all AsyncSomethingAPIs:
    all_expected_files = []
    files_needing_lint = []
    for read_file in filter(is_pyfile, list_apis()):
        try:
            write_file, was_modified = main(read_file, dot_path_lookup)
            if write_file is not None:
                all_expected_files.append(write_file)
            if was_modified:
                files_needing_lint.append(write_file)
        except Exception as e:
            print(f"- Failed to generate sync client code for: '{read_file}' ❌ {e}")
            continue

    # Invoke run via pre-commit (subprocess) as it doesn't have a python API interface:
    run_ruff(files_needing_lint)

    # Clean up files that are no longer needed:
    clean_up_files(all_expected_files)

    # Finally, gather all sync APIs into the CogniteClient class itself:
    create_sync_cognite_client(dot_path_lookup, file_path_lookup)
