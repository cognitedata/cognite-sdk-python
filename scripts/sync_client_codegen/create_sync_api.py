import ast
import dataclasses
import tempfile
import textwrap
from argparse import Namespace
from dataclasses import dataclass
from pathlib import Path

from scripts.sync_client_codegen.codegen_utils import (
    clean_up_files,
    ensure_parent_dir,
    file_has_changed,
    find_api_class_name,
    find_class_node,
    find_self_assignments,
    fix_imports_for_sync_apis,
    foolish_cls_name_rewrite,
    get_all_imports,
    get_canonical_source,
    get_dot_path_lookup,
    get_source_code,
    hash_file,
    is_pyfile,
    list_apis,
    list_sync_apis,
    method_should_be_converted,
    read_hash_from_file,
    run_ruff,
    setup_async_mock_client,
)
from scripts.sync_client_codegen.constants import (
    ASYNC_API_DIR,
    EIGHT_SPACES,
    FOUR_SPACES,
    KNOWN_FILES_SKIP_LIST,
    SYNC_API_DIR,
    SYNC_CLIENT_PATH,
)
from scripts.sync_client_codegen.create_sync_client import (
    create_sync_cognite_client,
    verify_cognite_client_is_up_to_date,
)

SYNC_API_TEMPLATE = Path("scripts/sync_client_codegen/sync_api_template.txt").read_text()


def _generate_code_for_single_sync_api(
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
            "EventsAPI -> 'events', DatapointsAPI -> 'time_series.data'."
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
        # The last definition is the function implementation, the rest are overloads:
        overloads = method_nodes[:-1]
        func = method_nodes[-1]

        for overload_node in overloads:
            sync_def = "{indent}@overload\n{indent}def {name}({args}) -> {return_type}: ...".format(
                indent=FOUR_SPACES,
                name=name,
                args=ast.unparse(overload_node.args),
                return_type=ast.unparse(overload_node.returns).replace("AsyncIterator", "Iterator"),
            )
            generated_methods.append(sync_def)

        docstring = ast.get_docstring(func)

        # Create the list of arguments to pass to the async call
        call_parts = []
        # 1. Handle positional-only arguments (e.g., func(a, /))
        call_parts.extend([arg.arg for arg in func.args.posonlyargs])
        # 2. Handle regular arguments (can be pos or keyword)
        # We will pass these by keyword for safety.
        regular_args = [f"{arg.arg}={arg.arg}" for arg in func.args.args if arg.arg != "self"]
        call_parts.extend(regular_args)
        # 3. Handle variadic positional arguments (*args)
        if func.args.vararg:
            call_parts.append(f"*{func.args.vararg.arg}")
        # 4. Handle keyword-only arguments (e.g., func(*, a))
        kw_only_args = [f"{arg.arg}={arg.arg}" for arg in func.args.kwonlyargs]
        call_parts.extend(kw_only_args)
        # 5. Handle variadic keyword arguments (**kwargs)
        if func.args.kwarg:
            call_parts.append(f"**{func.args.kwarg.arg}")

        # Check return type for AsyncIterator
        return_type_str = ast.unparse(func.returns)
        is_iterator = "AsyncIterator" in return_type_str
        is_async_fn = isinstance(func, ast.AsyncFunctionDef)
        sync_return_type = return_type_str.replace("AsyncIterator", "Iterator")

        maybe_name = "" if name == "__call__" else f".{name}"
        nested_client_call = f"self.__async_client.{dotted_path}{maybe_name}({', '.join(call_parts)})"
        if is_iterator:
            # We add type ignore because mypy fail at unions of coroutines... (pyright on the other hand)
            method_body = f"yield from SyncIterator({nested_client_call})  # type: ignore [misc]"
        elif is_async_fn:
            method_body = f"return run_sync({nested_client_call})"
        else:
            method_body = f"return {nested_client_call}"

        # Decorators not typing.overload:
        decorators = maybe_self = ""
        if not is_async_fn:
            for deco in func.decorator_list:
                if deco.id == "staticmethod":
                    # Uhm.. what? Well, we delegate to self.__async_client...
                    maybe_self = "self, "
                else:
                    decorators += f"{FOUR_SPACES}@{ast.unparse(deco)}\n"

        indented_docstring = ""
        if docstring:
            maybe_noqa = "  # noqa: DOC404" if is_iterator else ""
            indented_docstring = (
                f'{EIGHT_SPACES}"""\n{textwrap.indent(docstring, EIGHT_SPACES)}\n{EIGHT_SPACES}"""{maybe_noqa}\n'
            )

        impl_def = decorators + (
            f"{FOUR_SPACES}def {name}({maybe_self}{ast.unparse(func.args)}) -> {sync_return_type}:\n"
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
        + "\n\n".join(generated_methods)
        + "\n"
    )


@dataclass
class SingleAPIFile:
    filepath: Path
    read_filepath: Path
    class_name: str | None = None
    new_source: str | None = dataclasses.field(default=None, repr=False)
    new_hash: str | None = None

    def __hash__(self) -> int:
        return hash(id(self))

    @property
    def get_new_source(self) -> str:
        assert self.new_source is not None
        return self.new_source

    @property
    def get_new_hash(self) -> str:
        assert self.new_hash is not None
        return self.new_hash

    @property
    def temp_filepath(self) -> Path:
        return self._tmp_filepath

    @temp_filepath.setter
    def temp_filepath(self, value: Path) -> None:
        self._tmp_filepath = value


def _maybe_regenerate_file(
    read_file: Path, dot_path_lookup: dict[str, str], args: Namespace, verify: bool
) -> SingleAPIFile | None:
    """
    Evaluates whether the read file (an async API) needs to have its sync counterpart regenerated.
    If so, performs the regeneration and writes to disk.

    Returns:
        SingleAPIFile | None: The path to the written sync API file (or None if skipped),
            and a bool indicating whether the file was modified on disk.
    """
    if not read_file.is_relative_to(ASYNC_API_DIR):
        if args.verbose:
            print(f"- Skipping codegen for '{read_file}': not in {ASYNC_API_DIR} ⏭️")
        return None

    if read_file in KNOWN_FILES_SKIP_LIST:
        if args.verbose and not verify:
            print(f"- Skipping codegen for '{read_file}': on skip list ⏭️")
        return None

    source_code = get_source_code(read_file)
    class_name = find_api_class_name(source_code, read_file)
    write_file = SYNC_API_DIR / read_file.relative_to(ASYNC_API_DIR)
    if class_name is None:
        if read_file.name != "__init__.py":
            raise RuntimeError(f"Could not find API class name in file='{read_file}'")

        if not verify:
            ensure_parent_dir(write_file)
            write_file.touch(exist_ok=True)
            if args.verbose:
                print(f"- Skipping codegen for '{read_file}': empty __init__.py file ⏭️")
        return SingleAPIFile(write_file, read_file)

    read_file_hash = hash_file(read_file)
    if not file_has_changed(write_file, read_file_hash):
        if args.verbose and not verify:
            print(f"- Skipping codegen for '{read_file}': no changes detected ⏭️")
        return SingleAPIFile(write_file, read_file)

    generated_code = _generate_code_for_single_sync_api(
        class_name, source_code, read_file, dot_path_lookup, read_file_hash
    )
    if generated_code is None:
        if args.verbose and not verify:
            print(f"- Skipping codegen for '{read_file}': on skip list ⏭️")
        return None

    return SingleAPIFile(
        write_file,
        read_filepath=read_file,
        class_name=class_name,
        new_source=generated_code,
        new_hash=read_file_hash,
    )


def _compare_normalized_files_and_maybe_update_hash_only(files_to_update: set[SingleAPIFile], args: Namespace):
    with tempfile.TemporaryDirectory() as tmp_dir:
        for f in files_to_update:
            f.temp_filepath = tmp_dir / f.filepath
            ensure_parent_dir(f.temp_filepath)
            f.temp_filepath.write_text(f.get_new_source)

        # Before compare, we need to run ruff (large overhead, so we do it once for all files):
        run_ruff([f.temp_filepath for f in files_to_update], verbose=False)

        for f in files_to_update.copy():
            write_file = f.filepath
            normalized_src = get_canonical_source(write_file)
            normalized_new_src = get_canonical_source(f.temp_filepath)

            try:
                is_valid, existing_hash = read_hash_from_file(write_file)
                if not is_valid:
                    continue
            except FileNotFoundError:
                continue

            # The hash is part of the module docstring, which gets preserved, so we must
            # replace it before comparing:
            normalized_src = normalized_src.replace(existing_hash, f.get_new_hash)
            if normalized_src != normalized_new_src:
                continue

            # We just need to update the hash of this file:
            files_to_update.remove(f)

            if args.verbose:
                print(
                    f"- Updating hash only for '{f.read_filepath}'. The newly generated code is functionally "
                    "identical to current (including docstrings) ✅"
                )
            else:
                print(f"- Updating hash only for '{f.read_filepath}' ✅")
            ensure_parent_dir(write_file)
            # Note: DO NOT use normalized_src here, the reason we do all of this is precisely to preserve
            # e.g. type-ignore comments that would otherwise be lost on regeneration.
            current_source = get_source_code(write_file)
            write_file.write_text(current_source.replace(existing_hash, f.get_new_hash))


def _report_after_verification(
    all_expected_files: list[Path],
    files_to_update: set[SingleAPIFile],
    something_failed: bool,
    client_has_changed: bool,
) -> bool:
    should_be_cleaned_up = set(filter(is_pyfile, list_sync_apis())).difference(all_expected_files)

    if not (should_be_cleaned_up or files_to_update or something_failed or client_has_changed):
        print("All sync client/API files are up to date and nothing needs to be cleaned up ✅")
        return True

    if something_failed:
        print("One or more errors occurred during code generation/verification ❌")
    if files_to_update:
        print(
            "One or more sync API files are not up to date. Hint, use one of:\n"
            "$ python scripts/sync_client_codegen/main.py run --all-files\n"
            "$ python scripts/sync_client_codegen/main.py run --files FILE1 FILE2"
        )
        for not_updated in files_to_update:
            print(f"- File needs update: '{not_updated}' ❌")
    if should_be_cleaned_up:
        print("One or more sync API files are stale and should be removed:")
        for stale_file in should_be_cleaned_up:
            print(f"- Stale file: '{stale_file}' ❌")
    if client_has_changed:
        print(
            "The (sync) CogniteClient source is outdated ❌. You probably want to run:\n"
            "$ python scripts/sync_client_codegen/main.py run --all-files"
        )
    return False


def _write_updated_sync_api_files(files_to_update: set[SingleAPIFile]) -> None:
    for f in files_to_update:
        ensure_parent_dir(f.filepath)
        f.filepath.write_text(f.get_new_source)
        if f.class_name:
            print(f"- Generated/updated sync API code for {f.class_name} from '{f.read_filepath}' ✅")
        else:
            print(f"- Generated/updated sync API code from '{f.read_filepath}' ✅")


def _run_files(
    args: Namespace,
    files: list[Path] | None,
    cleanup: bool = False,
    verify: bool = False,
) -> bool | None:
    all_files = files is None
    if all_files:
        files = list(filter(is_pyfile, list_apis()))

    assert not (cleanup and verify), "Cannot cleanup when in verify mode."
    assert not cleanup or all_files, "Cleanup can only be done when running all files."

    # We need a client to inspect, it does not need to be functional:
    async_client = setup_async_mock_client()

    # Let's say I have the SimulatorRoutineRevisionsAPI, and want to know the `simulators.routines.revisions`
    # (aka dotted) path to it so that I can magically do `self.__async_client.<dotted_path>.<method>`:
    dot_path_lookup, file_path_lookup = get_dot_path_lookup(async_client)

    all_expected_files = []
    files_to_update: set[SingleAPIFile] = set()
    something_failed = False
    for read_file in files:
        try:
            api_write_file = _maybe_regenerate_file(read_file, dot_path_lookup, args, verify)
            if api_write_file is not None:
                all_expected_files.append(api_write_file.filepath)
                if api_write_file.new_source:
                    files_to_update.add(api_write_file)
        except Exception as e:
            print(f"- Failed to generate sync client code for: '{read_file}' ❌ {e}")
            something_failed = True
            continue

    # Gather all sync APIs into the CogniteClient class itself and check if up-to-date:
    new_client_source_code = create_sync_cognite_client(dot_path_lookup, file_path_lookup)
    client_has_changed = verify_cognite_client_is_up_to_date(new_client_source_code)

    if verify:
        return _report_after_verification(all_expected_files, files_to_update, something_failed, client_has_changed)

    # Before we overwrite existing files, we want to compare if the changes are functionally
    # identical (including docstrings, excluding comments and the stored file hash):
    _compare_normalized_files_and_maybe_update_hash_only(files_to_update, args)
    to_be_linted = [f.filepath for f in files_to_update]

    if client_has_changed:
        to_be_linted.append(SYNC_CLIENT_PATH)
        SYNC_CLIENT_PATH.write_text(new_client_source_code)
        print(f"- Regenerated (sync) CogniteClient: '{SYNC_CLIENT_PATH}' ✅")
    elif args.verbose:
        print(f"- Skipped (sync) CogniteClient, is up to date: '{SYNC_CLIENT_PATH}' ⏭️")

    # Write all updated files to disk, then invoke formatting (ruff) run via pre-commit
    # (subprocess) as it doesn't have a python API interface:
    _write_updated_sync_api_files(files_to_update)
    run_ruff(to_be_linted, args.verbose)

    # (Maybe) clean up files that are no longer needed:
    if cleanup:
        if something_failed:
            if args.verbose:
                print("One or more errors occurred, skipping cleanup of stale files.")
        else:
            clean_up_files(all_expected_files, args.verbose)


def run_specific_files(args: Namespace, files: list[str]) -> None:
    """Runs codegen for specific files only."""
    paths = list(map(Path, files))
    _run_files(args, paths)


def run_all_files(args: Namespace) -> None:
    """Runs codegen for all files and includes a cleanup step to remove stale files."""
    _run_files(args, files=None, cleanup=True)


def run_verify(args: Namespace) -> None:
    """Verifies that all sync API files are up to date and no stale files exist."""
    success = _run_files(args, files=None, cleanup=False, verify=True)
    if not success:
        # Signal to CI that we failed verification with nonzero exit code:
        raise SystemExit(1)
