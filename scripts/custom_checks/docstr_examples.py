import importlib
import textwrap
from inspect import getdoc, getmembers, isclass, isfunction
from pathlib import Path

WS = " " * 8


def path_to_importable(path: Path) -> str:
    return ".".join(path.with_suffix("").parts)


def get_valid_members(module, dotted):
    for cls_name, cls in getmembers(module, predicate=isclass):
        if cls.__module__ != dotted:
            continue  # was imported from elsewhere
        elif cls_name.startswith("_"):
            continue
        yield cls_name, cls


def get_info_on_valid_methods(cls, full_text):
    for method_name, method in getmembers(cls, predicate=isfunction):
        if (docstr := getdoc(method)) is None:
            continue
        docstr = textwrap.indent(docstr, WS)[8:]
        if docstr not in full_text:
            # Unable to match the docstring to the source code (probably whitespace is not == 8...)
            continue
        lines = docstr.splitlines()
        try:
            ex_idx = lines.index(WS + "Examples:")
        except ValueError:
            continue
        yield method_name, ex_idx, docstr, lines


def parse_example_section(lines, ex_idx) -> tuple[bool, set[int]]:
    method_needs_fix = False
    drop_idxs = set()  # lines we should remove
    client_import_count, client_instaniate_count = 0, 0

    for i, line in enumerate(lines[ex_idx:], ex_idx):
        ls = line.strip()
        if ls.endswith("::"):
            method_needs_fix = True
            lines[i] = line.replace("::", ":")

        match ls:
            case ">>> client = CogniteClient()":
                client_instaniate_count += 1
                if client_instaniate_count > 1:
                    method_needs_fix = True
                    drop_idxs.add(i)
            case ">>> from cognite.client import CogniteClient":
                client_import_count += 1
                if client_import_count > 1:
                    method_needs_fix = True
                    drop_idxs.add(i)
    return method_needs_fix, drop_idxs


def fix_single_file(path: Path) -> str | None:
    was_fixed = []
    dotted = path_to_importable(path)
    full_text = path.read_text(encoding="utf-8")
    module = importlib.import_module(dotted)
    for cls_name, cls in get_valid_members(module, dotted):
        for method_name, ex_idx, docstr, lines in get_info_on_valid_methods(cls, full_text):
            method_needs_fix, drop_idxs = parse_example_section(lines, ex_idx)
            if not method_needs_fix:
                continue
            elif drop_idxs:
                lines = [line for i, line in enumerate(lines) if i not in drop_idxs]
            full_text = full_text.replace(docstr, "\n".join(lines))
            was_fixed.append(f"Fixed docstring examples for '{cls_name}.{method_name}'")
    # We need to store all changes to the file in one go:
    if was_fixed:
        path.write_text(full_text)
        return "\n".join(was_fixed)


def format_docstring_examples(files: list[Path]) -> str:
    return "\n".join(filter(None, map(fix_single_file, files)))
