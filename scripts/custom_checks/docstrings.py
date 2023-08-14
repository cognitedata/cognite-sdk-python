from __future__ import annotations

import inspect
import re
from pathlib import Path
from dataclasses import is_dataclass

from cognite.client._api_client import APIClient
from cognite.client.data_classes._base import CogniteResource
from tests.utils import all_subclasses

CWD = Path.cwd()


class Param:
    def __init__(self, line: str):
        try:
            self._parse(line)
        except ValueError:
            raise ValueError(f"Unable to parse line: {line.strip()!r}")

    def _parse(self, line: str):
        if ":" not in line:
            line += ": No description."

        check = line.split(":", 1)[0]
        if check.count("(") and line[(idx := line.index("(")) - 1] != " ":
            # User forgot to add a space before first parenthesis:
            line = f"{line[:idx]} {line[idx:]}"

        if check.count("(") or check.count(")"):
            # We probably have an annotation:
            self.var_name, split_line = line.strip().split(" ", 1)
            self.annot, self.descr = split_line.split(": ", 1)
            self.annot = self.annot.lstrip("(").rstrip(")")

        elif check.count("(") == check.count(")") == 0:
            # ...we probably don't:
            self.var_name, self.descr = line.strip().split(": ", 1)
            self.annot = ""
        else:
            raise ValueError

    def __repr__(self):
        return f"Param({self.var_name!r}, {self.annot!r}, {self.descr!r})"


def count_indent(s):
    return re.search("[^ ]", s + "x").start()


class DocstrFormatter:
    NO_ANNOT = object()

    def __init__(self, doc, method):
        self.params = []
        self.original_doc = doc
        self.doc_annots, self.return_annot = self._extract_annotations(method)
        if not self.doc_annots:  # Function takes no args
            return

        self.doc_before, self.doc_after, args_indent, parameters = self.parse_doc(doc)
        if args_indent is None:
            self.doc_annots = {}  # Hack to quit as no parameters were found
            return

        if not self.doc_after:
            # Make sure closing triple-quote aligns correctly:
            self.doc_after.append(" " * args_indent)

        self.indentation = args_indent + 4
        self.params = list(map(Param, parameters))

    def _extract_annotations(self, method):
        def fix_literal(s):
            # Example: Union[Literal[('aaa', 'bbb')]] -> Union[Literal["aaa", "bbb"]]
            if match := re.search(r"Literal\[(\((.*)\))\]", s):
                return s.replace(match.group(1), match.group(2).replace("'", '"'))
            return s

        annots = {var: fix_literal(str(annot)) for var, annot in method.__annotations__.items()}
        return_annot = annots.pop("return", self.NO_ANNOT)
        return annots, return_annot

    @staticmethod
    def parse_doc(doc):
        idx_start, idx_end = None, None
        args_indent = None
        start_capture = False
        parameters = []

        lines = doc.splitlines()
        if not lines[-1].strip():
            lines[:-1] = [line.rstrip() for line in lines[:-1]]

        for i, line in enumerate(lines):
            line_indent = count_indent(line)
            if start_capture:
                if not line.strip():
                    continue
                if line_indent == args_indent:
                    idx_end = i
                    break
                if line_indent > args_indent + 4:
                    # Assume multilines belong to previous line:
                    parameters[-1] += f" {line.strip()}"
                    continue
                parameters.append(line)
                continue

            elif "args:" in line.lower():
                args_indent = line_indent
                start_capture = True
                idx_start = i + 1
        else:
            # End was not found:
            idx_end = len(lines)

        return lines[:idx_start], lines[idx_end:], args_indent, parameters

    def docstring_is_correct(self):
        annots = dict((p.var_name, p.annot) for p in self.params)
        return (
            # Takes no args?
            not self.doc_annots
            # Do the variables match? ...correct order?
            or list(self.doc_annots.keys()) == list(annots.keys())
            # Do the annotations match?
            and list(self.doc_annots.values()) == list(annots.values())
        )

    def _create_docstring_param_description(self):
        whitespace = " " * self.indentation
        fixed_lines = []
        doc_annot_dct = dict((p.var_name, p.descr) for p in self.params)
        for var, annot in self.doc_annots.items():
            description = doc_annot_dct.get(var, "No description.")
            fixed_lines.append(f"{whitespace}{var} ({annot}): {description}")
        return fixed_lines

    def create_docstring(self):
        fixed_param_description = self._create_docstring_param_description()
        return "\n".join(self.doc_before + fixed_param_description + self.doc_after)

    def update_py_file(self, cls, attr) -> str:
        source_code = (path := Path(inspect.getfile(cls))).read_text()
        new_source = source_code.replace(self.original_doc, self.create_docstring())
        if source_code == new_source:
            return f"Couldn't update docstring for '{cls.__name__}.{attr}', please inspect manually"

        with path.open("w") as file:
            file.write(new_source)

        return f"Fixed docstring for '{cls.__name__}.{attr}'"


def get_all_methods(cls):
    return [(attr, method) for attr in dir(cls) if inspect.isfunction(method := getattr(cls, attr))]


def format_docstrings_for_subclasses(cls) -> list[str]:
    failed = []
    for cls in all_subclasses(cls):
        for attr, method in get_all_methods(cls):
            # The __init__ method is documented in the class level docstring
            is_init = attr == "__init__"
            doc = cls.__doc__ if is_init else method.__doc__

            if not doc or (is_init and is_dataclass(cls)):
                continue

            try:
                doc_fmt = DocstrFormatter(doc, method)
            except ValueError as e:
                failed.append(
                    f"Couldn't parse parameters in docstring for '{cls.__name__}.{attr}', "
                    f"please inspect manually. Reason: {e}"
                )
                continue

            if not doc_fmt.docstring_is_correct():
                if err_msg := doc_fmt.update_py_file(cls, attr):
                    failed.append(err_msg)
    return failed


def format_docstrings() -> list[str]:
    # TODO: Add more baseclasses to parse, e.g. CogniteResourceList?:
    return "\n".join(
        sum((format_docstrings_for_subclasses(base_cls) for base_cls in [APIClient, CogniteResource]), [])
    )
