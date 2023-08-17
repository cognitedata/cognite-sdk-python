from __future__ import annotations

import importlib
import inspect
import itertools
import re
from dataclasses import is_dataclass
from pathlib import Path

import numpy as np

from cognite.client.data_classes import TimeSeries
from cognite.client.data_classes.data_modeling.query import Query

EXCEPTIONS = {
    (Query, "__init__"),  # Reason: Uses a parameter 'with_'; and we need to escape the underscore
}

# Helper for testing specific class + method/property:
TESTING = False
ONLY_RUN = {
    (TimeSeries, "latest"),  # Just an example
}


class FalsePositiveDocstring(Exception):
    pass


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
            self.annotation, self.description = split_line.split(": ", 1)
            self.annotation = self.annotation.lstrip("(").rstrip(")")

        elif check.count("(") == check.count(")") == 0:
            # ...we probably don't:
            self.var_name, self.description = line.strip().split(": ", 1)
            self.annotation = ""
        else:
            raise ValueError

    def __repr__(self):
        return f"Param({self.var_name!r}, {self.annotation!r}, {self.description!r})"


class ReturnParam(Param):
    def _parse(self, line: str):
        line = line.strip()
        if ":" not in line:
            # Better to guess wrong and not lose description - we can always recreate the annotation:
            self.annotation, self.description = "", line
        else:
            self.annotation, self.description = line.split(": ", 1)

    def __repr__(self):
        return f"Param({self.annotation!r}, {self.description!r})"


def count_indent(s):
    return re.search("[^ ]", s + "x").start()


class DocstrFormatter:
    def __init__(self, doc, method):
        self.original_doc = doc
        self.is_generator = inspect.isgeneratorfunction(method)
        self.RETURN_STRING = "Yields:" if self.is_generator else "Returns:"

        self.lines_grouped, self.indentation = self._separate_docstring(doc)
        self.actual_annotations, self.actual_return_annotation = self._extract_annotations(method)
        self.parse_doc_and_store(self.lines_grouped, self.indentation)

    def _extract_annotations(self, method):
        def fix_literal(s):
            # Example: Union[Literal[('aaa', 'bbb')]] -> Union[Literal["aaa", "bbb"]]
            if match := re.search(r"Literal\[(\((.*)\))\]", s):
                return s.replace(match.group(1), match.group(2).replace("'", '"'))
            return s

        annots = {}
        if isinstance(method, property):
            method_signature = inspect.signature(lambda: ...)  # just 'self' anyways
            return_annot = method.fget.__annotations__.get("return", inspect._empty)
        else:
            method_signature = inspect.signature(method)
            return_annot = method_signature.return_annotation

        if return_annot is inspect._empty:
            raise ValueError("Missing return type annotation")

        for var_name, param in method_signature.parameters.items():
            if var_name in {"self", "cls"}:
                continue
            if param.kind is param.VAR_POSITIONAL:
                var_name = "*" + var_name
            elif param.kind is param.VAR_KEYWORD:
                var_name = "**" + var_name
            annots[var_name] = fix_literal(str(param.annotation))

        return annots, return_annot

    @staticmethod
    def _separate_docstring(doc):
        lines = doc.splitlines()
        indentations = np.array(list(map(count_indent, lines)))
        if any(indentations % 4 != 0):
            # Developer-help to find wrongly indented lines: (uncomment and run again)
            # for info in zip(indentations, indentations % 4 != 0, lines):
            #     print(*info)
            raise ValueError("One or more lines is not indented a multiple of 4 spaces")

        # TODO: Short, or only-text docstrings is most likely ok to skip, at least for now:
        if len(non_zero := np.nonzero(indentations)[0]) == 0:
            raise FalsePositiveDocstring

        section_indent = indentations[(non_zero,)].min()
        all_chunks, chunk = [], []
        for line, indent in zip(lines, indentations):
            # If a line is non-empty, remove any excess whitespace at end:
            if line.strip():
                line = line.rstrip()

            if indent != section_indent:
                chunk.append(line)
            else:
                all_chunks.append(chunk)
                chunk = [line]
        all_chunks.append(chunk)
        return all_chunks, section_indent

    def parse_doc_and_store(self, lines_grouped, indentation):
        self.parameters, self.return_parameter = [], None
        self.line_args_group, self.line_return_group = None, None

        for line_group in lines_grouped:
            if (first := line_group[0].strip()) == "Args:":
                self.line_args_group = line_group
            elif len(first.split("Args:")) > 1:
                raise ValueError("'Args:'-line contains additional text")

            elif first == self.RETURN_STRING:
                self.line_return_group = line_group
            elif len(first.split(self.RETURN_STRING)) > 1:
                raise ValueError(f"'{self.RETURN_STRING}'-line contains additional text")

        self.add_space_after_args, self.add_space_after_returns = False, False
        if self.line_args_group is not None:
            self.parameters = list(map(Param, DocstrFormatter._parse_args_section(self.line_args_group, indentation)))
            self.add_space_after_args = not self.line_args_group[-1].strip()

        if self.line_return_group is not None:
            self.return_parameter = DocstrFormatter._parse_returns_section(self.line_return_group, indentation)
            self.add_space_after_returns = not self.line_return_group[-1].strip()

    @staticmethod
    def _parse_args_section(lines, indent):
        parameters = []
        for line in lines[1:]:
            if not line.strip():
                continue

            if count_indent(line) == indent + 4:
                parameters.append(line)

            elif parameters:
                # Assume multilines belong to the previous line
                parameters[-1] += f" {line.strip()}"
            else:
                # We can infer that this line is indented wrong (and prob the rest, best to raise):
                raise ValueError(
                    "First parameter description after 'Args:' is not indented correctly (should be 4 spaces)"
                )

        return parameters

    @staticmethod
    def _parse_returns_section(lines, indent):
        line = lines[1].strip()
        for extra_line in lines[2:]:
            # Assume multilines regardless of (missing extra) indentation belongs:
            line += f" {extra_line.strip()}"
        return ReturnParam(line)

    def docstring_is_correct(self):
        return_annot_is_correct = False
        if self.actual_return_annotation == "None":
            # If the function returns None, we don't want a returns-section:
            return_annot_is_correct = self.return_parameter is None
        elif self.return_parameter is not None:
            return_annot_is_correct = self.actual_return_annotation == self.return_parameter.annotation

        parsed_annotations = dict((p.var_name, p.annotation) for p in self.parameters)
        parameters_are_correct = (
            # Takes no args?
            not self.actual_annotations
            # Do the variables match? ...correct order?
            or list(self.actual_annotations.keys()) == list(parsed_annotations.keys())
            # Do the annotations match?
            and list(self.actual_annotations.values()) == list(parsed_annotations.values())
        )
        return return_annot_is_correct and parameters_are_correct

    def _create_docstring_param_description(self):
        if not self.actual_annotations:
            return []
        whitespace = " " * self.indentation
        fixed_lines = [f"{whitespace}Args:"]
        doc_descr = dict((p.var_name, p.description) for p in self.parameters)
        for var, annot in self.actual_annotations.items():
            description = doc_descr.get(var, "No description.")
            fixed_lines.append(f"{whitespace}    {var} ({annot}): {description}")
        if self.add_space_after_args:
            fixed_lines.append("")
        return fixed_lines

    def _create_docstring_return_description(self):
        if self.actual_return_annotation == "None":
            return []

        description = "No description."
        if self.return_parameter is not None:
            description = self.return_parameter.description

        whitespace = " " * self.indentation
        fixed_lines = [
            f"{whitespace}{self.RETURN_STRING}",
            f"{whitespace}    {self.actual_return_annotation}: {description}",
        ]
        if self.add_space_after_returns:
            fixed_lines.append("")
        return fixed_lines

    def create_docstring(self):
        final_doc_lines = []
        args_missing = self.line_args_group is None
        return_missing = self.line_return_group is None

        # Reconstruct the docstring, modifying params + return sections:
        for lines in self.lines_grouped:
            if lines is self.line_args_group:
                lines = self._create_docstring_param_description()
                if return_missing:
                    lines += self._create_docstring_return_description()

            elif lines is self.line_return_group:
                lines = self._create_docstring_return_description()
                if args_missing:
                    # If return section exists, but args section doesnt, we can't add 'args' later (wrong order):
                    lines = self._create_docstring_param_description() + lines

            final_doc_lines.extend(lines)

        # If both sections are missing, we punch them in at the end, hopefully the user will move if weird order
        if args_missing and return_missing:
            lines = self._create_docstring_param_description() + self._create_docstring_return_description()
            final_doc_lines.extend(lines)

        # Remove unwanted space from right, but keep for last (avoid moving closing triple quote):
        last = final_doc_lines[-1]
        final_doc_lines = [s.rstrip() for s in final_doc_lines]
        if not last.strip():
            final_doc_lines[-1] = last

        return "\n".join(final_doc_lines)

    def update_py_file(self, cls, attr) -> str:
        source_code = (path := Path(inspect.getfile(cls))).read_text()

        was_tested = f"{cls.__name__}.{attr}"
        if (n_matches := source_code.count(self.original_doc)) == 0:
            return f"Couldn't fix docstring for '{was_tested}', as the old doc was not found in the file"

        elif n_matches == 1:
            new_docstr = self.create_docstring()
            if self.original_doc == new_docstr:
                # Shouldn't be possible, but surely it will happen :D
                raise RuntimeError(
                    "Existing docstring was considered wrong, but the newly generated one was identical... "
                    "If pre-commit does not report any other errors, consider committing using '--no-verify' "
                    "and create an issue on github!"
                )

            path.write_text(source_code.replace(self.original_doc, new_docstr))
            return f"Fixed docstring for '{was_tested}'"

        else:
            return f"Couldn't fix docstring for '{was_tested}', as the old doc was not unique to the file"


def get_all_non_inherited_attributes(cls):
    return [
        (attr, method)
        for attr, method in inspect.getmembers(
            cls, predicate=lambda method: inspect.isfunction(method) or isinstance(method, property)
        )
        if attr in cls.__dict__
    ]


def format_docstring(cls) -> list[str]:
    failed = []
    for attr, method in get_all_non_inherited_attributes(cls):
        if (cls, attr) in EXCEPTIONS:
            continue

        if TESTING and (cls, attr) not in ONLY_RUN:
            continue

        # The __init__ method is documented in the class level docstring
        is_init = attr == "__init__"
        doc = cls.__doc__ if is_init else method.__doc__

        if not doc or (is_init and is_dataclass(cls)):
            continue

        try:
            doc_fmt = DocstrFormatter(doc, method)
        except FalsePositiveDocstring:
            continue
        except (ValueError, IndexError) as e:
            failed.append(
                f"Couldn't parse parameters in docstring for '{cls.__name__}.{attr}', "
                f"please inspect manually. Reason: {e}"
            )
            continue

        if not doc_fmt.docstring_is_correct():
            if err_msg := doc_fmt.update_py_file(cls, attr):
                failed.append(err_msg)
    return failed


def find_all_classes_and_funcs_in_sdk():
    def predicate(obj):
        return inspect.isclass(obj) or inspect.isfunction(obj)

    locations = [".".join(p.parts)[:-3] for p in Path("cognite/client/").glob("**/*.py") if "_pb2.py" not in str(p)]
    return {
        cls_or_fn
        for loc in locations
        for _, cls_or_fn in inspect.getmembers(importlib.import_module(loc), predicate=predicate)
        if cls_or_fn.__module__.startswith("cognite.client.")
    }


def format_docstrings() -> list[str]:
    return "\n".join(itertools.chain.from_iterable(map(format_docstring, find_all_classes_and_funcs_in_sdk())))
