from __future__ import annotations

import random
import re
import string
from collections.abc import Iterator
from functools import lru_cache
from typing import Any

from cognite.client.utils.useful_types import SequenceNotStr


class DrawTables:
    HLINE = "\N{BOX DRAWINGS LIGHT HORIZONTAL}"
    VLINE = "\N{BOX DRAWINGS LIGHT VERTICAL}"
    XLINE = "\N{BOX DRAWINGS LIGHT VERTICAL AND HORIZONTAL}"
    TOPLINE = "\N{BOX DRAWINGS LIGHT DOWN AND HORIZONTAL}"


def random_string(size: int = 100, sample_from: str = string.ascii_uppercase + string.digits) -> str:
    return "".join(random.choices(sample_from, k=size))


@lru_cache(maxsize=128)
def to_camel_case(snake_case_string: str) -> str:
    components = snake_case_string.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


@lru_cache(maxsize=128)
def to_snake_case(camel_case_string: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", camel_case_string)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def iterable_to_case(seq: SequenceNotStr[str], camel_case: bool) -> Iterator[str]:
    if camel_case:
        yield from map(to_camel_case, seq)
    else:
        yield from map(to_snake_case, seq)


def convert_all_keys_to_camel_case(dct: dict[str, Any]) -> dict[str, Any]:
    try:
        return dict(zip(map(to_camel_case, dct.keys()), dct.values()))
    except AttributeError:
        raise TypeError("Expected a dictionary")


def convert_all_keys_to_camel_case_recursive(dct: dict[str, Any]) -> dict[str, Any]:
    """Converts all the dictionary keys from snake to camel cases included nested objects.
    >>> convert_all_keys_to_camel_case_recursive({"my_key": {"my_key": 1}})
    {'myKey': {'myKey': 1}}
    """
    return {
        to_camel_case(k): (convert_all_keys_to_camel_case_recursive(v) if isinstance(v, dict) else v)
        for k, v in dct.items()
    }


def convert_all_keys_recursive(dct: dict[str, Any], camel_case: bool = False) -> dict[str, Any]:
    """Converts all the dictionary keys from snake to camel cases included nested objects.
    >>> convert_all_keys_recursive({"my_key": {"my_key": 1}}, camel_case=True)
    {'myKey': {'myKey': 1}}
    """
    return {
        (to_camel_case(k) if camel_case else k): (
            convert_all_keys_recursive(v, camel_case) if isinstance(v, dict) else v
        )
        for k, v in dct.items()
    }


def convert_all_keys_to_snake_case(dct: dict[str, Any]) -> dict[str, Any]:
    return dict(zip(map(to_snake_case, dct.keys()), dct.values()))


def convert_dict_to_case(dct: dict[str, Any], camel_case: bool) -> dict[str, Any]:
    if camel_case:
        return convert_all_keys_to_camel_case(dct)
    return convert_all_keys_to_snake_case(dct)


def shorten(obj: Any, width: int = 20, placeholder: str = "...") -> str:
    # 'textwrap.shorten' skips entire words... so we make our own:
    if width < (n := len(placeholder)):
        raise ValueError("Width must be larger than or equal to the length of 'placeholder'")

    s = obj if isinstance(obj, str) else repr(obj)
    if len(s) <= width:
        return s
    return f"{s[:width-n]}{placeholder}"
