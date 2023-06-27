from string import ascii_lowercase

import pytest

from cognite.client.utils._text import (
    convert_all_keys_to_camel_case,
    convert_all_keys_to_snake_case,
    iterable_to_case,
    random_string,
    shorten,
    to_camel_case,
    to_snake_case,
)
from tests.utils import rng_context


def test_random_string():
    # Truly pointless, but the things we do for coverage
    with rng_context(7_999_878):
        assert "abcde" == random_string(5, sample_from=ascii_lowercase)


@pytest.mark.parametrize(
    "obj, width, placeholder, expected",
    (
        (ascii_lowercase, 26, "...", ascii_lowercase),
        (ascii_lowercase, 5, "...", "ab..."),
        (ascii_lowercase, 3, "...", "..."),
        ([1, 2, 3], 9, "...]", "[1, 2, 3]"),
        ([1, 2, 3], 8, "...]", "[1, ...]"),
    ),
)
def test_shorten(obj, width, placeholder, expected):
    assert expected == shorten(obj, width, placeholder)


def test_shorten__fails():
    with pytest.raises(ValueError, match="^Width must be larger than "):
        shorten(object(), width=2, placeholder="...")


@pytest.mark.parametrize(
    "inp, expected",
    (
        ("camel_case", "camelCase"),
        ("camelCase", "camelCase"),
        ("a", "a"),
    ),
)
def test_to_camel_case(inp, expected):
    assert expected == to_camel_case(inp)


@pytest.mark.parametrize(
    "inp, expected",
    (
        ("snakeCase", "snake_case"),
        ("snake_case", "snake_case"),
        ("a", "a"),
    ),
)
def test_to_snake_case(inp, expected):
    assert expected == to_snake_case(inp)


@pytest.mark.parametrize(
    "inp_type, camel_case, inp, expected",
    (
        (list, True, {"foo": "a_B_c_D", "b_aR_e": 2}, ["foo", "bArE"]),
        (list, False, {"foo": "a_B_c_D", "b_aR_e": 2}, ["foo", "b_a_r_e"]),
        (set, True, iter(["a", "a_a", "aA"]), {"a", "aA"}),
        (set, False, ["a", "a_a", "aA"], {"a", "a_a"}),
        (tuple, True, ("a", "a_a", "aA"), ("a", "aA", "aA")),
        (tuple, False, (s for s in ("a", "a_a", "aA")), ("a", "a_a", "a_a")),
    ),
)
def test_iterable_to_case(inp_type, camel_case, inp, expected):
    assert inp_type(iterable_to_case(inp, camel_case)) == expected


def test_convert_all_keys_to_camel_case():
    inp = {"foo": 1, "f_o_o": 2, "fOo": 3}
    assert convert_all_keys_to_camel_case(inp) == {"foo": 1, "fOO": 2, "fOo": 3}


def test_convert_all_keys_to_snake_case():
    inp = {"foo": 1, "f_o_o": 2, "fOo": 3}
    assert convert_all_keys_to_snake_case(inp) == {"foo": 1, "f_o_o": 2, "f_oo": 3}
