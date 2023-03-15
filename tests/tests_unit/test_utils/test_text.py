from string import ascii_lowercase

import pytest

from cognite.client.utils._auxiliary import random_string, shorten, to_camel_case, to_snake_case
from tests.utils import rng_context


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
