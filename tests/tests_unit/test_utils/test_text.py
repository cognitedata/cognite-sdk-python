from string import ascii_lowercase

import pytest

from cognite.client.utils._auxiliary import shorten


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
