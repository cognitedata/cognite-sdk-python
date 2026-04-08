from __future__ import annotations

import re

import pytest

from cognite.client.data_classes.filters import And, Equals, Filter, In, Or
from tests.utils import FakeCogniteResourceGenerator


@pytest.mark.dsl  # requirement when using FakeCogniteResourceGenerator
def test_filters_warn_in_boolean_contexts() -> None:
    # We've seen multiple users using filters in boolean contexts (like 'if my_filter:' or 'flt1 and flt2'),
    # which is not recommended. The proper way to combine filters is using & and | operators.
    fcrg = FakeCogniteResourceGenerator()
    # Create two random filters:
    flt1 = fcrg.create_instance(Filter)  # type: ignore[type-abstract]
    flt2 = fcrg.create_instance(Filter)  # type: ignore[type-abstract]

    # Test that warnings are issued when using filters in boolean contexts
    match_str = "^" + re.escape("You may be trying to combine two (or more) filters using 'and' or 'or'")
    with pytest.warns(UserWarning, match=match_str):
        flt1 and flt2

    with pytest.warns(UserWarning, match=match_str):
        flt1 or flt2

    # Test that the proper filter combination operators still work correctly
    assert type(flt1 & flt2) is And
    assert type(flt1 | flt2) is Or


@pytest.mark.parametrize(
    "user_filter, expected",
    [
        (
            In(property=["do_not_convert_me", "do_not_convert_me"], values=[1, 2, 3]),
            {"in": {"property": ["do_not_convert_me", "do_not_convert_me"], "values": [1, 2, 3]}},
        ),
        (
            In(property=["do_not_convert_me", "do_not_convert_me/v1", "do_not_convert_me"], values=[1, 2, 3]),
            {
                "in": {
                    "property": ["do_not_convert_me", "do_not_convert_me/v1", "do_not_convert_me"],
                    "values": [1, 2, 3],
                }
            },
        ),
        # Only single-element property references should be converted to camelCase
        (In(property=["convert_me"], values=[1, 2, 3]), {"in": {"property": ["convertMe"], "values": [1, 2, 3]}}),
    ],
)
def test_filter_property_case_conversion(user_filter: Filter, expected: dict) -> None:
    assert user_filter.dump(camel_case_property=True) == expected


def test_filter_is_hashable_and_uses_identity() -> None:
    # Bug in 8.0.0 to 8.0.5: __eq__ was added to Filter thus implicitly setting __hash__ = None,
    # making filters unhashable.
    flt = Equals(property=["node", "type"], value="pump")
    hash(flt)  # must not raise

    # Hashing is identity-based: two filters with equal content hash differently.
    flt2 = Equals(property=["node", "type"], value="pump")
    assert hash(flt) != hash(flt2)
    assert flt != flt2
