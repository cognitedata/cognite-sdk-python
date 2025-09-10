import re

import pytest

from cognite.client.data_classes.filters import And, Filter, In, Or
from tests.utils import FakeCogniteResourceGenerator


def test_filters_raise_on_and_or_or() -> None:
    # We've seen multiple users are trying to combine filters using `and` and `or`, which does not work,
    # as the behavior can not be overloaded in Python (must use & and |).
    fcrg = FakeCogniteResourceGenerator()
    # Create two random filters:
    flt1 = fcrg.create_instance(Filter)
    flt2 = fcrg.create_instance(Filter)

    match_str = re.escape("You can not combine filters using 'and' or 'or', you must use the And filter (&)")
    with pytest.raises(RuntimeError, match=match_str):
        flt1 and flt2  # type: ignore[operator]
    with pytest.raises(RuntimeError, match=match_str):
        flt1 or flt2  # type: ignore[operator]

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
