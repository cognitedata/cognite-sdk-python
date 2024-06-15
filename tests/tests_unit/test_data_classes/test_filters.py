import pytest

from cognite.client.data_classes.filters import Filter, In


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
