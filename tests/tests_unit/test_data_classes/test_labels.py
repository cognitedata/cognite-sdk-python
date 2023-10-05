import pytest
from cognite.client.data_classes.labels import Label


class TestLabel:
    @pytest.mark.parametrize(
        ["fields", "is_camel_case", "expected"],
        [
            ({"external_id": "what-the-fox", "snake_rules": True}, True, {"externalId": "what-the-fox", "snakeRules": True}),
            ({"external_id": "what-the-fox", "snake_rules": True}, False, {"external_id": "what-the-fox", "snake_rules": True}),
            ({"externalId": "what-the-fox", "snakeRules": True}, False, {"external_id": "what-the-fox", "snake_rules": True}),
            ({"externalId": "what-the-fox", "snakeRules": True}, True, {"externalId": "what-the-fox", "snakeRules": True}),
        ]
    )
    def test_dump(self, fields: dict, is_camel_case: bool, expected: dict):
        assert Label(**fields).dump(is_camel_case) == expected
