import pytest

from cognite.client.data_classes.labels import Label, LabelDefinition


class TestLabel:
    @pytest.mark.parametrize(
        ["fields", "is_camel_case", "expected"],
        [
            (
                {"external_id": "what-the-fox", "snake_rules": True},
                True,
                {"externalId": "what-the-fox", "snakeRules": True},
            ),
            (
                {"external_id": "what-the-fox", "snake_rules": True},
                False,
                {"external_id": "what-the-fox", "snake_rules": True},
            ),
            (
                {"externalId": "what-the-fox", "snakeRules": True},
                False,
                {"external_id": "what-the-fox", "snake_rules": True},
            ),
            (
                {"externalId": "what-the-fox", "snakeRules": True},
                True,
                {"externalId": "what-the-fox", "snakeRules": True},
            ),
            (
                {"externalId": "what-the-fox", "snake_rules": True},
                True,
                {"externalId": "what-the-fox", "snakeRules": True},
            ),
            (
                {"externalId": "what-the-fox", "snake_rules": True},
                False,
                {"external_id": "what-the-fox", "snake_rules": True},
            ),
        ],
    )
    def test_dump(self, fields: dict, is_camel_case: bool, expected: dict):
        assert Label(**fields).dump(is_camel_case) == expected

    def test_load_list(self):
        assert Label._load_list(None) is None
        labels = [{"externalId": "a"}, "b", Label("c"), LabelDefinition("d")]
        assert Label._load_list(labels) == [Label("a"), Label("b"), Label("c"), Label("d")]
