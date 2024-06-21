import pytest

from cognite.client._constants import MAX_VALID_INTERNAL_ID
from cognite.client.utils._identifier import Identifier, IdentifierSequence, UserIdentifier, UserIdentifierSequence


class TestIdentifier:
    @pytest.mark.parametrize(
        "id, external_id, exp_tpl",
        (
            (1, None, ("id", 1)),
            (MAX_VALID_INTERNAL_ID, None, ("id", MAX_VALID_INTERNAL_ID)),
            (None, "foo", ("external_id", "foo")),
        ),
    )
    def test_of_either__normal_input(self, id, external_id, exp_tpl):
        assert exp_tpl == Identifier.of_either(id, external_id).as_tuple(camel_case=False)

    @pytest.mark.parametrize(
        "id, external_id, instance_id, err_msg",
        (
            (None, None, None, "Exactly one of id, external id, or instance_id must be specified, got neither"),
            (123, "foo", None, "Exactly one of id, external id, or instance_id must be specified, got multiple"),
            (0, None, None, f"Invalid id, must satisfy: 1 <= id <= {MAX_VALID_INTERNAL_ID}"),
            (MAX_VALID_INTERNAL_ID + 1, None, None, f"Invalid id, must satisfy: 1 <= id <= {MAX_VALID_INTERNAL_ID}"),
        ),
    )
    def test_of_either__bad_input(self, id, external_id, instance_id, err_msg):
        with pytest.raises(ValueError, match=err_msg):
            Identifier.of_either(id, external_id, instance_id)

    def test_handles_id_type_correctly(self):
        # int is ok
        Identifier(1)

        # string is ok
        Identifier("abc")

        # anything else is not ok
        with pytest.raises(TypeError, match="Expected id/external_id to be of type int or str"):
            Identifier(object())


class TestIdentifierSequence:
    @pytest.mark.parametrize(
        "ids, external_ids, wrap_ids, expected",
        [
            (1, None, False, [1]),
            ([1, 2], None, False, [1, 2]),
            ((1, 2), None, False, [1, 2]),
            (1, None, True, [{"id": 1}]),
            ([1, 2], None, True, [{"id": 1}, {"id": 2}]),
            (1, "1", True, [{"id": 1}, {"externalId": "1"}]),
            (1, ["1"], True, [{"id": 1}, {"externalId": "1"}]),
            ([1, 2], ["1"], True, [{"id": 1}, {"id": 2}, {"externalId": "1"}]),
            (None, "1", True, [{"externalId": "1"}]),
            (None, ["1", "2"], True, [{"externalId": "1"}, {"externalId": "2"}]),
            (None, ("1", "2"), True, [{"externalId": "1"}, {"externalId": "2"}]),
        ],
    )
    def test_load_and_dump(self, ids, external_ids, wrap_ids, expected) -> None:
        identifiers = IdentifierSequence.load(ids, external_ids)
        if wrap_ids:
            assert identifiers.as_dicts() == expected
        else:
            assert identifiers.as_primitives() == expected

    def test_process_ids_empty(self) -> None:
        sequence = IdentifierSequence.load(None, None)

        assert len(sequence) == 0

    @pytest.mark.parametrize(
        "id, external_id, expected",
        [(1, None, True), (None, "1", True), ([1], None, False), (None, ["1"], False)],
    )
    def test_is_singleton(self, id, external_id, expected):
        identifiers = IdentifierSequence.load(id, external_id)
        assert identifiers.is_singleton() == expected

    def test_assert_singleton(self) -> None:
        with pytest.raises(ValueError):
            IdentifierSequence.load(1, "1").assert_singleton()
        IdentifierSequence.load(1, None).assert_singleton()
        IdentifierSequence.load(None, "1").assert_singleton()

    @pytest.mark.parametrize("external_ids, is_singleton", [("bla", True), (["bla"], False)])
    def test_disambiguate_str_and_seq_str(self, external_ids, is_singleton) -> None:
        seq = IdentifierSequence.load(ids=None, external_ids=external_ids)
        assert seq.is_singleton() is is_singleton

        seq = IdentifierSequence.of(external_ids)
        assert seq.is_singleton() is is_singleton


class TestUserIdentifier:
    def test_methods(self):
        user_id = UserIdentifier("foo")
        assert user_id.as_primitive() == "foo"
        assert user_id.as_dict(camel_case=True) == {"userIdentifier": "foo"}
        assert user_id.as_dict(camel_case=False) == {"user_identifier": "foo"}


class TestUserIdentifierSequence:
    @pytest.mark.parametrize(
        "user_ids, exp_dcts, exp_primitives",
        (
            ("foo", [{"userIdentifier": "foo"}], ["foo"]),
            (["foo", "bar"], [{"userIdentifier": "foo"}, {"userIdentifier": "bar"}], ["foo", "bar"]),
        ),
    )
    def test_load_and_dump(self, user_ids, exp_dcts, exp_primitives):
        user_id_seq = UserIdentifierSequence.load(user_ids)
        assert user_id_seq.as_primitives() == exp_primitives
        assert user_id_seq.as_dicts() == exp_dcts

    def test_load_wrong_type(self):
        with pytest.raises(TypeError):
            UserIdentifierSequence.load(123)
