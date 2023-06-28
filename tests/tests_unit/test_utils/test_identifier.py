import pytest

from cognite.client._constants import MAX_VALID_INTERNAL_ID
from cognite.client.utils._identifier import Identifier, IdentifierSequence


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
        "id, external_id, err_msg",
        (
            (None, None, "Exactly one of id or external id must be specified, got neither"),
            (123, "foo", "Exactly one of id or external id must be specified, got both"),
            (0, None, f"Invalid id, must satisfy: 1 <= id <= {MAX_VALID_INTERNAL_ID}"),
            (MAX_VALID_INTERNAL_ID + 1, None, f"Invalid id, must satisfy: 1 <= id <= {MAX_VALID_INTERNAL_ID}"),
        ),
    )
    def test_of_either__bad_input(self, id, external_id, err_msg):
        with pytest.raises(ValueError, match=err_msg):
            Identifier.of_either(id, external_id)


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

    @pytest.mark.parametrize(
        "ids, external_ids, exception, match",
        [(None, None, ValueError, "No identifiers specified")],
    )
    def test_process_ids_fail(self, ids, external_ids, exception, match) -> None:
        with pytest.raises(exception, match=match):
            IdentifierSequence.load(ids, external_ids)

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
