import re

import pytest

from cognite.client.utils._validation import (
    assert_type,
    process_asset_subtree_ids,
    process_data_set_ids,
    validate_user_input_dict_with_identifier,
)


class TestAssertions:
    @pytest.mark.parametrize("var, var_name, types", [(1, "var1", [int]), ("1", "var2", [int, str])])
    def test_assert_type_ok(self, var, var_name, types):
        assert_type(var, var_name, types=types)

    @pytest.mark.parametrize("var, var_name, types", [("1", "var", [int, float]), ((1,), "var2", [dict, list])])
    def test_assert_type_fail(self, var, var_name, types):
        with pytest.raises(TypeError, match=str(types)):
            assert_type(var, var_name, types)


class TestValidateUserInputDictWithIdentifier:
    @pytest.mark.parametrize(
        "dct, keys, expected",
        (
            ({"id": 123, "a": None, "b": 0}, {"a", "b"}, {"id": 123}),
            ({"external_id": "foo", "c": None}, {"c"}, {"externalId": "foo"}),
            ({"externalId": "foo"}, set(), {"externalId": "foo"}),
        ),
    )
    def test_validate_normal_input(self, dct, keys, expected):
        assert expected == validate_user_input_dict_with_identifier(dct, required_keys=keys)

    @pytest.mark.parametrize(
        "dct, keys, err, err_msg",
        (
            (["id", 123], set(), TypeError, "Expected dict-like object, got <class 'list'>"),
            ({}, set(), ValueError, "must be specified, got neither"),
            ({"id": 123, "external_id": "foo"}, set(), ValueError, "must be specified, got both"),
            ({"id": 123, "externalId": "foo"}, set(), ValueError, "must be specified, got both"),
            ({"id": 123}, {"a"}, ValueError, re.escape("Invalid key(s): [], required key(s) missing: ['a'].")),
            (
                {"id": 123, "a": 0},
                {"b"},
                ValueError,
                re.escape("Invalid key(s): ['a'], required key(s) missing: ['b']."),
            ),
            (
                {"id": 123, "a": 0, "b": 1},
                {"c", "d"},
                ValueError,
                re.escape("Invalid key(s): ['a', 'b'], required key(s) missing: ['c', 'd']."),
            ),
            ({"id": 123, "a": 0}, set(), ValueError, re.escape("Invalid key(s): ['a'], required key(s) missing: [].")),
        ),
    )
    def test_validate_bad_input(self, dct, keys, err, err_msg):
        with pytest.raises(err, match=err_msg):
            validate_user_input_dict_with_identifier(dct, required_keys=keys)


process_fns = process_data_set_ids, process_asset_subtree_ids
process_fns_names = "data_set", "asset_subtree"


class TestProcessIdentifiers:
    @pytest.mark.parametrize("fn, name", zip(process_fns, process_fns_names))
    def test_all_process_fns_bad_input(self, fn, name):
        exp_match = rf"^{name}_ids must be of type int or Sequence\[int\]\. Found <class 'str'>$"
        with pytest.raises(TypeError, match=exp_match):
            fn("97", "a")

        exp_match = rf"^{name}_external_ids must be of type str or Sequence\[str\]\. Found <class 'int'>$"
        with pytest.raises(TypeError, match=exp_match):
            fn(97, ord("a"))

    @pytest.mark.parametrize("fn", process_fns)
    def test_all_process_fns_normal_input(self, fn):
        assert fn(None, None) is None
        # As singletons:
        assert fn(1, None) == [{"id": 1}]
        assert fn(None, "a") == [{"externalId": "a"}]
        assert fn(1, "a") == [{"id": 1}, {"externalId": "a"}]
        # As lists:
        assert fn([1], None) == [{"id": 1}]
        assert fn(None, ["a"]) == [{"externalId": "a"}]
        assert fn([1], ["a"]) == [{"id": 1}, {"externalId": "a"}]
        assert fn([1, 2], ["a", "b"]) == [{"id": 1}, {"id": 2}, {"externalId": "a"}, {"externalId": "b"}]
