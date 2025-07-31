from hypothesis import given
from hypothesis import strategies as st

from cognite.client.exceptions import _SENSITIVE_FIELDS, CogniteAPIError


@st.composite
def nested_structure(draw):
    simple_values = st.one_of(
        st.text(min_size=5, max_size=30, alphabet=st.characters()),
        st.integers(min_value=-1000, max_value=1000),
        st.booleans(),
        st.none(),
    )

    key_strategy = st.one_of(
        st.sampled_from(list(_SENSITIVE_FIELDS)).flatmap(
            lambda field: st.sampled_from([field.lower(), field.upper(), field.title(), field])
        ),
        st.text(min_size=3, max_size=15, alphabet=st.characters()).filter(lambda x: x.lower() not in _SENSITIVE_FIELDS),
    )

    return draw(
        st.recursive(
            simple_values,
            lambda children: st.one_of(
                st.dictionaries(keys=key_strategy, values=children, max_size=5), st.lists(children, max_size=5)
            ),
            max_leaves=20,
        )
    )


class TestAPIErrorMasking:
    @staticmethod
    def create_error(failed_items=None, successful=None, unknown=None, skipped=None):
        return CogniteAPIError(
            message="Test error", code=400, failed=failed_items, successful=successful, unknown=unknown, skipped=skipped
        )

    def validate_masking(self, original, masked):
        if isinstance(original, dict):
            assert isinstance(masked, dict) and set(original.keys()) == set(masked.keys())

            for key, value in original.items():
                if isinstance(value, (dict, list)) and key.lower() not in _SENSITIVE_FIELDS:
                    self.validate_masking(value, masked[key])
                else:
                    expected = "***" if key.lower() in _SENSITIVE_FIELDS else value
                    assert masked[key] == expected

        elif isinstance(original, list):
            assert isinstance(masked, list) and len(original) == len(masked)
            for i, (orig, msk) in enumerate(zip(original, masked)):
                self.validate_masking(orig, msk)
        else:
            assert original == masked

    @given(nested_structure())
    def test_comprehensive_masking(self, complex_data):
        error = self.create_error([complex_data])
        masked_data = error._mask_sensitive_values(complex_data)
        self.validate_masking(complex_data, masked_data)
