import pytest

from cognite.client.exceptions import CogniteAPIError


class TestAPIErrorMasking:
    @pytest.fixture
    def error_with_data(self):
        def _create_error(failed_items):
            return CogniteAPIError(message="Test error", code=400, failed=failed_items)

        return _create_error

    def test_sensitive_fields_masked(self, error_with_data):
        failed_items = [
            {
                "nonce": "SECRET_NONCE",
                "secret": "SECRET_VALUE",
                "token": "SECRET_TOKEN",
                "clientSecret": "SECRET_CLIENT",
            }
        ]

        error_str = str(error_with_data(failed_items))

        assert "SECRET_NONCE" not in error_str
        assert "SECRET_VALUE" not in error_str
        assert "SECRET_TOKEN" not in error_str
        assert "SECRET_CLIENT" not in error_str

        assert "'nonce': '***'" in error_str
        assert "'secret': '***'" in error_str
        assert "'token': '***'" in error_str
        assert "'clientSecret': '***'" in error_str

    def test_nested_masking(self, error_with_data):
        failed_items = [{"user": {"auth": {"nonce": "NESTED_SECRET", "password": "NESTED_PASSWORD"}}}]

        error_str = str(error_with_data(failed_items))

        assert "NESTED_SECRET" not in error_str
        assert "NESTED_PASSWORD" in error_str
        assert "'nonce': '***'" in error_str

    def test_mixed_data_types(self, error_with_data):
        failed_items = ["simple_string", 123, {"nonce": "SECRET"}, None]

        error_str = str(error_with_data(failed_items))

        assert "simple_string" in error_str
        assert "123" in error_str
        assert "None" in error_str

        assert "SECRET" not in error_str
        assert "'nonce': '***'" in error_str

    def test_truncation_with_masking(self, error_with_data):
        failed_items = [{"nonce": f"SECRET_{i}"} for i in range(3)]

        error_str = str(error_with_data(failed_items))

        assert "..." in error_str
        assert all(f"SECRET_{i}" not in error_str for i in range(3))
