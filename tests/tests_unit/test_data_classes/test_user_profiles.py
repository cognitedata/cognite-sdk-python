from __future__ import annotations

import pytest

from cognite.client.data_classes.user_profiles import UserProfile


@pytest.fixture
def minimal_user_profile_dct():
    return {
        "userIdentifier": "my-user",
        "lastUpdatedTime": 123,
    }


class TestUserProfile:
    def test_load_minimal(self, minimal_user_profile_dct):
        # Act
        profile = UserProfile._load(minimal_user_profile_dct)

        # Assert
        assert profile.user_identifier == "my-user"
        assert profile.last_updated_time == 123
        assert profile.given_name is None
        assert profile.surname is None
        assert profile.email is None
        assert profile.display_name is None
        assert profile.job_title is None
