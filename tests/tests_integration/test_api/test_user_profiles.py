import os

import pytest

from cognite.client.data_classes import UserProfile, UserProfileList
from cognite.client.exceptions import CogniteNotFoundError
from cognite.client.utils._text import random_string


@pytest.fixture(scope="module")
def profiles(cognite_client):
    if profiles := cognite_client.iam.user_profiles.list(limit=5):
        return profiles
    pytest.skip("Can't test user profiles without any user profiles available", allow_module_level=True)


def test_user_profiles_api__list(cognite_client, profiles):
    assert 1 <= len(profiles) <= 5
    assert isinstance(profiles, UserProfileList)
    assert isinstance(profiles[0], UserProfile)
    assert isinstance(profiles[0].user_identifier, str)


@pytest.mark.skipif(os.getenv("LOGIN_FLOW") != "interactive", reason="This test requires interactive auth")
def test_user_profiles_api__get_my_own_profile(cognite_client):
    profile = cognite_client.iam.user_profiles.me()
    assert isinstance(profile, UserProfile)
    # Only two required fields returned:
    assert isinstance(profile.user_identifier, str)
    assert isinstance(profile.last_updated_time, int)


def test_user_profiles_api__retrieve_single(cognite_client, profiles):
    profile = profiles[0]
    profile_retrieve = cognite_client.iam.user_profiles.retrieve(profile.user_identifier)
    assert profile_retrieve is not None
    assert isinstance(profile, UserProfile)
    assert profile == profile_retrieve


def test_user_profiles_api__retrieve_not_exist(cognite_client):
    assert cognite_client.iam.user_profiles.retrieve(random_string(10)) is None


def test_user_profiles_api__retrieve_many(cognite_client, profiles):
    profiles_retrieve = cognite_client.iam.user_profiles.retrieve([p.user_identifier for p in profiles])

    assert isinstance(profiles, UserProfileList)
    assert isinstance(profiles_retrieve, UserProfileList)
    assert profiles == profiles_retrieve


def test_user_profiles_api__retrieve_many_not_exist(cognite_client, profiles):
    user_idents = [profiles[0].user_identifier, user_id := random_string(10)]

    # Endpoint does not support 'ignore unknown ids'
    with pytest.raises(CogniteNotFoundError) as err:
        cognite_client.iam.user_profiles.retrieve(user_idents)
    assert err.value.not_found == [{"userIdentifier": user_id}]
