import os

import pytest

from cognite.client.data_classes import UserProfile, UserProfileList
from cognite.client.exceptions import CogniteNotFoundError
from cognite.client.utils._text import random_string


@pytest.mark.skipif(os.getenv("LOGIN_FLOW") != "interactive", reason="This test requires interactive auth")
def test_user_profiles_api__get_my_own_profile(cognite_client):
    profile = cognite_client.iam.user_profiles.me()
    assert isinstance(profile, UserProfile)
    # Only two required fields returned:
    assert isinstance(profile.user_identifier, str)
    assert isinstance(profile.last_updated_time, int)


def test_user_profiles_api__list(cognite_client):
    profiles = cognite_client.iam.user_profiles.list(limit=5)
    assert 1 <= len(profiles) <= 5
    assert isinstance(profiles, UserProfileList)
    assert isinstance(profiles[0], UserProfile)
    assert isinstance(profiles[0].user_identifier, str)


def test_user_profiles_api__retrieve(cognite_client):
    profile = cognite_client.iam.user_profiles.list(limit=1)[0]
    assert isinstance(profile.user_identifier, str)

    profile_retrieve = cognite_client.iam.user_profiles.retrieve(profile.user_identifier)
    assert profile_retrieve is not None
    assert profile == profile_retrieve


def test_user_profiles_api__retrieve_not_exist(cognite_client):
    assert cognite_client.iam.user_profiles.retrieve(random_string(10)) is None


def test_user_profiles_api__retrieve_multiple(cognite_client):
    profiles = cognite_client.iam.user_profiles.list(limit=5)
    profiles_retrieve = cognite_client.iam.user_profiles.retrieve_multiple([p.user_identifier for p in profiles])

    assert isinstance(profiles, UserProfileList)
    assert isinstance(profiles_retrieve, UserProfileList)
    assert profiles == profiles_retrieve


def test_user_profiles_api__retrieve_multiple_not_exist(cognite_client):
    # Endpoint does not support 'ignore unknown ids'
    with pytest.raises(CogniteNotFoundError) as err:
        cognite_client.iam.user_profiles.retrieve_multiple([user_id := random_string(10)])
    assert err.value.not_found == [{"userIdentifier": user_id}]
