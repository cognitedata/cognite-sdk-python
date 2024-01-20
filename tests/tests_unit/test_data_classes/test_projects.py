from __future__ import annotations

from typing import Any, Iterable

import pytest
from _pytest.mark import ParameterSet

from cognite.client.data_classes import Claim, ProjectUpdate, UserProfilesConfiguration


def project_update_dump_test_cases() -> Iterable[ParameterSet]:
    update = ProjectUpdate("my_project").name.set("new_name")
    yield pytest.param(
        update,
        {"update": {"name": {"set": "new_name"}}},
        id="Set name",
    )
    update = ProjectUpdate("my_project").user_profiles_configuration.modify.enabled.set(False).name.set("new_name")

    yield pytest.param(
        update,
        {"update": {"userProfilesConfiguration": {"modify": {"enabled": False}}, "name": {"set": "new_name"}}},
        id="Modify user profiles configuration",
    )
    update = ProjectUpdate("my_project").user_profiles_configuration.set(UserProfilesConfiguration(enabled=True))

    yield pytest.param(
        update,
        {"update": {"userProfilesConfiguration": {"set": {"enabled": True}}}},
        id="Set user profiles configuration",
    )

    update = ProjectUpdate("my_project").oidc_configuration.set(None).name.set("new_name")
    yield pytest.param(
        update,
        {"update": {"oidcConfiguration": {"setNull": True}, "name": {"set": "new_name"}}},
        id="Set oidc configuration and name",
    )

    update = (
        ProjectUpdate("my_project")
        .oidc_configuration.modify.jwks_url.set("new_url")
        .oidc_configuration.modify.skew_ms.set(None)
    )

    yield pytest.param(
        update,
        {"update": {"oidcConfiguration": {"modify": {"jwksUrl": {"set": "new_url"}, "skewMs": {"setNull": True}}}}},
        id="Modify oidc configuration",
    )

    update = (
        ProjectUpdate("my_project").oidc_configuration.modify.access_claims.add(Claim("new_claim")).name.set("new_name")
    )

    yield pytest.param(
        update,
        {
            "update": {
                "oidcConfiguration": {"modify": {"accessClaims": {"add": [{"claimName": "new_claim"}]}}},
                "name": {"set": "new_name"},
            }
        },
        id="Modify oidc configuration",
    )


class TestProjectUpdate:
    @pytest.mark.parametrize("project_update, expected_dump", list(project_update_dump_test_cases()))
    def test_dump(self, project_update: ProjectUpdate, expected_dump: dict[str, Any]) -> None:
        assert project_update.dump() == expected_dump
