from __future__ import annotations

import os

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import CreatedSession, Group, GroupList, SecurityCategory
from cognite.client.data_classes.capabilities import EventsAcl, ProjectCapabilityList
from cognite.client.data_classes.iam import (
    ServiceAccount,
    ServiceAccountSecret,
    ServiceAccountSecretWrite,
    ServiceAccountUpdate,
    ServiceAccountWrite,
)
from cognite.client.data_classes.iam import SecurityCategoryWrite
from cognite.client.utils._text import random_string


@pytest.fixture(scope="session")
def group_list(cognite_client) -> GroupList:
    groups = cognite_client.iam.groups.list(all=True)
    assert len(groups) > 0
    return groups


class TestGroupsAPI:
    def test_dump_load_group_list(self, group_list: GroupList) -> None:
        loaded = GroupList._load(group_list.dump(camel_case=True), allow_unknown=True)
        assert group_list.dump() == loaded.dump()

    @pytest.mark.skip(
        reason="CogniteAPIError: There can only be 1500 undeleted or deleted groups per project | code: 400"
    )
    @pytest.mark.parametrize("source_id, members", (("abc-123", None), (None, ["user1", "user2"])))
    def test_create(self, cognite_client, source_id, members):
        metadata = {"haha": "blabla"}
        group: Group | None = None
        try:
            group = cognite_client.iam.groups.create(
                Group(
                    name="bla",
                    capabilities=[EventsAcl([EventsAcl.Action.Read], EventsAcl.Scope.All())],
                    metadata=metadata,
                    source_id=source_id,
                    members=members,
                )
            )
            assert "bla" == group.name
            assert metadata == group.metadata
            assert group.source_id == (source_id or "")
            if members is None:
                assert group.members is None
            else:
                assert sorted(group.members) == members
        finally:
            if group:
                cognite_client.iam.groups.delete(group.id)


class TestTokensAPI:
    def test_inspect(self, cognite_client: CogniteClient) -> None:
        result = cognite_client.iam.token.inspect()
        assert isinstance(result.capabilities, ProjectCapabilityList)


@pytest.fixture(scope="session")
def security_category(cognite_client: CogniteClient) -> SecurityCategory:
    result = cognite_client.iam.security_categories.list(limit=1)
    if result:
        return result[0]
    return cognite_client.iam.security_categories.create(SecurityCategoryWrite(name="integration_test"))


class TestSecurityCategoriesAPI:
    def test_list(self, cognite_client: CogniteClient, security_category: SecurityCategory):
        res = cognite_client.iam.security_categories.list()
        assert len(res) > 0

    def test_create_and_delete(self, cognite_client):
        random_name = "test_" + random_string(10)
        res = cognite_client.iam.security_categories.create(SecurityCategory(name=random_name))
        assert res.id in {s.id for s in cognite_client.iam.security_categories.list()}
        cognite_client.iam.security_categories.delete(res.id)
        assert res.id not in {s.id for s in cognite_client.iam.security_categories.list()}


@pytest.mark.skipif(
    os.getenv("LOGIN_FLOW") == "client_certificate", reason="Sessions do not work with client_certificate"
)
class TestSessionsAPI:
    def test_create_retrieve_and_revoke(self, cognite_client: CogniteClient) -> None:
        created: CreatedSession | None = None
        try:
            created = cognite_client.iam.sessions.create()

            retrieved = cognite_client.iam.sessions.retrieve(created.id)

            assert retrieved.id == created.id
            assert created.id in {s.id for s in cognite_client.iam.sessions.list("READY", limit=-1)}
        finally:
            if created:
                revoked = cognite_client.iam.sessions.revoke(created.id)
                assert created.id == revoked.id


class TestServiceAccountsAPI:
    def test_create_update_retrieve_delete(self, cognite_client: CogniteClient) -> None:
        org = "pytest_org"
        item = ServiceAccountWrite(
            name="test_" + random_string(10),
            external_id=f"test_{random_string(10)}",
            description="Original description",
        )

        created: ServiceAccount | None = None
        secret: ServiceAccountSecret | None = None
        try:
            created = cognite_client.iam.service_accounts.create(org, item)
            assert created.as_write().dump() == item.dump()

            update = ServiceAccountUpdate(id=created.id).description.set("Updated description")

            updated = cognite_client.iam.service_accounts.update(org, update)
            assert updated.description == "Updated description"

            retrieved = cognite_client.iam.service_accounts.retrieve(created.id)
            assert retrieved.as_write().dump() == updated.as_write().dump()

            secret = cognite_client.iam.service_accounts.secrets.create(
                org, created.id, ServiceAccountSecretWrite(expires_in_seconds=3600)
            )
            assert secret.id is not None

            listed = cognite_client.iam.service_accounts.secrets.list(org, created.id)
            assert len(listed) == 1
            assert listed[0].id == secret.id
        finally:
            if created:
                if secret:
                    cognite_client.iam.service_accounts.secrets.delete(org, created.id, secret.id)
                cognite_client.iam.service_accounts.delete(created.id)
