from __future__ import annotations

import os

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import CreatedSession, Group, GroupList, SecurityCategory
from cognite.client.data_classes.capabilities import EventsAcl, ProjectCapabilityList
from cognite.client.utils._text import random_string


@pytest.fixture(scope="session")
def group_list(cognite_client) -> GroupList:
    groups = cognite_client.iam.groups.list(all=True)
    assert len(groups) > 0
    return groups


class TestGroupsAPI:
    def test_dump_load_group_list(self, group_list: GroupList) -> None:
        loaded = GroupList.load(group_list.dump(camel_case=True))
        assert group_list.dump() == loaded.dump()

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


class TestSecurityCategoriesAPI:
    def test_list(self, cognite_client):
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
