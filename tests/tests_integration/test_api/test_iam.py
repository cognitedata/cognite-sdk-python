import os

import pytest

from cognite.client.data_classes import Group, SecurityCategory
from cognite.client.utils._text import random_string


class TestGroupsAPI:
    def test_list(self, cognite_client):
        res = cognite_client.iam.groups.list(all=True)
        assert len(res) > 0

    def test_create(self, cognite_client):
        group = cognite_client.iam.groups.create(
            Group(name="bla", capabilities=[{"eventsAcl": {"actions": ["READ"], "scope": {"all": {}}}}])
        )
        assert "bla" == group.name
        cognite_client.iam.groups.delete(group.id)


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


class TestSessionsAPI:
    @pytest.mark.skipif(
        os.getenv("LOGIN_FLOW") == "client_certificate", reason="Sessions do not work with client_certificate"
    )
    def test_create_and_revoke(self, cognite_client):
        res = cognite_client.iam.sessions.create()
        assert res.id in {s.id for s in cognite_client.iam.sessions.list("READY")}
        assert res.id in {s.id for s in cognite_client.iam.sessions.revoke(res.id) if s.status == "REVOKED"}
