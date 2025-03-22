import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.postgres_gateway import (
    UserList,
)


class TestUsers:
    def test_list(self, cognite_client: CogniteClient) -> None:
        res = cognite_client.postgres_gateway.users.list(limit=1)
        assert isinstance(res, UserList)
        for user in res:
            cognite_client.postgres_gateway.users.delete(user.username, ignore_unknown_ids=True)
