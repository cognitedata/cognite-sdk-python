from __future__ import annotations

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import CreatedSession, DataSet
from cognite.client.data_classes.hosted_extractors import (
    Destination,
    DestinationList,
    DestinationUpdate,
    DestinationWrite,
    SessionWrite,
)
from cognite.client.utils._text import random_string
from tests.tests_integration.test_api.test_hosted_extractors.conftest import (
    DESTINATION_FOR_TESTING_PREFIX,
    UPDATE_DESTINATION_PREFIX,
)


class TestDestinations:
    def test_create_update_retrieve_delete(
        self, cognite_client: CogniteClient, fresh_session: SessionWrite, a_data_set: DataSet
    ) -> None:
        my_dest = DestinationWrite(
            external_id=f"{DESTINATION_FOR_TESTING_PREFIX}{random_string(10)}",
            credentials=fresh_session,
        )
        created: Destination | None = None
        try:
            created = cognite_client.hosted_extractors.destinations.create(my_dest)
            assert isinstance(created, Destination)
            update = DestinationUpdate(external_id=my_dest.external_id).target_data_set_id.set(a_data_set.id)
            updated = cognite_client.hosted_extractors.destinations.update(update)
            assert updated.target_data_set_id == a_data_set.id
            retrieved = cognite_client.hosted_extractors.destinations.retrieve(created.external_id)
            assert retrieved is not None
            assert retrieved.external_id == created.external_id
            assert retrieved.target_data_set_id == a_data_set.id

            cognite_client.hosted_extractors.destinations.delete(created.external_id)

            assert cognite_client.hosted_extractors.destinations.retrieve(created.external_id) is None
            assert (
                cognite_client.hosted_extractors.destinations.retrieve(created.external_id, ignore_unknown_ids=True)
                is None
            )

        finally:
            if created:
                cognite_client.hosted_extractors.destinations.delete(created.external_id, ignore_unknown_ids=True)

    @pytest.mark.usefixtures("one_destination")
    def test_list(self, cognite_client: CogniteClient) -> None:
        res = cognite_client.hosted_extractors.destinations.list(limit=1)
        assert len(res) == 1
        assert isinstance(res, DestinationList)

    def test_update_using_write_object(
        self, cognite_client: CogniteClient, fresh_session: SessionWrite, a_data_set: DataSet
    ) -> None:
        my_dest = DestinationWrite(
            external_id=f"{UPDATE_DESTINATION_PREFIX}{random_string(10)}",
            credentials=fresh_session,
        )

        created: Destination | None = None
        new_session: CreatedSession | None = None
        try:
            new_session = cognite_client.iam.sessions.create(session_type="ONESHOT_TOKEN_EXCHANGE")
            created = cognite_client.hosted_extractors.destinations.create(my_dest)

            update = DestinationWrite(
                external_id=my_dest.external_id,
                credentials=SessionWrite(nonce=new_session.nonce),
                target_data_set_id=a_data_set.id,
            )

            updated = cognite_client.hosted_extractors.destinations.update(update)

            assert updated.target_data_set_id == a_data_set.id
        finally:
            if created:
                cognite_client.hosted_extractors.destinations.delete(created.external_id, ignore_unknown_ids=True)
            if new_session:
                cognite_client.iam.sessions.revoke(new_session.id)
