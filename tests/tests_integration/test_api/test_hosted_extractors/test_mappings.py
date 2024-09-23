from __future__ import annotations

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.hosted_extractors import (
    CustomMapping,
    Mapping,
    MappingList,
    MappingUpdate,
    MappingWrite,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._text import random_string


@pytest.fixture
def one_mapping(cognite_client: CogniteClient) -> Mapping:
    my_mapping = MappingWrite(
        external_id=f"myNewMapping-{random_string(10)}",
        mapping=CustomMapping("2 * 3"),
        published=False,
        input="json",
    )
    created = cognite_client.hosted_extractors.mappings.create(my_mapping)
    yield created

    cognite_client.hosted_extractors.mappings.delete(created.external_id, ignore_unknown_ids=True)


class TestMappings:
    def test_create_update_retrieve_delete(self, cognite_client: CogniteClient) -> None:
        my_mapping = MappingWrite(
            external_id=f"myNewMappingForTesting-{random_string(10)}",
            mapping=CustomMapping("2 * 3"),
            published=False,
            input="xml",
        )
        created: Mapping | None = None
        try:
            created = cognite_client.hosted_extractors.mappings.create(my_mapping)
            assert isinstance(created, Mapping)
            update = MappingUpdate(external_id=my_mapping.external_id).published.set(True)
            updated = cognite_client.hosted_extractors.mappings.update(update)
            assert updated.published is True
            retrieved = cognite_client.hosted_extractors.mappings.retrieve(created.external_id)
            assert retrieved is not None
            assert retrieved.external_id == my_mapping.external_id
            assert retrieved.published == updated.published

            cognite_client.hosted_extractors.mappings.delete(created.external_id)

            with pytest.raises(CogniteAPIError):
                cognite_client.hosted_extractors.mappings.retrieve(created.external_id)

            cognite_client.hosted_extractors.mappings.retrieve(created.external_id, ignore_unknown_ids=True)

        finally:
            if created:
                cognite_client.hosted_extractors.mappings.delete(created.external_id, ignore_unknown_ids=True)

    @pytest.mark.usefixtures("one_mapping")
    def test_list(self, cognite_client: CogniteClient) -> None:
        res = cognite_client.hosted_extractors.mappings.list(limit=1)
        assert len(res) == 1
        assert isinstance(res, MappingList)

    def test_update_using_write_object(self, cognite_client: CogniteClient) -> None:
        my_mapping = MappingWrite(
            external_id=f"myMappingForUpdate-{random_string(10)}",
            mapping=CustomMapping("2 * 3"),
            published=False,
            input="xml",
        )
        created: Mapping | None = None
        try:
            created = cognite_client.hosted_extractors.mappings.create(my_mapping)

            update = MappingWrite(
                external_id=my_mapping.external_id,
                mapping=CustomMapping("2 * 3"),
                published=True,
                input="json",
            )

            updated = cognite_client.hosted_extractors.mappings.update(update)
            assert updated.published is True
        finally:
            if created:
                cognite_client.hosted_extractors.mappings.delete(created.external_id, ignore_unknown_ids=True)
