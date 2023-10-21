from unittest import mock

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import (
    Asset,
    Sequence,
    SequenceColumnUpdate,
    SequenceFilter,
    SequenceList,
    SequenceUpdate,
    filters,
)
from cognite.client.data_classes.sequences import SequenceProperty, SortableSequenceProperty
from cognite.client.exceptions import CogniteNotFoundError
from tests.utils import set_request_limit


@pytest.fixture
def new_seq(cognite_client):
    column_def = [
        {"valueType": "STRING", "externalId": "user", "description": "some description"},
        {"valueType": "DOUBLE", "externalId": "amount"},
        {"valueType": "LONG", "externalId": "age"},
    ]
    seq = cognite_client.sequences.create(Sequence(name="test_temp", columns=column_def, metadata={"a": "b"}))
    yield seq
    cognite_client.sequences.delete(id=seq.id)
    assert cognite_client.sequences.retrieve(seq.id) is None


@pytest.fixture
def get_spy(cognite_client):
    with mock.patch.object(cognite_client.sequences, "_get", wraps=cognite_client.sequences._get) as _:
        yield


@pytest.fixture
def post_spy(cognite_client):
    with mock.patch.object(cognite_client.sequences, "_post", wraps=cognite_client.sequences._post) as _:
        yield


@pytest.fixture()
def root_asset(cognite_client: CogniteClient) -> Asset:
    root_asset = Asset(
        name="integration_test:root_asset",
        external_id="integration_test:root_asset",
    )
    if retrieved := cognite_client.assets.retrieve_multiple(
        external_ids=[root_asset.external_id], ignore_unknown_ids=True
    ):
        return retrieved[0]
    return cognite_client.assets.create(root_asset)


@pytest.fixture
def sequence_list(cognite_client: CogniteClient, root_asset: Asset) -> SequenceList:
    prefix = "integration_test:"
    columns = [
        {"valueType": "STRING", "externalId": "text"},
        {"valueType": "DOUBLE", "externalId": "value"},
    ]
    sequences = SequenceList(
        [
            Sequence(
                external_id=f"{prefix}sequence1",
                columns=columns,
                asset_id=root_asset.id,
                metadata={"unit": "m/s"},
            ),
            Sequence(
                external_id=f"{prefix}sequence2",
                columns=columns,
                metadata={"unit": "km/h"},
            ),
        ]
    )
    retrieved = cognite_client.sequences.retrieve_multiple(
        external_ids=sequences.as_external_ids(), ignore_unknown_ids=True
    )
    if len(retrieved) == len(sequences):
        return retrieved
    return cognite_client.sequences.upsert(sequences, mode="replace")


class TestSequencesAPI:
    def test_retrieve(self, cognite_client):
        listed_asset = cognite_client.sequences.list(limit=1)[0]
        retrieved_asset = cognite_client.sequences.retrieve(listed_asset.id)
        assert retrieved_asset == listed_asset

    def test_retrieve_multiple(self, cognite_client):
        res = cognite_client.sequences.list(limit=2)
        retrieved_assets = cognite_client.sequences.retrieve_multiple([s.id for s in res])
        for listed_asset, retrieved_asset in zip(res, retrieved_assets):
            retrieved_asset.external_id = listed_asset.external_id
        assert res == retrieved_assets

    @pytest.mark.parametrize("ignore_unknown_ids", [False, True])
    def test_retrieve_multiple_ignore_unknown_ids(self, cognite_client, ignore_unknown_ids):
        res = cognite_client.sequences.list(limit=2)
        invalid_id = 1
        try:
            retrieved_assets = cognite_client.sequences.retrieve_multiple(
                [s.id for s in res] + [invalid_id], ignore_unknown_ids=ignore_unknown_ids
            )
            failed = False
            assert {s.id for s in retrieved_assets} == {s.id for s in res}
        except CogniteNotFoundError:
            failed = True

        assert failed ^ ignore_unknown_ids

    def test_call(self, cognite_client, post_spy):
        with set_request_limit(cognite_client.sequences, 10):
            res = [s for s in cognite_client.sequences(limit=20)]

        assert 20 == len(res)
        assert 2 == cognite_client.sequences._post.call_count

    def test_list(self, cognite_client, post_spy):
        with set_request_limit(cognite_client.sequences, 10):
            res = cognite_client.sequences.list(limit=20)

        assert 20 == len(res)
        assert 2 == cognite_client.sequences._post.call_count

    def test_list_assetid_nothing(self, cognite_client):
        res = cognite_client.sequences.list(asset_ids=[12345678910], limit=20)
        assert 0 == len(res)

    def test_aggregate(self, cognite_client):
        res = cognite_client.sequences.aggregate(filter=SequenceFilter(name="42"))
        assert res[0].count > 0

    def test_search(self, cognite_client):
        res = cognite_client.sequences.search(name="42", filter=SequenceFilter(created_time={"min": 0}))
        assert len(res) > 0

    def test_update(self, cognite_client, new_seq):
        assert new_seq.metadata == {"a": "b"}
        update_seq = SequenceUpdate(new_seq.id).name.set("newname").metadata.set(None)
        res = cognite_client.sequences.update(update_seq)
        assert "newname" == res.name
        assert res.metadata == {}

    def test_update_full(self, cognite_client, new_seq):
        assert new_seq.metadata == {"a": "b"}
        new_seq.name = "newname"
        res = cognite_client.sequences.update(new_seq)
        assert "newname" == res.name

    def test_update_columns_add_remove_single(self, cognite_client, new_seq):
        assert len(new_seq.columns) == 3
        update_seq = SequenceUpdate(new_seq.id).columns.add(
            {"valueType": "STRING", "externalId": "user_added", "description": "some description"}
        )
        res = cognite_client.sequences.update(update_seq)
        assert len(res.columns) == 4
        assert res.column_external_ids[3] == "user_added"

    def test_update_columns_add_multiple(self, cognite_client, new_seq):
        assert len(new_seq.columns) == 3
        column_def = [
            {"valueType": "STRING", "externalId": "user_added", "description": "some description"},
            {"valueType": "DOUBLE", "externalId": "amount_added"},
        ]
        update_seq = SequenceUpdate(new_seq.id).columns.add(column_def)
        res = cognite_client.sequences.update(update_seq)
        assert len(res.columns) == 5
        assert res.column_external_ids[3:5] == ["user_added", "amount_added"]

    def test_update_columns_remove_single(self, cognite_client, new_seq):
        assert len(new_seq.columns) == 3
        update_seq = SequenceUpdate(new_seq.id).columns.remove(new_seq.columns[0]["externalId"])
        res = cognite_client.sequences.update(update_seq)
        assert len(res.columns) == 2
        assert res.columns[0:2] == new_seq.columns[1:3]

    def test_update_columns_remove_multiple(self, cognite_client, new_seq):
        assert len(new_seq.columns) == 3
        update_seq = SequenceUpdate(new_seq.id).columns.remove([col["externalId"] for col in new_seq.columns[0:2]])
        res = cognite_client.sequences.update(update_seq)
        assert len(res.columns) == 1
        assert res.columns[0] == new_seq.columns[2]

    def test_update_columns_modify(self, cognite_client, new_seq):
        assert new_seq.columns[1].get("description") is None
        column_update = [
            SequenceColumnUpdate(external_id=new_seq.columns[0]["externalId"]).external_id.set("new_col_external_id"),
            SequenceColumnUpdate(external_id=new_seq.columns[1]["externalId"]).description.set("my new description"),
        ]
        update_seq = SequenceUpdate(new_seq.id).columns.modify(column_update)
        res = cognite_client.sequences.update(update_seq)
        assert len(res.columns) == 3
        assert res.columns[0]["externalId"] == "new_col_external_id"
        assert res.columns[1]["description"] == "my new description"

    def test_get_new(self, cognite_client, new_seq):
        cognite_client.sequences.retrieve(id=new_seq.id)
        # assert ["DOUBLE"] == res.column_value_types # soon to change
        assert len(new_seq.columns) == 3

    def test_upsert_2_sequence_one_preexisting(self, cognite_client: CogniteClient) -> None:
        # Arrange
        new_sequence = Sequence(
            external_id="test_upsert_2_sequence_one_preexisting:new",
            name="my new sequence",
            columns=[{"externalId": "col1", "valueType": "STRING"}],
        )
        preexisting = Sequence(
            external_id="test_upsert_2_sequence_one_preexisting:preexisting",
            name="my preexisting sequence",
            columns=[{"externalId": "col1", "valueType": "STRING"}],
        )
        preexisting_update = Sequence.load(preexisting.dump(camel_case=True))
        preexisting_update.name = "my preexisting sequence updated"

        try:
            created_existing = cognite_client.sequences.create(preexisting)
            assert created_existing.id is not None

            # Act
            res = cognite_client.sequences.upsert([new_sequence, preexisting_update], mode="replace")

            # Assert
            assert len(res) == 2
            assert new_sequence.external_id == res[0].external_id
            assert preexisting.external_id == res[1].external_id
            assert new_sequence.name == res[0].name
            assert preexisting_update.name == res[1].name
        finally:
            cognite_client.sequences.delete(
                external_id=[new_sequence.external_id, preexisting.external_id], ignore_unknown_ids=True
            )

    def test_filter_equals(self, cognite_client: CogniteClient, sequence_list: SequenceList, root_asset: Asset) -> None:
        # Arrange
        f = filters
        is_integration_test = f.Prefix(SequenceProperty.external_id, "integration_test:")
        is_asset = f.Equals(SequenceProperty.asset_id, root_asset.id)

        # Act
        result = cognite_client.sequences.filter(
            f.And(is_integration_test, is_asset), sort=SortableSequenceProperty.created_time
        )

        # Assert
        assert len(result) == 1, "Expected only one sequence in subtree"
        assert result[0].external_id == sequence_list[0].external_id

    def test_filter_without_sort(
        self, cognite_client: CogniteClient, sequence_list: SequenceList, root_asset: Asset
    ) -> None:
        # Arrange
        f = filters
        is_integration_test = f.Prefix(SequenceProperty.external_id, "integration_test:")
        is_asset = f.Equals(SequenceProperty.asset_id, root_asset.id)

        # Act
        result = cognite_client.sequences.filter(f.And(is_integration_test, is_asset), sort=None)

        # Assert
        assert len(result) == 1
        assert result[0].external_id == sequence_list[0].external_id

    def test_aggregate_count(self, cognite_client: CogniteClient, sequence_list: SequenceList) -> None:
        f = filters
        is_integration_test = f.Prefix("externalId", "integration_test:")

        count = cognite_client.sequences.aggregate_count(advanced_filter=is_integration_test)

        assert count >= len(sequence_list)

    def test_aggregate_asset_id_count(self, cognite_client: CogniteClient, sequence_list: SequenceList) -> None:
        f = filters
        is_integration_test = f.Prefix("externalId", "integration_test:")

        count = cognite_client.sequences.aggregate_cardinality_values(
            SequenceProperty.asset_id, advanced_filter=is_integration_test
        )
        assert count >= len([s for s in sequence_list if s.asset_id is not None])

    def test_aggregate_metadata_keys_count(self, cognite_client: CogniteClient, sequence_list: SequenceList) -> None:
        f = filters
        is_integration_test = f.Prefix("externalId", "integration_test:")

        count = cognite_client.sequences.aggregate_cardinality_properties(
            SequenceProperty.metadata, advanced_filter=is_integration_test
        )

        assert count >= len({k for s in sequence_list for k in s.metadata.keys()})

    def test_aggregate_metadata_key_count(self, cognite_client: CogniteClient, sequence_list: SequenceList) -> None:
        f = filters
        is_integration_test = f.Prefix("externalId", "integration_test:")

        count = cognite_client.sequences.aggregate_cardinality_values(
            SequenceProperty.metadata_key("unit"), advanced_filter=is_integration_test
        )

        assert count >= len({s.metadata["unit"] for s in sequence_list if "unit" in s.metadata})

    def test_aggregate_unique_asset_ids(self, cognite_client: CogniteClient, sequence_list: SequenceList) -> None:
        f = filters
        is_integration_test = f.Prefix("externalId", "integration_test:")

        result = cognite_client.sequences.aggregate_unique_values(
            SequenceProperty.asset_id, advanced_filter=is_integration_test
        )

        assert result
        assert {int(item) for item in result.unique} >= {s.asset_id for s in sequence_list if s.asset_id is not None}

    def test_aggregate_unique_metadata_keys(self, cognite_client: CogniteClient, sequence_list: SequenceList) -> None:
        f = filters
        is_integration_test = f.Prefix("externalId", "integration_test:")

        result = cognite_client.sequences.aggregate_unique_properties(
            SequenceProperty.metadata, advanced_filter=is_integration_test
        )

        assert result
        assert {tuple(item.value["property"]) for item in result} >= {
            ("metadata", key.casefold()) for a in sequence_list for key in a.metadata or []
        }
