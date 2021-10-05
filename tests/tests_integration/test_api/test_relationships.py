import uuid

import pytest

from cognite.client.data_classes import (
    Asset,
    Label,
    LabelDefinition,
    LabelFilter,
    Relationship,
    RelationshipList,
    RelationshipUpdate,
    TimeSeries,
)
from cognite.client.exceptions import CogniteNotFoundError


@pytest.fixture
def new_relationship(new_label, cognite_client):
    external_id = uuid.uuid4().hex[0:20]

    pre_existing_data_set = cognite_client.data_sets.retrieve(external_id="pre_existing_data_set")
    relationship = cognite_client.relationships.create(
        Relationship(
            external_id=external_id,
            source_external_id=external_id,
            source_type="asset",
            target_external_id=external_id,
            data_set_id=pre_existing_data_set.id,
            target_type="event",
            labels=[Label(new_label.external_id)],
            confidence=1,
        )
    )
    yield relationship, external_id
    cognite_client.relationships.delete(external_id=external_id)
    assert cognite_client.relationships.retrieve(external_id=relationship.external_id) is None


@pytest.fixture
def new_label(cognite_client):
    # create a label to use in relationships
    external_id = uuid.uuid4().hex[0:20]
    tp = cognite_client.labels.create(LabelDefinition(external_id=external_id, name="mandatory"))
    assert isinstance(tp, LabelDefinition)
    yield tp
    cognite_client.labels.delete(external_id=tp.external_id)


@pytest.fixture
def new_asset(cognite_client):
    # create an asset to use in relationships
    external_id = uuid.uuid4().hex[0:20]
    tp = cognite_client.assets.create(Asset(external_id=external_id, name="mandatory"))
    assert isinstance(tp, Asset)
    yield tp
    cognite_client.assets.delete(external_id=tp.external_id)


@pytest.fixture
def new_time_series(cognite_client):
    # create a time series to use in relationships
    external_id = uuid.uuid4().hex[0:20]
    tp = cognite_client.time_series.create(TimeSeries(external_id=external_id, name="mandatory"))
    assert isinstance(tp, TimeSeries)
    yield tp
    cognite_client.time_series.delete(external_id=tp.external_id)


@pytest.fixture
def create_multiple_relationships(new_label, cognite_client):
    ext_id = new_label.external_id
    relationships_ext_id = [uuid.uuid4().hex[0:20] for i in range(5)]
    random_ext_id = [uuid.uuid4().hex[0:20] for i in range(5)]
    relationship_list = [
        Relationship(
            external_id=relationships_ext_id[0],
            source_type="asset",
            source_external_id=random_ext_id[0],
            target_type="event",
            target_external_id=random_ext_id[1],
            labels=[Label(ext_id)],
        ),
        Relationship(
            external_id=relationships_ext_id[1],
            source_type="timeSeries",
            source_external_id=random_ext_id[3],
            target_type="asset",
            target_external_id=random_ext_id[2],
            confidence=1,
        ),
        Relationship(
            external_id=relationships_ext_id[2],
            source_type="event",
            source_external_id=random_ext_id[2],
            target_type="timeseries",
            target_external_id=random_ext_id[3],
            labels=[Label(ext_id)],
        ),
        Relationship(
            external_id=relationships_ext_id[3],
            source_type="sequence",
            source_external_id=random_ext_id[3],
            target_type="sequence",
            target_external_id=random_ext_id[4],
            labels=[Label(ext_id)],
        ),
        Relationship(
            external_id=relationships_ext_id[4],
            source_type="file",
            source_external_id=random_ext_id[4],
            target_type="file",
            target_external_id=random_ext_id[0],
            confidence=1,
        ),
    ]
    relationships = cognite_client.relationships.create(relationship_list)
    assert isinstance(relationships, RelationshipList)
    yield relationships_ext_id, ext_id, random_ext_id
    cognite_client.relationships.delete(external_id=[ext_ids["external_id"] for ext_ids in relationships.dump()])


@pytest.fixture
def relationship_with_resources(new_asset, new_time_series, cognite_client):
    external_id = uuid.uuid4().hex[0:20]
    asset_ext_id = new_asset.external_id
    time_series_ext_id = new_time_series.external_id
    relationship = cognite_client.relationships.create(
        Relationship(
            external_id=external_id,
            source_external_id=asset_ext_id,
            source_type="asset",
            target_external_id=time_series_ext_id,
            target_type="timeseries",
        )
    )
    yield relationship, external_id, new_asset, new_time_series
    cognite_client.relationships.delete(external_id=external_id)
    assert cognite_client.relationships.retrieve(external_id=relationship.external_id) is None


class TestRelationshipscognite_client:
    def test_get_single(self, cognite_client, new_relationship):
        new_rel, ext_id = new_relationship
        res = cognite_client.relationships.retrieve(external_id=new_rel.external_id)
        assert isinstance(res, Relationship)
        assert new_rel.external_id == ext_id
        assert res.confidence == 1

    def test_retrieve_unknown(self, cognite_client, new_relationship):
        with pytest.raises(CogniteNotFoundError):
            cognite_client.relationships.retrieve_multiple(external_ids=["this does not exist"])
        assert cognite_client.relationships.retrieve(external_id="this does not exist") is None

    def test_update_single(self, cognite_client, new_relationship):
        new_rel, ext_id = new_relationship
        updated_rel = cognite_client.relationships.update(
            RelationshipUpdate(ext_id).target_type.set("timeseries").confidence.set(None)
        )
        assert isinstance(updated_rel, Relationship)
        assert updated_rel.target_type == "timeSeries"
        assert updated_rel.confidence == None

    def test_list_filter(self, cognite_client, create_multiple_relationships):
        relationships_ext_ids, ext_id, source_ext_id = create_multiple_relationships
        res = cognite_client.relationships.list(source_external_ids=[source_ext_id[3]])
        assert len(res) == 2
        assert isinstance(res, RelationshipList)

    def test_list_data_set(self, cognite_client, new_relationship):
        new_rel, ext_id = new_relationship
        pre_existing_data_set = cognite_client.data_sets.retrieve(external_id="pre_existing_data_set")
        res = cognite_client.relationships.list(
            source_external_ids=[ext_id], data_set_external_ids=[pre_existing_data_set.external_id]
        )
        res2 = cognite_client.relationships.list(target_external_ids=[ext_id], data_set_ids=[pre_existing_data_set.id])
        assert res == res2
        assert isinstance(res, RelationshipList)

    def test_list_label_filter(self, cognite_client, create_multiple_relationships):
        relationships, ext_id, source_ext_id = create_multiple_relationships
        res = cognite_client.relationships.list(labels=LabelFilter(contains_all=[ext_id]))
        assert len(res) == 3
        assert isinstance(res, RelationshipList)

    def test_fetch_resources_list(self, cognite_client, relationship_with_resources):
        relationship, ext_id, asset, time_series = relationship_with_resources
        res = cognite_client.relationships.list(
            source_external_ids=[relationship.source_external_id], fetch_resources=True
        )
        assert res[0].source == asset
        assert res[0].target == time_series

    def test_fetch_resources_retrieve(self, cognite_client, relationship_with_resources):
        relationship, ext_id, asset, time_series = relationship_with_resources
        res = cognite_client.relationships.retrieve_multiple(external_ids=[ext_id], fetch_resources=True)
        assert res[0].source == asset
        assert res[0].target == time_series

    def test_deletes_ignore_unknown_ids(self, cognite_client):
        cognite_client.relationships.delete(external_id=["non_existing_rel"], ignore_unknown_ids=True)

    def test_partitioned_list(self, cognite_client, create_multiple_relationships):
        _, _, ext_ids = create_multiple_relationships
        res_flat = cognite_client.relationships.list(limit=None, source_external_ids=ext_ids)
        res_part = cognite_client.relationships.list(partitions=8, limit=None, source_external_ids=ext_ids)
        assert len(res_flat) > 0
        assert len(res_flat) == len(res_part)
        assert {a.external_id for a in res_flat} == {a.external_id for a in res_part}

    def test_compare_partitioned_gen_and_list(self, cognite_client, create_multiple_relationships):
        _, _, ext_ids = create_multiple_relationships
        res_generator = cognite_client.relationships(partitions=8, limit=None, source_external_ids=ext_ids)
        res_list = cognite_client.relationships.list(partitions=8, limit=None, source_external_ids=ext_ids)
        assert {a.external_id for a in res_generator} == {a.external_id for a in res_list}
