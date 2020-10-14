import uuid

import pytest

from cognite.client.beta import CogniteClient
from cognite.client.data_classes import (
    Label,
    LabelDefinition,
    LabelFilter,
    Relationship,
    RelationshipFilter,
    RelationshipList,
)
from cognite.client.exceptions import CogniteNotFoundError

API_REL = CogniteClient().relationships


@pytest.fixture
def new_relationship(new_label):
    external_id = uuid.uuid4().hex[0:20]
    from cognite.client import CogniteClient

    pre_existing_data_set = CogniteClient().data_sets.retrieve(external_id="pre_existing_data_set")
    relationship = API_REL.create(
        Relationship(
            external_id=external_id,
            source_external_id="source_ext_id",
            source_type="asset",
            target_external_id="target_ext_id",
            data_set_id=pre_existing_data_set.id,
            target_type="event",
            labels=[Label(new_label.external_id)],
            confidence=1,
        )
    )
    yield relationship
    API_REL.delete(external_id=external_id)
    assert API_REL.retrieve(external_id=relationship.external_id) is None


@pytest.fixture
def new_label():
    # create a label to use in relationships
    from cognite.client import CogniteClient

    external_id = uuid.uuid4().hex[0:20]
    tp = CogniteClient().labels.create(LabelDefinition(external_id=external_id, name="mandatory"))
    assert isinstance(tp, LabelDefinition)
    yield tp
    CogniteClient().labels.delete(external_id=tp.external_id)


@pytest.fixture
def create_multiple_relationships(new_label):
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
    relationships = API_REL.create(relationship_list)
    assert isinstance(relationships, RelationshipList)
    yield relationships_ext_id, ext_id, random_ext_id
    API_REL.delete(external_id=[ext_ids["external_id"] for ext_ids in relationships.dump()])


class TestRelationshipsAPI:
    def test_get_single(self, new_relationship):
        res = API_REL.retrieve(external_id=new_relationship.external_id)
        assert isinstance(res, Relationship)
        assert res.dump()["confidence"] == 1

    def test_retrieve_unknown(self, new_relationship):
        with pytest.raises(CogniteNotFoundError):
            API_REL.retrieve_multiple(external_ids=["this does not exist"])
        assert API_REL.retrieve(external_id="this does not exist") is None

    def test_list_filter(self, create_multiple_relationships):
        relationships_ext_ids, ext_id, source_ext_id = create_multiple_relationships
        res = API_REL.list(source_external_ids=[source_ext_id[3]])
        res2 = API_REL.list(confidence={"min": 1, "max": 1})
        assert len(res) == 2
        assert res2[0].external_id == relationships_ext_ids[1]
        assert res2[1].external_id == relationships_ext_ids[4]
        assert isinstance(res, RelationshipList)

    def test_list_label_filter(self, create_multiple_relationships):
        relationships, ext_id, source_ext_id = create_multiple_relationships
        res = API_REL.list(labels=LabelFilter(contains_all=[ext_id]))
        assert len(res) == 3
        assert isinstance(res, RelationshipList)
