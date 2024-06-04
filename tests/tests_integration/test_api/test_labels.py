from unittest import mock

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, AssetUpdate, Label, LabelDefinition, LabelDefinitionList
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._text import random_string


@pytest.fixture
def new_label(cognite_client):
    tp = cognite_client.labels.create(LabelDefinition(external_id=random_string(30), name="mandatory"))
    yield tp
    cognite_client.labels.delete(external_id=tp.external_id)


@pytest.fixture
def post_spy(cognite_client):
    with mock.patch.object(cognite_client.labels, "_post", wraps=cognite_client.labels._post) as _:
        yield


class TestLabelsAPI:
    def test_list(self, cognite_client, new_label, post_spy):
        res = cognite_client.labels.list(limit=100)
        assert 0 < len(res) <= 100
        assert 1 == cognite_client.labels._post.call_count

    def test_retrieve_existing_labels(self, cognite_client: CogniteClient, new_label: LabelDefinition) -> None:
        res = cognite_client.labels.retrieve(external_id=new_label.external_id)
        assert isinstance(res, LabelDefinition)
        assert res.external_id == new_label.external_id
        assert res.name == new_label.name

    def test_retrieve_label_list(self, cognite_client: CogniteClient, new_label: LabelDefinition) -> None:
        res = cognite_client.labels.retrieve(external_id=[new_label.external_id])
        assert isinstance(res, LabelDefinitionList)
        assert len(res) == 1
        assert res[0].external_id == new_label.external_id
        assert res[0].name == new_label.name

    def test_retrieve_non_existing_labels(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(CogniteAPIError) as e:
            cognite_client.labels.retrieve(external_id="this does not exist")
        assert e.value.code == 404

    def test_retrieve_none_existing_labels_ignore_unknown(self, cognite_client: CogniteClient) -> None:
        res = cognite_client.labels.retrieve(external_id="this does not exist", ignore_unknown_ids=True)
        assert res is None

    def test_create_asset_with_label(self, cognite_client, new_label):
        ac = cognite_client.assets.create(Asset(name="any", labels=[Label(external_id=new_label.external_id)]))
        assert isinstance(ac, Asset)
        assert len(ac.labels) == 1
        cognite_client.assets.delete(id=ac.id)

    def test_update_asset_with_label(self, cognite_client, new_label):
        ac = cognite_client.assets.create(Asset(name="any", description="delete me"))
        assert not ac.labels
        update = AssetUpdate(id=ac.id).labels.add([new_label.external_id])
        assert isinstance(update, AssetUpdate)
        ua = cognite_client.assets.update(update)
        assert len(ua.labels) == 1
        assert new_label.external_id == ua.labels[0].external_id

        update = AssetUpdate(id=ac.id).labels.remove([new_label.external_id])
        assert isinstance(update, AssetUpdate)
        ua = cognite_client.assets.update(update)

        assert not ua.labels
        cognite_client.assets.delete(id=ac.id)
