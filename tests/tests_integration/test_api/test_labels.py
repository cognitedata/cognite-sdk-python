from unittest import mock

import pytest

from cognite.client.data_classes import Asset, AssetUpdate, Label, LabelDefinition
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

    def test_retrieve(self, cognite_client):
        res = cognite_client.labels.list(limit=1)
        assert 1 == len(res)
        res_retrieve = cognite_client.labels.retrieve_multiple([res[0].external_id])
        assert res_retrieve.external_id == res[0].external_id

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

