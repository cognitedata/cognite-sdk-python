from unittest import mock

import pytest

from cognite.client.data_classes import Asset, AssetUpdate, Label, LabelDefinition
from cognite.client.data_classes.labels import LabelDefinitionList
from cognite.client.exceptions import CogniteNotFoundError
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
        xids = res.as_external_ids()
        res_lst = cognite_client.labels.retrieve(xids)
        assert isinstance(res_lst, LabelDefinitionList)
        assert res_lst.as_external_ids() == xids

        res_single = cognite_client.labels.retrieve(xids[0])
        assert isinstance(res_single, LabelDefinition)
        assert res_single.external_id == xids[0]

    def test_retrieve_not_found(self, cognite_client):
        xids = ["this does not exist"]
        
        res_lst = cognite_client.labels.retrieve(xids, ignore_unknown_ids=True)
        assert len(res_lst) == 0

        with pytest.raises(CogniteNotFoundError) as error:
            cognite_client.labels.retrieve(xids)
        assert error.value.code == 400

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
