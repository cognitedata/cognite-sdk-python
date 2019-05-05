import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, AssetFilter, AssetUpdate
from cognite.client.exceptions import CogniteAPIError
from tests.utils import set_request_limit

COGNITE_CLIENT = CogniteClient()


@pytest.fixture
def new_asset():
    ts = COGNITE_CLIENT.assets.create(Asset(name="any"))
    yield ts
    COGNITE_CLIENT.assets.delete(id=ts.id)
    with pytest.raises(CogniteAPIError) as e:
        COGNITE_CLIENT.assets.retrieve(ts.id)
    assert 400 == e.value.code


class TestAssetsAPI:
    def test_get(self):
        res = COGNITE_CLIENT.assets.list(limit=1)
        assert res[0] == COGNITE_CLIENT.assets.retrieve(res[0].id)

    def test_list(self, mocker):
        mocker.spy(COGNITE_CLIENT.assets, "_post")

        with set_request_limit(COGNITE_CLIENT.assets, 10):
            res = COGNITE_CLIENT.assets.list(limit=20)

        assert 20 == len(res)
        assert 2 == COGNITE_CLIENT.assets._post.call_count

    def test_search(self):
        res = COGNITE_CLIENT.assets.search(name="test__asset_0", filter=AssetFilter(name="test__asset_0"))
        assert len(res) > 0

    def test_update(self, new_asset):
        update_asset = AssetUpdate(new_asset.id).name.set("newname")
        res = COGNITE_CLIENT.assets.update(update_asset)
        assert "newname" == res.name
