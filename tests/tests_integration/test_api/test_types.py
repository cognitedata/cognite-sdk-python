from datetime import datetime
from unittest import mock

import pytest

import cognite.client.utils._time
from cognite.client.data_classes import Asset, AssetUpdate, Type, TypeFilter, TypeList
from cognite.client.experimental import CogniteClient
from cognite.client.utils._auxiliary import random_string
from tests.utils import set_request_limit

COGNITE_CLIENT = CogniteClient()


@pytest.fixture
def new_type():
    tp = COGNITE_CLIENT.types.create(
        Type(external_id=random_string(30), properties=[{"propertyId": "prop", "type": "string"}])
    )
    yield tp
    COGNITE_CLIENT.types.delete(external_id=tp.external_id)
    # assert COGNITE_CLIENT.types.retrieve(external_id=tp.external_id) is None


@pytest.fixture
def post_spy():
    with mock.patch.object(COGNITE_CLIENT.types, "_post", wraps=COGNITE_CLIENT.types._post) as _:
        yield


class TestTypesAPI:
    def test_retrieve_eid(self):
        res = COGNITE_CLIENT.types.list()
        assert res[0] == COGNITE_CLIENT.types.retrieve(external_id=res[0].external_id)

    def test_retrieve_id(self):
        res = COGNITE_CLIENT.types.list()
        assert res[0] == COGNITE_CLIENT.types.retrieve(id=res[0].id)

    def test_retrieve_multiple(self):
        res_listed_ids = [e.external_id for e in COGNITE_CLIENT.types.list()[:2]]
        res_lookup_ids = [e.external_id for e in COGNITE_CLIENT.types.retrieve_multiple(external_ids=res_listed_ids)]
        for listed_id in res_listed_ids:
            assert listed_id in res_lookup_ids

    def test_list(self, post_spy):
        res = COGNITE_CLIENT.types.list()
        assert 0 < len(res) < 100
        assert 1 == COGNITE_CLIENT.types._post.call_count

    def test_create_asset_with_type(self, new_type):
        types = [
            {"type": {"externalId": new_type.external_id, "version": new_type.version}, "properties": {"prop": "str"}}
        ]
        ac = COGNITE_CLIENT.assets.create(Asset(name="any", types=types))
        assert isinstance(ac, Asset)
        del ac.types[0]["type"]["id"]
        assert types == ac.types
        COGNITE_CLIENT.assets.delete(id=ac.id)

    def test_update_asset_with_type(self, new_type):
        ac = COGNITE_CLIENT.assets.create(Asset(name="any", description="delete me"))
        assert not ac.types
        update = AssetUpdate(id=ac.id)
        update = update.put_type(external_id=new_type.external_id, version=new_type.version, properties={"prop": "str"})
        assert isinstance(update, AssetUpdate)
        ua = COGNITE_CLIENT.assets.update(update)
        assert len(ua.types) == 1
        assert new_type.external_id == ua.types[0]["type"]["externalId"]

        update = AssetUpdate(id=ac.id)
        update = update.remove_type(external_id=new_type.external_id, version=new_type.version)
        assert isinstance(update, AssetUpdate)
        ua = COGNITE_CLIENT.assets.update(update)

        assert not ua.types
        COGNITE_CLIENT.assets.delete(id=ac.id)
