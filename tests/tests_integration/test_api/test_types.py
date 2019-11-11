from datetime import datetime
from unittest import mock

import pytest

import cognite.client.utils._time
from cognite.client.data_classes import Type, TypeFilter, TypeList, TypeUpdate
from cognite.client.experimental import CogniteClient
from cognite.client.utils._auxiliary import random_string
from tests.utils import set_request_limit

COGNITE_CLIENT = CogniteClient()


@pytest.fixture
def new_type():
    type = COGNITE_CLIENT.types.create(Type(external_id=random_string(30)))
    yield type
    COGNITE_CLIENT.types.delete(external_id=type.external_id)
    assert COGNITE_CLIENT.types.retrieve(type.external_id) is None


@pytest.fixture
def post_spy():
    with mock.patch.object(COGNITE_CLIENT.types, "_post", wraps=COGNITE_CLIENT.types._post) as _:
        yield


class TestTypesAPI:
    def test_retrieve(self):
        res = COGNITE_CLIENT.types.list()
        assert res[0] == COGNITE_CLIENT.types.retrieve(external_id=res[0].external_id)

    def test_retrieve_multiple(self):
        res_listed_ids = [e.external_id for e in COGNITE_CLIENT.types.list()[:2]]
        res_lookup_ids = [e.external_id for e in COGNITE_CLIENT.types.retrieve_multiple(external_ids=res_listed_ids)]
        for listed_id in res_listed_ids:
            assert listed_id in res_lookup_ids

    def test_list(self, post_spy):
        res = COGNITE_CLIENT.types.list()
        assert 0 < len(res) < 100
        assert 1 == COGNITE_CLIENT.types._post.call_count


#    def test_update(self, new_type):
#        update_asset = TypeUpdate(new_type.external_id).description.set("bla")
#        res = COGNITE_CLIENT.types.update(update_asset)
#        assert "bla" == res.description
