import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadata, FileMetadataFilter, FileMetadataUpdate
from cognite.client.exceptions import CogniteAPIError

COGNITE_CLIENT = CogniteClient()


@pytest.fixture(autouse=True, scope="module")
def set_limit():
    limit_tmp = COGNITE_CLIENT.files._LIMIT
    COGNITE_CLIENT.files._LIMIT = 2
    yield set_limit
    COGNITE_CLIENT.files._LIMIT = limit_tmp


@pytest.fixture
def new_file():
    res = COGNITE_CLIENT.files.upload_bytes(content="blabla", name="myspecialfile")
    yield res
    COGNITE_CLIENT.files.delete(id=res.id)
    with pytest.raises(CogniteAPIError) as e:
        COGNITE_CLIENT.files.get(id=res.id)
    assert 400 == e.value.code


class TestFilesAPI:
    def test_get(self):
        res = COGNITE_CLIENT.files.list(limit=1)
        assert res[0] == COGNITE_CLIENT.files.get(res[0].id)

    def test_list(self, mocker):
        mocker.spy(COGNITE_CLIENT.files, "_post")

        res = COGNITE_CLIENT.files.list(limit=4)

        assert 4 == len(res)
        assert 2 == COGNITE_CLIENT.files._post.call_count

    def test_search(self):
        res = COGNITE_CLIENT.files.search(name="big.txt", filter=FileMetadataFilter(created_time={"min": 0}))
        assert len(res) > 0

    def test_update(self, new_file):
        update_file = FileMetadataUpdate(new_file.id).metadata.set({"bla": "bla"})
        res = COGNITE_CLIENT.files.update(update_file)
        assert {"bla": "bla"} == res.metadata
