import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadata, FileMetadataFilter, FileMetadataUpdate
from cognite.client.exceptions import CogniteAPIError

COGNITE_CLIENT = CogniteClient()


@pytest.fixture(scope="class")
def new_file():
    res = COGNITE_CLIENT.files.upload_bytes(content="blabla", name="myspecialfile")
    yield res
    COGNITE_CLIENT.files.delete(id=res.id)
    assert COGNITE_CLIENT.files.retrieve(id=res.id) is None


@pytest.fixture(scope="class")
def test_files():
    files = {}
    for file in COGNITE_CLIENT.files:
        if file.name in ["a.txt", "b.txt", "c.txt", "big.txt"]:
            files[file.name] = file
    return files


class TestFilesAPI:
    def test_retrieve(self):
        res = COGNITE_CLIENT.files.list(limit=1)
        assert res[0] == COGNITE_CLIENT.files.retrieve(res[0].id)

    def test_retrieve_multiple(self):
        res = COGNITE_CLIENT.files.list(limit=2)
        assert res == COGNITE_CLIENT.files.retrieve_multiple([f.id for f in res])

    def test_list(self):
        res = COGNITE_CLIENT.files.list(limit=4)
        assert 4 == len(res)

    def test_search(self):
        res = COGNITE_CLIENT.files.search(name="big.txt", filter=FileMetadataFilter(created_time={"min": 0}))
        assert len(res) > 0

    def test_update(self, new_file):
        update_file = FileMetadataUpdate(new_file.id).metadata.set({"bla": "bla"})
        res = COGNITE_CLIENT.files.update(update_file)
        assert {"bla": "bla"} == res.metadata

    def test_download(self, test_files):
        test_file = test_files["a.txt"]
        res = COGNITE_CLIENT.files.download_bytes(id=test_file.id)
        assert b"a" == res
