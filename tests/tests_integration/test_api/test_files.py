import time
import uuid
from tempfile import NamedTemporaryFile, TemporaryDirectory

import pytest

from cognite.client.data_classes import (
    FileMetadata,
    FileMetadataFilter,
    FileMetadataUpdate,
    GeoLocation,
    GeoLocationFilter,
    Geometry,
    GeometryFilter,
    Label,
    LabelDefinition,
)
from cognite.client.utils._auxiliary import random_string


@pytest.fixture(scope="class")
def mock_geo_location():
    geometry = Geometry(type="LineString", coordinates=[[30, 10], [10, 30], [40, 40]])
    yield GeoLocation(type="Feature", geometry=geometry, properties=dict())


@pytest.fixture(scope="class")
def new_file(cognite_client):
    res = cognite_client.files.upload_bytes(content="blabla", name="myspecialfile")
    while True:
        if cognite_client.files.retrieve(id=res.id).uploaded:
            break
        time.sleep(0.5)
    yield res
    cognite_client.files.delete(id=res.id)
    assert cognite_client.files.retrieve(id=res.id) is None


@pytest.fixture(scope="class")
def empty_file(cognite_client):
    name = "empty_" + random_string(10)
    res = cognite_client.files.upload_bytes(content=b"", name=name, external_id=name)
    while True:
        if cognite_client.files.retrieve(id=res.id).uploaded:
            break
        time.sleep(0.5)
    yield res
    cognite_client.files.delete(id=res.id)


@pytest.fixture(scope="class")
def new_file_with_geoLocation(mock_geo_location, cognite_client):
    res = cognite_client.files.upload_bytes(content="blabla", name="geoLocationFile", geo_location=mock_geo_location)
    while True:
        if cognite_client.files.retrieve(id=res.id).uploaded:
            break
        time.sleep(0.5)
    yield res
    cognite_client.files.delete(id=res.id)
    assert cognite_client.files.retrieve(id=res.id) is None


@pytest.fixture(scope="class")
def new_file_with_label(cognite_client):
    label_external_id = uuid.uuid4().hex[0:20]
    label = cognite_client.labels.create(LabelDefinition(external_id=label_external_id, name="mandatory"))
    file = cognite_client.files.upload_bytes(
        content="blabla", name="myspecialfile", labels=[Label(external_id=label_external_id)]
    )
    while True:
        if cognite_client.files.retrieve(id=file.id).uploaded:
            break
        time.sleep(0.5)
    yield file, label.external_id
    cognite_client.files.delete(id=file.id)
    cognite_client.labels.delete(external_id=label_external_id)
    assert cognite_client.files.retrieve(id=file.id) is None


@pytest.fixture(scope="class")
def test_files(cognite_client):
    files = {}
    for file in cognite_client.files:
        if file.name in ["a.txt", "b.txt", "c.txt", "big.txt"]:
            files[file.name] = file
    return files


A_WHILE_AGO = {"max": int(time.time() - 1800) * 1000}


class TestFilesAPI:
    def test_create(self, cognite_client):
        file_metadata = FileMetadata(name="mytestfile")
        returned_file_metadata, upload_url = cognite_client.files.create(file_metadata)
        assert returned_file_metadata.uploaded is False
        cognite_client.files.delete(id=returned_file_metadata.id)

    def test_create_with_geoLocation(self, cognite_client, mock_geo_location):
        file_metadata = FileMetadata(name="mytestfile", geo_location=mock_geo_location)
        returned_file_metadata, upload_url = cognite_client.files.create(file_metadata)
        assert returned_file_metadata.uploaded is False
        assert returned_file_metadata.geo_location == mock_geo_location
        cognite_client.files.delete(id=returned_file_metadata.id)

    def test_retrieve(self, cognite_client):
        res = cognite_client.files.list(name="big.txt", limit=1)
        assert res[0] == cognite_client.files.retrieve(res[0].id)

    def test_retrieve_multiple(self, cognite_client):
        res = cognite_client.files.list(uploaded_time=A_WHILE_AGO, limit=2)
        assert res == cognite_client.files.retrieve_multiple([f.id for f in res])

    def test_retrieve_download_urls(self, cognite_client):
        f1 = cognite_client.files.upload_bytes(b"f1", external_id=random_string(10), name="bla")
        f2 = cognite_client.files.upload_bytes(b"f2", external_id=random_string(10), name="bla")
        download_links = cognite_client.files.retrieve_download_urls(id=f1.id, external_id=f2.external_id)
        assert len(download_links.values()) == 2
        assert download_links[f1.id].startswith("http")
        assert download_links[f2.external_id].startswith("http")

    def test_list(self, cognite_client):
        res = cognite_client.files.list(limit=4)
        assert 4 == len(res)

    def test_aggregate(self, cognite_client):
        res = cognite_client.files.aggregate(filter=FileMetadataFilter(name="big.txt"))
        assert res[0].count > 0

    def test_search(self, cognite_client):
        res = cognite_client.files.search(name="big.txt", filter=FileMetadataFilter(created_time={"min": 0}))
        assert len(res) > 0

    def test_update(self, cognite_client, new_file):
        update_file = FileMetadataUpdate(new_file.id).metadata.set({"bla": "bla"})
        res = cognite_client.files.update(update_file)
        assert {"bla": "bla"} == res.metadata

    def test_update_directory(self, cognite_client, new_file):
        dir = "/some/directory"
        res = cognite_client.files.update(FileMetadata(id=new_file.id, directory=dir))
        assert res.directory == dir

    def test_download(self, cognite_client, test_files):
        test_file = test_files["a.txt"]
        res = cognite_client.files.download_bytes(id=test_file.id)
        assert b"a" == res

    def test_download_new_file(self, cognite_client, new_file):
        assert b"blabla" == cognite_client.files.download_bytes(id=new_file.id)

    def test_download_empty_file(self, cognite_client, empty_file):
        content = cognite_client.files.download_bytes(external_id=empty_file.external_id)
        assert content == b""

        with TemporaryDirectory() as tmpdir:
            cognite_client.files.download(directory=tmpdir, external_id=empty_file.external_id)

        with NamedTemporaryFile() as tmpfile:
            cognite_client.files.download_to_path(path=tmpfile.name, external_id=empty_file.external_id)

    def test_retrieve_file_with_labels(self, cognite_client, new_file_with_label):
        file, label_external_id = new_file_with_label
        res = cognite_client.files.retrieve(id=file.id)
        assert len(res.labels) == 1
        assert res.labels[0].external_id == label_external_id

    def test_filter_file_on_geoLocation(self, cognite_client, new_file_with_geoLocation, mock_geo_location):
        max_retries = 10
        geometry_filter = GeometryFilter(type="Point", coordinates=[30, 10])
        geo_location_filter = GeoLocationFilter(relation="intersects", shape=geometry_filter)
        res = cognite_client.files.list(geo_location=geo_location_filter)
        for _ in range(max_retries):
            if len(res) > 0:
                break
            time.sleep(0.2)
            res = cognite_client.files.list(geo_location=geo_location_filter)
        assert res[0].geo_location == new_file_with_geoLocation.geo_location
