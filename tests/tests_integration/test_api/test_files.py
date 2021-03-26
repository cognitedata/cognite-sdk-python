import time
import uuid
from tempfile import NamedTemporaryFile, TemporaryDirectory

import pytest

from cognite.client import CogniteClient
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

COGNITE_CLIENT = CogniteClient()


@pytest.fixture(scope="class")
def mock_geo_location():
    geometry = Geometry(type="LineString", coordinates=[[30, 10], [10, 30], [40, 40]])
    yield GeoLocation(type="Feature", geometry=geometry, properties=dict())


@pytest.fixture(scope="class")
def new_file():
    res = COGNITE_CLIENT.files.upload_bytes(content="blabla", name="myspecialfile")
    while True:
        if COGNITE_CLIENT.files.retrieve(id=res.id).uploaded:
            break
        time.sleep(0.5)
    yield res
    COGNITE_CLIENT.files.delete(id=res.id)
    assert COGNITE_CLIENT.files.retrieve(id=res.id) is None


@pytest.fixture(scope="class")
def empty_file():
    name = "empty_" + random_string(10)
    res = COGNITE_CLIENT.files.upload_bytes(content=b"", name=name, external_id=name)
    while True:
        if COGNITE_CLIENT.files.retrieve(id=res.id).uploaded:
            break
        time.sleep(0.5)
    yield res
    COGNITE_CLIENT.files.delete(id=res.id)


@pytest.fixture(scope="class")
def new_file_with_geoLocation(mock_geo_location):
    res = COGNITE_CLIENT.files.upload_bytes(content="blabla", name="geoLocationFile", geo_location=mock_geo_location)
    while True:
        if COGNITE_CLIENT.files.retrieve(id=res.id).uploaded:
            break
        time.sleep(0.5)
    yield res
    COGNITE_CLIENT.files.delete(id=res.id)
    assert COGNITE_CLIENT.files.retrieve(id=res.id) is None


@pytest.fixture(scope="class")
def new_file_with_label():
    label_external_id = uuid.uuid4().hex[0:20]
    label = COGNITE_CLIENT.labels.create(LabelDefinition(external_id=label_external_id, name="mandatory"))
    file = COGNITE_CLIENT.files.upload_bytes(
        content="blabla", name="myspecialfile", labels=[Label(external_id=label_external_id)]
    )
    while True:
        if COGNITE_CLIENT.files.retrieve(id=file.id).uploaded:
            break
        time.sleep(0.5)
    yield file, label.external_id
    COGNITE_CLIENT.files.delete(id=file.id)
    COGNITE_CLIENT.labels.delete(external_id=label_external_id)
    assert COGNITE_CLIENT.files.retrieve(id=file.id) is None


@pytest.fixture(scope="class")
def test_files():
    files = {}
    for file in COGNITE_CLIENT.files:
        if file.name in ["a.txt", "b.txt", "c.txt", "big.txt"]:
            files[file.name] = file
    return files


A_WHILE_AGO = {"max": int(time.time() - 1800) * 1000}


class TestFilesAPI:
    def test_create(self):
        file_metadata = FileMetadata(name="mytestfile")
        returned_file_metadata, upload_url = COGNITE_CLIENT.files.create(file_metadata)
        assert returned_file_metadata.uploaded is False
        COGNITE_CLIENT.files.delete(id=returned_file_metadata.id)

    def test_create_with_geoLocation(self, mock_geo_location):
        file_metadata = FileMetadata(name="mytestfile", geo_location=mock_geo_location)
        returned_file_metadata, upload_url = COGNITE_CLIENT.files.create(file_metadata)
        assert returned_file_metadata.uploaded is False
        assert returned_file_metadata.geo_location == mock_geo_location
        COGNITE_CLIENT.files.delete(id=returned_file_metadata.id)

    def test_retrieve(self):
        res = COGNITE_CLIENT.files.list(name="big.txt", limit=1)
        assert res[0] == COGNITE_CLIENT.files.retrieve(res[0].id)

    def test_retrieve_multiple(self):
        res = COGNITE_CLIENT.files.list(uploaded_time=A_WHILE_AGO, limit=2)
        assert res == COGNITE_CLIENT.files.retrieve_multiple([f.id for f in res])

    def test_list(self):
        res = COGNITE_CLIENT.files.list(limit=4)
        assert 4 == len(res)

    def test_aggregate(self):
        res = COGNITE_CLIENT.files.aggregate(filter=FileMetadataFilter(name="big.txt"))
        assert res[0].count > 0

    def test_search(self):
        res = COGNITE_CLIENT.files.search(name="big.txt", filter=FileMetadataFilter(created_time={"min": 0}))
        assert len(res) > 0

    def test_update(self, new_file):
        update_file = FileMetadataUpdate(new_file.id).metadata.set({"bla": "bla"})
        res = COGNITE_CLIENT.files.update(update_file)
        assert {"bla": "bla"} == res.metadata

    def test_update_directory(self, new_file):
        dir = "/some/directory"
        res = COGNITE_CLIENT.files.update(FileMetadata(id=new_file.id, directory=dir))
        assert res.directory == dir

    def test_download(self, test_files):
        test_file = test_files["a.txt"]
        res = COGNITE_CLIENT.files.download_bytes(id=test_file.id)
        assert b"a" == res

    def test_download_new_file(self, new_file):
        assert b"blabla" == COGNITE_CLIENT.files.download_bytes(id=new_file.id)

    def test_download_empty_file(self, empty_file):
        content = COGNITE_CLIENT.files.download_bytes(external_id=empty_file.external_id)
        assert content == b""

        with TemporaryDirectory() as tmpdir:
            COGNITE_CLIENT.files.download(directory=tmpdir, external_id=empty_file.external_id)

        with NamedTemporaryFile() as tmpfile:
            COGNITE_CLIENT.files.download_to_path(path=tmpfile.name, external_id=empty_file.external_id)

    def test_retrieve_file_with_labels(self, new_file_with_label):
        file, label_external_id = new_file_with_label
        res = COGNITE_CLIENT.files.retrieve(id=file.id)
        assert len(res.labels) == 1
        assert res.labels[0].external_id == label_external_id

    def test_filter_file_on_geoLocation(self, new_file_with_geoLocation, mock_geo_location):
        max_retries = 10
        geometry_filter = GeometryFilter(type="Point", coordinates=[30, 10])
        geo_location_filter = GeoLocationFilter(relation="intersects", shape=geometry_filter)
        res = COGNITE_CLIENT.files.list(geo_location=geo_location_filter)
        for _ in range(max_retries):
            if len(res) > 0:
                break
            time.sleep(0.2)
            res = COGNITE_CLIENT.files.list(geo_location=geo_location_filter)
        assert res[0].geo_location == new_file_with_geoLocation.geo_location
