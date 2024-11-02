import time
import uuid

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
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteFileApply
from cognite.client.utils._text import random_string


def await_file_upload(client, file_id):
    for i in range(50):
        if client.files.retrieve(id=file_id).uploaded:
            return
        time.sleep(0.5 + i / 10)
    raise RuntimeError(f"Test file id={file_id} never changed status to 'uploaded=True'")


@pytest.fixture(scope="class")
def mock_geo_location():
    geometry = Geometry(type="LineString", coordinates=[[30, 10], [10, 30], [40, 40]])
    yield GeoLocation(type="Feature", geometry=geometry, properties=dict())


@pytest.fixture(scope="class")
def new_file(cognite_client):
    res = cognite_client.files.upload_bytes(content="blabla", name="myspecialfile", directory="/foo/bar/baz")
    await_file_upload(cognite_client, res.id)
    yield res
    cognite_client.files.delete(id=res.id)
    assert cognite_client.files.retrieve(id=res.id) is None


@pytest.fixture(scope="class")
def empty_file(cognite_client):
    name = "empty_" + random_string(10)
    res = cognite_client.files.upload_bytes(content=b"", name=name, external_id=name)
    await_file_upload(cognite_client, res.id)
    yield res
    cognite_client.files.delete(id=res.id)


@pytest.fixture(scope="class")
def new_file_with_geoLocation(mock_geo_location, cognite_client):
    res = cognite_client.files.upload_bytes(content="blabla", name="geoLocationFile", geo_location=mock_geo_location)
    await_file_upload(cognite_client, res.id)
    yield res
    cognite_client.files.delete(id=res.id)
    assert cognite_client.files.retrieve(id=res.id) is None


@pytest.fixture(scope="class")
def new_file_with_label(cognite_client):
    label_external_id = uuid.uuid4().hex[0:20]
    label = cognite_client.labels.create(LabelDefinition(external_id=label_external_id, name="mandatory"))
    file = cognite_client.files.upload_bytes(
        content="blabla",
        name="myspecialfile",
        labels=[Label(external_id=label_external_id)],
    )
    await_file_upload(cognite_client, file.id)
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

    def test_retrieve_multiple_ignore_unknown_ids(self, cognite_client):
        assert [] == cognite_client.files.retrieve_multiple(
            external_ids=["this file doesn't exist"], ignore_unknown_ids=True
        )

    def test_retrieve_download_urls(self, cognite_client):
        f1 = cognite_client.files.upload_bytes(b"f1", external_id=random_string(10), name="bla")
        f2 = cognite_client.files.upload_bytes(b"f2", external_id=random_string(10), name="bla")
        time.sleep(0.5)
        download_links = cognite_client.files.retrieve_download_urls(id=f1.id, external_id=f2.external_id)
        assert len(download_links.values()) == 2
        assert download_links[f1.id].startswith("http")
        assert download_links[f2.external_id].startswith("http")

    def test_retrieve_download_urls_with_extended_expiration(self, cognite_client):
        f1 = cognite_client.files.upload_bytes(b"f1", external_id=random_string(10), name="bla")
        f2 = cognite_client.files.upload_bytes(b"f2", external_id=random_string(10), name="bla")
        time.sleep(0.5)
        download_links = cognite_client.files.retrieve_download_urls(
            id=f1.id, external_id=f2.external_id, extended_expiration=True
        )
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

    def test_download_multiple__name_conflict(self, tmp_path, cognite_client, new_file, new_file_with_label):
        new_file2 = new_file_with_label[0]
        assert new_file.directory is not None
        assert new_file2.directory is None

        # Both files are named the same, so this should trigger the duplicate warning:
        with pytest.warns(UserWarning, match=r"^There are 1 duplicate file name\(s\)"):
            cognite_client.files.download(
                directory=tmp_path,
                id=[new_file.id, new_file2.id],
                keep_directory_structure=False,
            )
        downloaded = list(tmp_path.rglob("*"))
        assert len(downloaded) == 1  # On name conflict, only 1 survive
        assert downloaded[0].stem == new_file.name

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

    def test_download_empty_file(self, cognite_client, empty_file, tmp_path):
        content = cognite_client.files.download_bytes(external_id=empty_file.external_id)
        assert content == b""
        cognite_client.files.download(directory=tmp_path, external_id=empty_file.external_id)

        tmp_file = tmp_path / empty_file.name
        cognite_client.files.download_to_path(path=tmp_file, external_id=empty_file.external_id)

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

    def test_upload_bytes_with_nordic_characters(self, cognite_client: CogniteClient) -> None:
        content = "æøåøøøø ååå ææææ"
        external_id = "test_upload_bytes_with_nordic_characters"

        _ = cognite_client.files.upload_bytes(
            content=content,
            name="nordic_chars.txt",
            external_id=external_id,
            overwrite=True,
        )

        retrieved_content = cognite_client.files.download_bytes(external_id=external_id)
        assert retrieved_content == content.encode("utf-8")

    def test_upload_multipart(self, cognite_client: CogniteClient) -> None:
        # Min file chunk size is 5MiB
        content_1 = "abcde" * 1_200_000
        content_2 = "fghij"

        external_id = "test_upload_multipart"

        with cognite_client.files.multipart_upload_session(
            name="test_multipart.txt",
            mime_type="text/plain",
            parts=2,
            external_id=external_id,
            overwrite=True,
        ) as session:
            session.upload_part(0, content_1)
            session.upload_part(1, content_2)

        for _ in range(10):
            file = cognite_client.files.retrieve(session.file_metadata.id)
            if file.uploaded:
                break
            time.sleep(1)

        retrieved_content = cognite_client.files.download_bytes(external_id=external_id)
        assert len(retrieved_content) == 6000005

        cognite_client.files.delete(session.file_metadata.id)

    def test_create_retrieve_update_delete_with_instance_id(
        self, cognite_client: CogniteClient, instance_id_test_space: str
    ) -> None:
        exernal_id = "file_python_sdk_instance_id_tests" + random_string(10)
        file = CogniteFileApply(
            space=instance_id_test_space,
            external_id=exernal_id,
            name="file_python_sdk_instance_id_tests",
            description="This file was created by the Python SDK",
            source_id="source:id",
            mime_type="text/plain",
            directory="/foo/bar/baz",
        )
        instance_id = file.as_id()
        try:
            created = cognite_client.data_modeling.instances.apply(file)
            assert len(created.nodes) == 1
            assert created.nodes[0].as_id() == instance_id

            f1 = cognite_client.files.upload_content_bytes(b"f1", instance_id=instance_id)
            time.sleep(0.5)
            download_links = cognite_client.files.retrieve_download_urls(instance_id=instance_id)
            assert len(download_links.values()) == 1
            assert download_links[f1.instance_id].startswith("http")

            content_1 = "abcde" * 1_200_000
            content_2 = "fghij"
            with cognite_client.files.multipart_upload_content_session(parts=2, instance_id=instance_id) as session:
                session.upload_part(0, content_1)
                session.upload_part(1, content_2)

            retrieved_content = cognite_client.files.download_bytes(instance_id=instance_id)
            assert len(retrieved_content) == 6000005

            retrieved = cognite_client.files.retrieve(instance_id=instance_id)
            assert retrieved is not None
            assert retrieved.instance_id == instance_id

            update_writable = retrieved.as_write()
            update_writable.metadata = {"a": "b"}
            update_writable.external_id = exernal_id + "updated"
            updated_writable = cognite_client.files.update(update_writable)
            assert updated_writable.metadata == {"a": "b"}
            assert updated_writable.external_id == exernal_id + "updated"

            updated = cognite_client.files.update(FileMetadataUpdate(instance_id=instance_id).metadata.add({"c": "d"}))
            assert updated.metadata == {"a": "b", "c": "d"}

            retrieved = cognite_client.files.retrieve_multiple(instance_ids=[instance_id])
            assert retrieved.dump() == [updated.dump()]
        finally:
            cognite_client.data_modeling.instances.delete(nodes=instance_id)

    def test_create_delete_ignore_unknown_ids(self, cognite_client: CogniteClient) -> None:
        file_metadata = FileMetadata(name="mytestfile")
        returned_file_metadata, upload_url = cognite_client.files.create(file_metadata)
        cognite_client.files.delete(id=[returned_file_metadata.id, 1], ignore_unknown_ids=True)
