from __future__ import annotations

import os
import re
from collections.abc import Iterator
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from httpx import Response

from cognite.client._api.files import FileMetadata, FileMetadataList, FileMetadataUpdate
from cognite.client.data_classes import (
    FileMetadataFilter,
    GeoLocation,
    GeoLocationFilter,
    Geometry,
    GeometryFilter,
    Label,
    LabelFilter,
    TimestampRange,
)
from cognite.client.data_classes.data_modeling.ids import NodeId
from cognite.client.exceptions import CogniteAPIError, CogniteAuthorizationError
from tests.utils import get_url, jsgz_load, set_request_limit


@pytest.fixture
def mock_geo_location() -> Iterator[GeoLocation]:
    geometry = Geometry(type="Point", coordinates=[35, 10])
    yield GeoLocation(type="Feature", geometry=geometry)


@pytest.fixture
def mock_files_response(httpx_mock, cognite_client, mock_geo_location: GeoLocation):
    response_body = {
        "items": [
            {
                "externalId": "string",
                "name": "string",
                "source": "string",
                "mimeType": "string",
                "metadata": {"metadata-key": "metadata-value"},
                "assetIds": [1],
                "labels": [{"externalId": "WELL LOG"}],
                "geoLocation": mock_geo_location.dump(camel_case=True),
                "id": 1,
                "uploaded": True,
                "uploadedTime": 0,
                "createdTime": 0,
                "lastUpdatedTime": 0,
            }
        ]
    }
    url_pattern = re.compile(re.escape(get_url(cognite_client.files)) + "/.+")

    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    yield response_body


@pytest.fixture
def example_file(mock_geo_location):
    return {
        "externalId": "string",
        "name": "string",
        "source": "string",
        "mimeType": "string",
        "metadata": {},
        "assetIds": [1],
        "labels": [{"externalId": "WELL LOG"}],
        "geoLocation": mock_geo_location.dump(camel_case=True),
        "id": 1,
        "uploaded": True,
        "uploadedTime": 0,
        "createdTime": 0,
        "lastUpdatedTime": 0,
        "uploadUrl": "https://upload.here",
    }


@pytest.fixture
def mock_file_upload_response(httpx_mock, cognite_client, example_file):
    httpx_mock.add_response(
        method="POST", url=get_url(cognite_client.files) + "/files?overwrite=false", status_code=200, json=example_file
    )
    httpx_mock.add_response(method="PUT", url="https://upload.here", status_code=200)
    yield example_file


@pytest.fixture
def mock_file_upload_response_without_netloc_in_upload_url(httpx_mock, cognite_client, mock_geo_location: GeoLocation):
    response_body = {
        "externalId": "string",
        "name": "string",
        "source": "string",
        "mimeType": "string",
        "metadata": {},
        "assetIds": [1],
        "labels": [{"externalId": "WELL LOG"}],
        "geoLocation": mock_geo_location.dump(camel_case=True),
        "id": 1,
        "uploaded": True,
        "uploadedTime": 0,
        "createdTime": 0,
        "lastUpdatedTime": 0,
        "uploadUrl": "upload/here/to/some/path",
    }
    httpx_mock.add_response(
        method="POST", url=get_url(cognite_client.files) + "/files?overwrite=false", status_code=200, json=response_body
    )
    httpx_mock.add_response(
        method="PUT", url=cognite_client.config.base_url + "/upload/here/to/some/path", status_code=200
    )
    yield response_body


@pytest.fixture
def mock_file_create_response(httpx_mock, cognite_client, mock_geo_location: GeoLocation):
    response_body = {
        "externalId": "string",
        "name": "string",
        "source": "string",
        "mimeType": "string",
        "metadata": {},
        "assetIds": [1],
        "labels": [{"externalId": "WELL LOG"}],
        "geoLocation": mock_geo_location.dump(camel_case=True),
        "id": 1,
        "uploaded": False,
        "uploadedTime": 0,
        "createdTime": 0,
        "lastUpdatedTime": 0,
        "uploadUrl": "https://upload.here",
    }
    httpx_mock.add_response(
        method="POST", url=get_url(cognite_client.files) + "/files?overwrite=false", status_code=200, json=response_body
    )
    yield response_body


@pytest.fixture
def mock_file_download_response(httpx_mock, cognite_client):
    for _ in range(2):
        httpx_mock.add_response(
            method="POST",
            url=get_url(cognite_client.files) + "/files/byids",
            status_code=200,
            json={"items": [{"id": 1, "name": "file1"}, {"id": 10, "externalId": "2", "name": "file2"}]},
            is_optional=True,
        )

    def download_link_callback(request):
        identifier = jsgz_load(request.content)["items"][0]
        response = {}
        if identifier.get("id") == 1:
            response = {"items": [{"id": 1, "downloadUrl": "https://download.file1.here"}]}
        if identifier.get("id") == 10:
            response = {"items": [{"id": 10, "externalId": "2", "downloadUrl": "https://download.file2.here"}]}
        return Response(status_code=200, json=response)

    httpx_mock.add_callback(
        download_link_callback,
        method="POST",
        url=get_url(cognite_client.files) + "/files/downloadlink",
        match_headers={"content-type": "application/json"},
        is_optional=True,
    )
    httpx_mock.add_callback(
        download_link_callback,
        method="POST",
        url=get_url(cognite_client.files) + "/files/downloadlink",
        match_headers={"content-type": "application/json"},
        is_optional=True,
    )
    httpx_mock.add_response(
        method="GET", url="https://download.file1.here", status_code=200, text="content1", is_optional=True
    )
    httpx_mock.add_response(
        method="GET", url="https://download.file2.here", status_code=200, text="content2", is_optional=True
    )
    yield httpx_mock


@pytest.fixture
def mock_file_download_response_with_folder_structure_same_name(httpx_mock, cognite_client):
    httpx_mock.add_response(
        method="POST",
        url=get_url(cognite_client.files) + "/files/byids",
        status_code=200,
        json={
            "items": [
                {"id": 1, "name": "file_a", "directory": "/rootdir/subdir"},
                {"id": 10, "externalId": "2", "name": "file_a"},
            ]
        },
    )

    def download_link_callback(request):
        identifier = jsgz_load(request.content)["items"][0]
        response = {}
        if identifier.get("id") == 1:
            response = {"items": [{"id": 1, "downloadUrl": "https://download.fileFromSubdir.here"}]}
        if identifier.get("id") == 10:
            response = {"items": [{"id": 10, "externalId": "2", "downloadUrl": "https://download.fileNoDir.here"}]}
        return Response(status_code=200, json=response)

    for _ in range(2):
        httpx_mock.add_callback(
            download_link_callback,
            method="POST",
            url=get_url(cognite_client.files) + "/files/downloadlink",
            match_headers={"content-type": "application/json"},
        )
    httpx_mock.add_response(
        method="GET", url="https://download.fileFromSubdir.here", status_code=200, text="contentSubDir"
    )
    httpx_mock.add_response(method="GET", url="https://download.fileNoDir.here", status_code=200, text="contentNoDir")
    yield httpx_mock


@pytest.fixture
def mock_file_download_response_one_fails(httpx_mock, cognite_client):
    httpx_mock.add_response(
        method="POST",
        url=get_url(cognite_client.files) + "/files/byids",
        status_code=200,
        json={
            "items": [
                {"id": 1, "externalId": "success", "name": "file1"},
                {"externalId": "fail", "id": 2, "name": "file2"},
            ]
        },
    )

    def download_link_callback(request):
        identifier = jsgz_load(request.content)["items"][0]
        if identifier.get("id") == 1:
            return Response(status_code=200, json={"items": [{"id": 1, "downloadUrl": "https://download.file1.here"}]})
        elif identifier.get("id") == 2:
            return Response(status_code=400, json={"error": {"message": "User error", "code": 400}})
        raise RuntimeError("Unknown id")

    for _ in range(2):
        httpx_mock.add_callback(
            download_link_callback,
            method="POST",
            url=get_url(cognite_client.files) + "/files/downloadlink",
            match_headers={"content-type": "application/json"},
        )
    httpx_mock.add_response(method="GET", url="https://download.file1.here", status_code=200, text="content1")
    yield httpx_mock


class TestFilesAPI:
    def test_create(self, cognite_client, mock_file_create_response):
        file_metadata = FileMetadata(name="bla")
        returned_file_metadata, upload_url = cognite_client.files.create(file_metadata)
        assert FileMetadata.load(mock_file_create_response) == returned_file_metadata
        assert mock_file_create_response["uploadUrl"] == upload_url

    def test_create_with_label(self, cognite_client, mock_file_create_response):
        file_metadata = FileMetadata(name="bla", labels=[Label(external_id="WELL LOG")])
        returned_file_metadata, upload_url = cognite_client.files.create(file_metadata)
        assert FileMetadata.load(mock_file_create_response) == returned_file_metadata
        assert mock_file_create_response["uploadUrl"] == upload_url
        assert mock_file_create_response["labels"][0]["externalId"] == "WELL LOG"

    def test_create_with_label_request(self, cognite_client, mock_file_create_response, httpx_mock):
        file_metadata = FileMetadata(name="bla", labels=[Label(external_id="WELL LOG")])
        returned_file_metadata, _ = cognite_client.files.create(file_metadata)
        request_body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert FileMetadata.load(mock_file_create_response) == returned_file_metadata
        assert all(body["labels"][0]["externalId"] == "WELL LOG" for body in [request_body, mock_file_create_response])

    def test_create_with_geo_location(self, cognite_client, mock_file_create_response, mock_geo_location):
        file_metadata = FileMetadata(name="bla", geo_location=mock_geo_location)
        returned_file_metadata, upload_url = cognite_client.files.create(file_metadata)
        assert FileMetadata.load(mock_file_create_response) == returned_file_metadata
        assert mock_file_create_response["geoLocation"] == mock_geo_location.dump(camel_case=True)

    def test_create_geo_location_with_invalid_geometry_type(self):
        # TODO: Move? This is a test of the Geometry class
        with pytest.raises(ValueError):
            Geometry(type="someInvalidType", coordinates=[1, 2])

    def test_create_geo_location_with_invalid_geojson_type(self):
        # TODO: Move? This is a test of the GeoLocation class
        g = Geometry(type="Point", coordinates=[1, 2])
        with pytest.raises(ValueError):
            GeoLocation(type="FeatureCollection", geometry=g)

    def test_create_with_geo_location_request(
        self, cognite_client, mock_file_create_response, mock_geo_location, httpx_mock
    ):
        file_metadata = FileMetadata(name="bla", geo_location=mock_geo_location)
        returned_file_metadata, _ = cognite_client.files.create(file_metadata)
        request_body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert FileMetadata.load(mock_file_create_response) == returned_file_metadata
        assert request_body["geoLocation"] == mock_geo_location.dump(camel_case=True)
        assert mock_file_create_response["geoLocation"] == mock_geo_location.dump(camel_case=True)

    def test_retrieve_single(self, cognite_client, mock_files_response):
        res = cognite_client.files.retrieve(id=1)
        assert isinstance(res, FileMetadata)
        assert mock_files_response["items"][0] == res.dump(camel_case=True)

    def test_retrieve_multiple(self, cognite_client, mock_files_response):
        res = cognite_client.files.retrieve_multiple(ids=[1])
        assert isinstance(res, FileMetadataList)
        assert mock_files_response["items"] == res.dump(camel_case=True)

    def test_list(self, cognite_client, mock_files_response, httpx_mock):
        res = cognite_client.files.list(source="bla", limit=10)
        assert isinstance(res, FileMetadataList)
        assert mock_files_response["items"] == res.dump(camel_case=True)
        assert "bla" == jsgz_load(httpx_mock.get_requests()[0].content)["filter"]["source"]
        assert 10 == jsgz_load(httpx_mock.get_requests()[0].content)["limit"]

    def test_list_params(self, cognite_client, mock_files_response, httpx_mock):
        cognite_client.files.list(data_set_external_ids=["x"], limit=10)
        calls = httpx_mock.get_requests()
        assert 1 == len(calls)
        expected = {
            "limit": 10,
            "filter": {
                "dataSetIds": [{"externalId": "x"}],
            },
        }
        assert expected == jsgz_load(calls[0].content)

    def test_list_subtrees(self, cognite_client, mock_files_response, httpx_mock):
        cognite_client.files.list(asset_subtree_ids=[1], asset_subtree_external_ids=["a"], limit=10)
        calls = httpx_mock.get_requests()
        assert 1 == len(calls)
        expected = {"limit": 10, "filter": {"assetSubtreeIds": [{"id": 1}, {"externalId": "a"}]}}
        assert expected == jsgz_load(calls[0].content)

    def test_filter_directory(self, cognite_client, mock_files_response, httpx_mock):
        cognite_client.files.list(directory_prefix="/test", limit=10)
        calls = httpx_mock.get_requests()
        assert len(calls) == 1
        assert jsgz_load(calls[0].content) == {"filter": {"directoryPrefix": "/test"}, "limit": 10}

    def test_filter_geo_location(self, cognite_client, mock_files_response, httpx_mock):
        cognite_client.files.list(
            geo_location=GeoLocationFilter(relation="within", shape=GeometryFilter(type="Point", coordinates=[35, 10])),
            limit=10,
        )
        calls = httpx_mock.get_requests()
        assert len(calls) == 1
        assert jsgz_load(calls[0].content) == {
            "filter": {"geoLocation": {"relation": "within", "shape": {"type": "Point", "coordinates": [35, 10]}}},
            "limit": 10,
        }

    def test_list_with_time_dict(self, cognite_client, mock_files_response, httpx_mock):
        cognite_client.files.list(created_time={"min": 20})
        assert 20 == jsgz_load(httpx_mock.get_requests()[0].content)["filter"]["createdTime"]["min"]
        assert "max" not in jsgz_load(httpx_mock.get_requests()[0].content)["filter"]["createdTime"]

    def test_list_with_timestamp_range(self, cognite_client, mock_files_response, httpx_mock):
        cognite_client.files.list(created_time=TimestampRange(min=20))
        assert 20 == jsgz_load(httpx_mock.get_requests()[0].content)["filter"]["createdTime"]["min"]
        assert "max" not in jsgz_load(httpx_mock.get_requests()[0].content)["filter"]["createdTime"]

    def test_delete_single(self, cognite_client, mock_files_response, httpx_mock):
        res = cognite_client.files.delete(id=1)
        expected = {"items": [{"id": 1}], "ignoreUnknownIds": False}
        assert expected == jsgz_load(httpx_mock.get_requests()[0].content)
        assert res is None

    def test_delete_multiple(self, cognite_client, mock_files_response, httpx_mock):
        res = cognite_client.files.delete(id=[1])
        assert res is None
        exp = {"items": [{"id": 1}], "ignoreUnknownIds": False}
        assert exp == jsgz_load(httpx_mock.get_requests()[0].content)

    def test_update_with_resource_class(self, cognite_client, mock_files_response, httpx_mock):
        res = cognite_client.files.update(FileMetadata(id=1, source="bla"))
        assert isinstance(res, FileMetadata)
        assert {"items": [{"id": 1, "update": {"source": {"set": "bla"}}}]} == jsgz_load(
            httpx_mock.get_requests()[0].content
        )

    def test_update_with_update_class(self, cognite_client, mock_files_response, httpx_mock):
        res = cognite_client.files.update(FileMetadataUpdate(id=1).source.set("bla"))
        assert isinstance(res, FileMetadata)
        assert {"items": [{"id": 1, "update": {"source": {"set": "bla"}}}]} == jsgz_load(
            httpx_mock.get_requests()[0].content
        )

    def test_update_with_update_class_using_instance_id(self, cognite_client, mock_files_response, httpx_mock):
        res = cognite_client.files.update(FileMetadataUpdate(instance_id=NodeId("foo", "bar")).source.set("bla"))
        assert isinstance(res, FileMetadata)
        assert {
            "items": [{"instanceId": {"space": "foo", "externalId": "bar"}, "update": {"source": {"set": "bla"}}}]
        } == jsgz_load(httpx_mock.get_requests()[0].content)

    @pytest.mark.parametrize("extra_identifiers", (dict(id=1), dict(external_id="a"), dict(id=1, external_id="a")))
    def test_update_with_update_class_using_instance_id_and_other_identifier(
        self, extra_identifiers, cognite_client, mock_files_response, httpx_mock
    ):
        with pytest.raises(ValueError, match="Exactly one of 'id', 'external_id' or 'instance_id' must be provided."):
            FileMetadataUpdate(instance_id=NodeId("foo", "bar"), **extra_identifiers)

    def test_update_labels_single(self, cognite_client, mock_files_response, httpx_mock):
        cognite_client.files.update([FileMetadataUpdate(id=1).labels.add("PUMP").labels.remove("WELL LOG")])
        expected = {"labels": {"add": [{"externalId": "PUMP"}], "remove": [{"externalId": "WELL LOG"}]}}
        assert jsgz_load(httpx_mock.get_requests()[0].content)["items"][0]["update"] == expected

    def test_update_labels_multiple(self, cognite_client, mock_files_response, httpx_mock):
        cognite_client.files.update(
            [FileMetadataUpdate(id=1).labels.add(["PUMP", "ROTATING_EQUIPMENT"]).labels.remove(["WELL LOG"])]
        )
        expected = {
            "labels": {
                "add": [{"externalId": "PUMP"}, {"externalId": "ROTATING_EQUIPMENT"}],
                "remove": [{"externalId": "WELL LOG"}],
            }
        }
        assert jsgz_load(httpx_mock.get_requests()[0].content)["items"][0]["update"] == expected

    def test_update_labels_resource_class(self, cognite_client, mock_files_response, httpx_mock):
        cognite_client.files.update(FileMetadata(id=1, labels=[Label(external_id="Pump")], external_id="newId"))
        expected = {"externalId": {"set": "newId"}, "labels": {"set": [{"externalId": "Pump"}]}}
        assert expected == jsgz_load(httpx_mock.get_requests()[0].content)["items"][0]["update"]

    def test_labels_filter_contains_all(self, cognite_client, mock_files_response, httpx_mock):
        my_label_filter = LabelFilter(contains_all=["WELL LOG", "VERIFIED"])
        cognite_client.files.list(labels=my_label_filter)
        assert jsgz_load(httpx_mock.get_requests()[0].content)["filter"]["labels"] == {
            "containsAll": [{"externalId": "WELL LOG"}, {"externalId": "VERIFIED"}]
        }

    def test_labels_filter_contains_any(self, cognite_client, mock_files_response, httpx_mock):
        my_label_filter = LabelFilter(contains_any=["WELL LOG", "WELL REPORT"])
        cognite_client.files.list(labels=my_label_filter)
        assert jsgz_load(httpx_mock.get_requests()[0].content)["filter"]["labels"] == {
            "containsAny": [{"externalId": "WELL LOG"}, {"externalId": "WELL REPORT"}]
        }

    def test_update_multiple(self, cognite_client, mock_files_response, httpx_mock):
        res = cognite_client.files.update(
            [FileMetadataUpdate(id=1).source.set(None), FileMetadata(external_id="2", source="bla")]
        )
        assert isinstance(res, FileMetadataList)
        assert {
            "items": [
                {"id": 1, "update": {"source": {"setNull": True}}},
                {"externalId": "2", "update": {"source": {"set": "bla"}}},
            ]
        } == jsgz_load(httpx_mock.get_requests()[0].content)

    def test_iter_single(self, cognite_client, mock_files_response, httpx_mock):
        for file in cognite_client.files:
            assert isinstance(file, FileMetadata)
            assert mock_files_response["items"][0] == file.dump(camel_case=True)

    def test_iter_chunk(self, cognite_client, mock_files_response, httpx_mock):
        for file in cognite_client.files(chunk_size=1):
            assert isinstance(file, FileMetadataList)
            assert mock_files_response["items"] == file.dump(camel_case=True)

    def test_search(self, cognite_client, mock_files_response, httpx_mock):
        res = cognite_client.files.search(filter=FileMetadataFilter(external_id_prefix="abc"))
        assert mock_files_response["items"] == res.dump(camel_case=True)
        assert {"search": {"name": None}, "filter": {"externalIdPrefix": "abc"}, "limit": 25} == jsgz_load(
            httpx_mock.get_requests()[0].content
        )

    @pytest.mark.parametrize("filter_field", ["external_id_prefix", "externalIdPrefix"])
    def test_search_dict_filter(self, cognite_client, mock_files_response, filter_field, httpx_mock):
        res = cognite_client.files.search(filter={filter_field: "abc"})
        assert mock_files_response["items"] == res.dump(camel_case=True)
        assert {"search": {"name": None}, "filter": {"externalIdPrefix": "abc"}, "limit": 25} == jsgz_load(
            httpx_mock.get_requests()[0].content
        )

    def test_upload(self, cognite_client, mock_file_upload_response, httpx_mock):
        directory = os.path.join(os.path.dirname(__file__), "files_for_test_upload")
        path = os.path.join(directory, "file_for_test_upload_1.txt")
        res = cognite_client.files.upload(path, name="bla", directory=directory)
        del mock_file_upload_response["uploadUrl"]
        assert FileMetadata.load(mock_file_upload_response) == res
        assert "https://upload.here" == str(httpx_mock.get_requests()[1].url)
        assert {"name": "bla", "directory": directory} == jsgz_load(httpx_mock.get_requests()[0].content)
        assert b"content1\n" == httpx_mock.get_requests()[1].content

    def test_upload_with_external_id(self, cognite_client, mock_file_upload_response):
        path = os.path.join(os.path.dirname(__file__), "files_for_test_upload", "file_for_test_upload_1.txt")
        cognite_client.files.upload(path, external_id="blabla", name="bla", data_set_id=42)

    def test_upload_with_label(self, cognite_client, mock_file_upload_response):
        """
        Uploading a file with a label gave a `ValueError: Could not parse label: {'external_id': 'foo-bar'}` from v7.0.0
        """
        path = os.path.join(os.path.dirname(__file__), "files_for_test_upload", "file_for_test_upload_1.txt")
        cognite_client.files.upload(path, name="bla", labels=[Label("PUMP")])

    def test_upload_no_name(self, cognite_client, mock_file_upload_response, httpx_mock):
        directory = os.path.join(os.path.dirname(__file__), "files_for_test_upload")
        path = os.path.join(directory, "file_for_test_upload_1.txt")
        cognite_client.files.upload(path, directory=directory)
        assert {"name": "file_for_test_upload_1.txt", "directory": directory} == jsgz_load(
            httpx_mock.get_requests()[0].content
        )

    def test_upload_set_directory(self, cognite_client, mock_file_upload_response, httpx_mock):
        set_dir = "/Some/custom/directory"
        directory = os.path.join(os.path.dirname(__file__), "files_for_test_upload")
        path = os.path.join(directory, "file_for_test_upload_1.txt")
        cognite_client.files.upload(path, directory=set_dir)
        assert {"name": "file_for_test_upload_1.txt", "directory": set_dir} == jsgz_load(
            httpx_mock.get_requests()[0].content
        )

    def test_upload_from_directory(self, cognite_client, mock_file_upload_response, httpx_mock):
        httpx_mock.add_response(
            method="POST",
            url=get_url(cognite_client.files) + "/files?overwrite=false",
            status_code=200,
            json=mock_file_upload_response,
        )
        httpx_mock.add_response(method="PUT", url="https://upload.here", status_code=200)

        path = os.path.join(os.path.dirname(__file__), "files_for_test_upload")
        res = cognite_client.files.upload(path=path, asset_ids=[1, 2])
        del mock_file_upload_response["uploadUrl"]
        assert (
            FileMetadataList(
                [FileMetadata.load(mock_file_upload_response), FileMetadata.load(mock_file_upload_response)]
            )
            == res
        )
        assert 4 == len(httpx_mock.get_requests())
        count = 0
        for request in httpx_mock.get_requests():
            if not (payload_compressed := request.content).startswith(b"content"):
                payload = jsgz_load(payload_compressed)
                count += 1
                assert [1, 2] == payload["assetIds"]
                assert payload["name"] in ["file_for_test_upload_1.txt", "file_for_test_upload_2.txt"]
        assert count == 2

    def test_upload_from_directory_fails(self, cognite_client, httpx_mock):
        for _ in range(2):
            httpx_mock.add_response(
                method="POST", url=get_url(cognite_client.files) + "/files?overwrite=false", status_code=400, json={}
            )

        path = os.path.join(os.path.dirname(__file__), "files_for_test_upload")
        with pytest.raises(CogniteAPIError) as e:
            cognite_client.files.upload(path=path)

        assert "file_for_test_upload_1.txt" in e.value.failed
        assert "file_for_test_upload_2.txt" in e.value.failed

    def test_upload_from_directory_recursively(self, cognite_client, httpx_mock, example_file):
        for _ in range(3):
            httpx_mock.add_response(method="PUT", url="https://upload.here", status_code=200)
            httpx_mock.add_response(
                method="POST",
                url=get_url(cognite_client.files) + "/files?overwrite=false",
                status_code=200,
                json=example_file,
            )

        path = os.path.join(os.path.dirname(__file__), "files_for_test_upload")
        res = cognite_client.files.upload(path=path, recursive=True, asset_ids=[1, 2])
        del example_file["uploadUrl"]
        assert FileMetadataList([FileMetadata.load(example_file) for _ in range(3)]) == res
        assert 6 == len(httpx_mock.get_requests())
        count = 0
        for request in httpx_mock.get_requests():
            if not (payload_compressed := request.content).startswith(b"content"):
                payload = jsgz_load(payload_compressed)
                count += 1
                assert payload["name"] in [
                    "file_for_test_upload_1.txt",
                    "file_for_test_upload_2.txt",
                    "file_for_test_upload_3.txt",
                ]
                assert [1, 2] == payload["assetIds"]
        assert count == 3

    def test_upload_from_memory(self, cognite_client, mock_file_upload_response, httpx_mock):
        res = cognite_client.files.upload_bytes(content=b"content", name="bla")
        del mock_file_upload_response["uploadUrl"]
        assert FileMetadata.load(mock_file_upload_response) == res
        assert "https://upload.here" == httpx_mock.get_requests()[1].url
        assert {"name": "bla"} == jsgz_load(httpx_mock.get_requests()[0].content)
        assert b"content" == httpx_mock.get_requests()[1].content

    def test_upload_with_netloc(self, cognite_client, mock_file_upload_response, httpx_mock):
        """When uploading a file, the upload URL should be used as-is if it contains a netloc."""
        cognite_client.files.upload_bytes(content=b"content", name="bla")
        assert httpx_mock.get_requests()[1].url == "https://upload.here"

    def test_upload_without_netloc(
        self, cognite_client, mock_file_upload_response_without_netloc_in_upload_url, httpx_mock
    ):
        """When uploading a file, the upload URL should be appended to the base URL if it does not contain a netloc."""
        cognite_client.files.upload_bytes(content=b"content", name="bla")
        assert httpx_mock.get_requests()[1].url == "https://api.cognitedata.com/upload/here/to/some/path"

    def test_upload_using_file_handle(self, cognite_client, mock_file_upload_response, httpx_mock):
        path = os.path.join(os.path.dirname(__file__), "files_for_test_upload", "file_for_test_upload_1.txt")
        with open(path, "rb") as fh:
            res = cognite_client.files.upload_bytes(fh, name="bla")
        del mock_file_upload_response["uploadUrl"]
        assert FileMetadata.load(mock_file_upload_response) == res
        assert "https://upload.here" == httpx_mock.get_requests()[1].url
        assert {"name": "bla"} == jsgz_load(httpx_mock.get_requests()[0].content)
        assert b"content1\n" == httpx_mock.get_requests()[1].content

    def test_upload_path_does_not_exist(self, cognite_client):
        with pytest.raises(ValueError, match="does not exist"):
            cognite_client.files.upload(path="/no/such/path")

    def test_download(self, cognite_client, mock_file_download_response):
        with TemporaryDirectory() as tmpdir:  # TODO: Use tmp_path?
            res = cognite_client.files.download(directory=tmpdir, id=[1], external_id=["2"])
            assert {"ignoreUnknownIds": False, "items": [{"id": 1}, {"externalId": "2"}]} == jsgz_load(
                mock_file_download_response.get_requests()[0].content
            )
            assert res is None
            fp1 = os.path.join(tmpdir, "file1")
            fp2 = os.path.join(tmpdir, "file2")
            assert os.path.isfile(fp1)
            assert os.path.isfile(fp2)
            with open(fp1, "rb") as fh:
                assert b"content1" == fh.read()
            with open(fp2, "rb") as fh:
                assert b"content2" == fh.read()

    @pytest.mark.parametrize(
        "input_list,expected_output_list",
        [
            (["a.txt", "a.txt"], ["a.txt", "a(1).txt"]),
            (["a.txt", "a.txt", "a(1).txt"], ["a.txt", "a(2).txt", "a(1).txt"]),
            (["a.txt", "file", "a(1).txt", "a.txt", "file"], ["a.txt", "file", "a(1).txt", "a(2).txt", "file(1)"]),
            (
                [
                    str(Path("posixfolder/a.txt")),
                    str(Path("posixfolder/a.txt")),
                    str(Path(r"winfolder\a.txt")),
                    str(Path(r"winfolder\a.txt")),
                ],
                [
                    str(Path("posixfolder/a.txt")),
                    str(Path("posixfolder/a(1).txt")),
                    str(Path(r"winfolder\a.txt")),
                    str(Path(r"winfolder\a(1).txt")),
                ],
            ),
            (
                [str(Path("folder/sub.folder/arch.tar.gz")), str(Path("folder/sub.folder/arch.tar.gz"))],
                [str(Path("folder/sub.folder/arch.tar.gz")), str(Path("folder/sub.folder/arch(1).tar.gz"))],
            ),
        ],
    )
    def test_create_unique_file_names(self, cognite_client, input_list, expected_output_list):
        assert cognite_client.files._create_unique_file_names(input_list) == expected_output_list

    def test_download_with_duplicate_names(
        self, tmp_path, cognite_client, mock_file_download_response_with_folder_structure_same_name
    ):
        cognite_client.files.download(
            directory=tmp_path,
            id=[1],
            external_id=["2"],
            keep_directory_structure=False,
            resolve_duplicate_file_names=True,
        )
        assert {"ignoreUnknownIds": False, "items": [{"id": 1}, {"externalId": "2"}]} == jsgz_load(
            mock_file_download_response_with_folder_structure_same_name.get_requests()[0].content
        )
        fp1 = tmp_path / "file_a"
        fp2 = tmp_path / "file_a(1)"
        assert fp1.is_file()
        assert fp2.is_file()

    def test_download_with_folder_structure(
        self, tmp_path, cognite_client, mock_file_download_response_with_folder_structure_same_name
    ):
        cognite_client.files.download(directory=tmp_path, id=[1], external_id=["2"], keep_directory_structure=True)
        assert {"ignoreUnknownIds": False, "items": [{"id": 1}, {"externalId": "2"}]} == jsgz_load(
            mock_file_download_response_with_folder_structure_same_name.get_requests()[0].content
        )
        fp1 = tmp_path / "rootdir/subdir/file_a"
        fp2 = tmp_path / "file_a"
        assert fp1.is_file()
        assert fp2.is_file()
        assert fp1.read_text() == "contentSubDir"
        assert fp2.read_text() == "contentNoDir"

    @pytest.fixture
    def mock_byids_response__file_with_double_dots(self, httpx_mock, cognite_client):
        filename = "../file1"
        httpx_mock.add_response(
            method="POST",
            url=get_url(cognite_client.files) + "/files/byids",
            status_code=200,
            json={"items": [{"id": 1, "name": filename}]},
        )
        yield httpx_mock

    def test_download_file_outside_download_directory(self, cognite_client, mock_byids_response__file_with_double_dots):
        with TemporaryDirectory() as directory:
            with pytest.raises(RuntimeError, match="not inside download directory"):
                cognite_client.files.download(directory=directory, id=[1])

    def test_download_one_file_fails(self, cognite_client, mock_file_download_response_one_fails):
        with TemporaryDirectory() as directory:
            with pytest.raises(CogniteAPIError) as e:
                cognite_client.files.download(directory=directory, id=[1], external_id="fail")
            assert [FileMetadata(id=1, name="file1", external_id="success")] == e.value.successful
            assert [FileMetadata(id=2, name="file2", external_id="fail")] == e.value.failed
            assert os.path.isfile(os.path.join(directory, "file1"))

    def test_download_file_to_path(self, cognite_client, mock_file_download_response):
        mock_file_download_response.assert_all_requests_are_fired = False
        with TemporaryDirectory() as directory:
            file_path = os.path.join(directory, "my_downloaded_file.txt")
            res = cognite_client.files.download_to_path(path=file_path, id=1)
            assert res is None
            assert os.path.isfile(file_path)
            with open(file_path, "rb") as f:
                assert b"content1" == f.read()

    def test_download_to_memory(self, cognite_client, mock_file_download_response):
        mock_file_download_response.assert_all_requests_are_fired = False
        res = cognite_client.files.download_bytes(id=1)
        assert {"items": [{"id": 1}]} == jsgz_load(mock_file_download_response.get_requests()[0].content)
        assert res == b"content1"

    def test_download_ids_over_limit(self, cognite_client, mock_file_download_response):
        with set_request_limit(cognite_client.files, 1):
            with TemporaryDirectory() as directory:
                res = cognite_client.files.download(directory=directory, id=[1], external_id=["2"])
                assert res is None
                bodies = [jsgz_load(req.content) for req in mock_file_download_response.get_requests()[:2]]
                assert {"ignoreUnknownIds": False, "items": [{"id": 1}]} in bodies
                assert {"ignoreUnknownIds": False, "items": [{"externalId": "2"}]} in bodies
                assert os.path.isfile(os.path.join(directory, "file1"))
                assert os.path.isfile(os.path.join(directory, "file2"))

    def test_files_update_object(self):
        update = (
            FileMetadataUpdate(1)
            .asset_ids.add([])
            .asset_ids.remove([])
            .external_id.set("1")
            .external_id.set(None)
            .directory.set("/some/new/directory")
            .metadata.add({})
            .metadata.remove([])
            .labels.add(["WELL LOG"])
            .labels.remove(["CV"])
            .geo_location.set(mock_geo_location)
            .geo_location.set(None)
            .source.set(1)
            .source.set(None)
        )
        assert isinstance(update, FileMetadataUpdate)

    @pytest.mark.parametrize(
        ["data_set_id", "api_error", "expected_error", "expected_error_message"],
        [
            (
                12345,
                CogniteAPIError(
                    message="Resource not found. This may also be due to insufficient access rights.",
                    code=403,
                    x_request_id="abc123",
                ),
                CogniteAuthorizationError,
                "Could not create a file due to insufficient access rights.",
            ),
            (
                None,
                CogniteAPIError(
                    message="Resource not found. This may also be due to insufficient access rights.",
                    code=403,
                    x_request_id="abc123",
                ),
                CogniteAuthorizationError,
                "Could not create a file due to insufficient access rights. Try to provide a data_set_id.",
            ),
            (
                12345,
                CogniteAPIError(message="Bad request.", code=400, x_request_id="abc123"),
                CogniteAPIError,
                "Bad request.",
            ),
            (
                None,
                CogniteAPIError(message="Bad request.", code=400, x_request_id="abc123"),
                CogniteAPIError,
                "Bad request.",
            ),
        ],
    )
    def test_upload_bytes_post_error(
        self,
        cognite_client,
        data_set_id: int,
        api_error: CogniteAPIError,
        expected_error: type[CogniteAPIError],
        expected_error_message: str,
    ):
        def raise_api_error(*args, **kwargs):
            raise api_error

        cognite_client.files._post = raise_api_error

        with pytest.raises(expected_error) as e:
            cognite_client.files.upload_bytes(content=b"content", name="bla", data_set_id=data_set_id)

        assert e.value.message == expected_error_message
        assert e.value.code == api_error.code
        assert e.value.x_request_id == api_error.x_request_id


@pytest.fixture
def mock_files_empty(httpx_mock, cognite_client):
    url_pattern = re.compile(re.escape(get_url(cognite_client.files)) + "/.+")
    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json={"items": []})
    yield httpx_mock


@pytest.mark.dsl
class TestPandasIntegration:
    def test_file_list_to_pandas(self, cognite_client, mock_files_response):
        import pandas as pd

        df = cognite_client.files.list().to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert 1 == df.shape[0]
        assert {"metadata-key": "metadata-value"} == df["metadata"][0]

    def test_file_list_to_pandas_empty(self, cognite_client, mock_files_empty):
        import pandas as pd

        df = cognite_client.files.list().to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_file_to_pandas(self, cognite_client, mock_files_response):
        import pandas as pd

        df = cognite_client.files.retrieve(id=1).to_pandas(expand_metadata=True, metadata_prefix="", camel_case=True)
        assert isinstance(df, pd.DataFrame)
        assert "metadata" not in df.columns
        assert [1] == df.at["assetIds", "value"]
        assert "metadata-value" == df.at["metadata-key", "value"]
