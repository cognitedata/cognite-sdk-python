from __future__ import annotations

import json
import os
import re
from io import BufferedReader
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from httpx import Request as HttpxRequest
from httpx import Response as HttpxResponse

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
from tests.utils import jsgz_load, set_request_limit


@pytest.fixture
def mock_geo_location() -> GeoLocation:
    geometry = Geometry(type="Point", coordinates=[35, 10])
    yield GeoLocation(type="Feature", geometry=geometry)


@pytest.fixture
def mock_files_response(respx_mock, cognite_client, mock_geo_location: GeoLocation):
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

    url_pattern = re.compile(re.escape(cognite_client.files._get_base_url_with_base_path()) + "/.+")
    
    respx_mock.post(url__regex=url_pattern).respond(status_code=200, json=response_body)
    respx_mock.get(url__regex=url_pattern).respond(status_code=200, json=response_body)
    yield respx_mock


@pytest.fixture
def mock_file_upload_response(respx_mock, cognite_client, mock_geo_location: GeoLocation):
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
        "uploadUrl": "https://upload.here/",
    }
    respx_mock.post(cognite_client.files._get_base_url_with_base_path() + "/files").respond(status_code=200, json=response_body)
    respx_mock.put("https://upload.here/").respond(status_code=200) 
    yield respx_mock


@pytest.fixture
def mock_file_upload_response_without_netloc_in_upload_url(respx_mock, cognite_client, mock_geo_location: GeoLocation):
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
        "uploadUrl": "upload/here/to/some/path", # Relative URL
    }
    respx_mock.post(cognite_client.files._get_base_url_with_base_path() + "/files").respond(status_code=200, json=response_body)
    upload_url_full = cognite_client.config.base_url.rstrip('/') + "/" + response_body["uploadUrl"].lstrip('/')
    respx_mock.put(upload_url_full).respond(status_code=200)
    yield respx_mock


@pytest.fixture
def mock_file_create_response(respx_mock, cognite_client, mock_geo_location: GeoLocation):
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
        "uploadUrl": "https://upload.here/",
    }
    respx_mock.post(cognite_client.files._get_base_url_with_base_path() + "/files").respond(status_code=200, json=response_body)
    yield respx_mock


@pytest.fixture
def mock_file_download_response(respx_mock, cognite_client):
    respx_mock.post(
        cognite_client.files._get_base_url_with_base_path() + "/files/byids"
    ).respond(
        status_code=200,
        json={"items": [{"id": 1, "name": "file1"}, {"id": 10, "externalId": "2", "name": "file2"}]},
    )

    def download_link_callback_side_effect(request: HttpxRequest):
        identifier = jsgz_load(request.content)["items"][0]
        response_json = {}
        if identifier.get("id") == 1:
            response_json = {"items": [{"id": 1, "downloadUrl": "https://download.file1.here/"}]} 
        if identifier.get("id") == 10:
            response_json = {"items": [{"id": 10, "externalId": "2", "downloadUrl": "https://download.file2.here/"}]} 
        return HttpxResponse(200, json=response_json)

    respx_mock.post(
        cognite_client.files._get_base_url_with_base_path() + "/files/downloadlink"
    ).mock(side_effect=download_link_callback_side_effect)
    
    respx_mock.get("https://download.file1.here/").respond(status_code=200, content=b"content1") 
    respx_mock.get("https://download.file2.here/").respond(status_code=200, content=b"content2") 
    yield respx_mock


@pytest.fixture
def mock_file_download_response_with_folder_structure_same_name(respx_mock, cognite_client):
    respx_mock.post(
        cognite_client.files._get_base_url_with_base_path() + "/files/byids"
    ).respond(
        status_code=200,
        json={
            "items": [
                {"id": 1, "name": "file_a", "directory": "/rootdir/subdir"},
                {"id": 10, "externalId": "2", "name": "file_a"},
            ]
        },
    )

    def download_link_callback_side_effect(request: HttpxRequest):
        identifier = jsgz_load(request.content)["items"][0]
        response_json = {}
        if identifier.get("id") == 1:
            response_json = {"items": [{"id": 1, "downloadUrl": "https://download.fileFromSubdir.here/"}]}
        if identifier.get("id") == 10:
            response_json = {"items": [{"id": 10, "externalId": "2", "downloadUrl": "https://download.fileNoDir.here/"}]}
        return HttpxResponse(200, json=response_json)

    respx_mock.post(
        cognite_client.files._get_base_url_with_base_path() + "/files/downloadlink"
    ).mock(side_effect=download_link_callback_side_effect)
    
    respx_mock.get("https://download.fileFromSubdir.here/").respond(status_code=200, content=b"contentSubDir")
    respx_mock.get("https://download.fileNoDir.here/").respond(status_code=200, content=b"contentNoDir")
    yield respx_mock


@pytest.fixture
def mock_file_download_response_one_fails(respx_mock, cognite_client):
    respx_mock.post(
        cognite_client.files._get_base_url_with_base_path() + "/files/byids"
    ).respond(
        status_code=200,
        json={
            "items": [
                {"id": 1, "externalId": "success", "name": "file1"},
                {"externalId": "fail", "id": 2, "name": "file2"},
            ]
        },
    )

    def download_link_callback_side_effect(request: HttpxRequest):
        identifier = jsgz_load(request.content)["items"][0]
        if identifier.get("id") == 1:
            return HttpxResponse(200, json={"items": [{"id": 1, "downloadUrl": "https://download.file1.here/"}]})
        elif identifier.get("id") == 2:
            return HttpxResponse(400, json={"error": {"message": "User error", "code": 400}})
        return HttpxResponse(404) 

    respx_mock.post(
        cognite_client.files._get_base_url_with_base_path() + "/files/downloadlink"
    ).mock(side_effect=download_link_callback_side_effect)
    respx_mock.get("https://download.file1.here/").respond(status_code=200, content=b"content1")
    yield respx_mock


class TestFilesAPI:
    def test_create(self, cognite_client, mock_file_create_response):
        file_metadata = FileMetadata(name="bla")
        returned_file_metadata, upload_url = cognite_client.files.create(file_metadata)
        response_body = mock_file_create_response.calls.last.response.json()
        assert FileMetadata.load(response_body) == returned_file_metadata
        assert response_body["uploadUrl"] == upload_url

    def test_create_with_label(self, cognite_client, mock_file_create_response):
        file_metadata = FileMetadata(name="bla", labels=[Label(external_id="WELL LOG")])
        returned_file_metadata, upload_url = cognite_client.files.create(file_metadata)
        response_body = mock_file_create_response.calls.last.response.json()
        assert FileMetadata.load(response_body) == returned_file_metadata
        assert response_body["uploadUrl"] == upload_url
        assert response_body["labels"][0]["externalId"] == "WELL LOG"

    def test_create_with_label_request(self, cognite_client, mock_file_create_response):
        file_metadata = FileMetadata(name="bla", labels=[Label(external_id="WELL LOG")])
        returned_file_metadata, upload_url = cognite_client.files.create(file_metadata)
        response_body = mock_file_create_response.calls.last.response.json()
        request_body = jsgz_load(mock_file_create_response.calls.last.request.content)
        assert FileMetadata.load(response_body) == returned_file_metadata
        assert all(body["labels"][0]["externalId"] == "WELL LOG" for body in [request_body, response_body])

    def test_create_with_geoLocation(self, cognite_client, mock_file_create_response, mock_geo_location):
        file_metadata = FileMetadata(name="bla", geo_location=mock_geo_location)
        returned_file_metadata, upload_url = cognite_client.files.create(file_metadata)
        response_body = mock_file_create_response.calls.last.response.json()
        assert FileMetadata.load(response_body) == returned_file_metadata
        assert response_body["geoLocation"] == mock_geo_location.dump(camel_case=True)

    def test_create_geoLocation_with_invalid_geometry_type(self):
        with pytest.raises(ValueError):
            _ = Geometry(type="someInvalidType", coordinates=[1, 2])

    def test_create_geoLocation_with_invalid_geojson_type(self):
        g = Geometry(type="Point", coordinates=[1, 2])
        with pytest.raises(ValueError):
            _ = GeoLocation(type="FeatureCollection", geometry=g)

    def test_create_with_geoLocation_request(self, cognite_client, mock_file_create_response, mock_geo_location):
        file_metadata = FileMetadata(name="bla", geo_location=mock_geo_location)
        returned_file_metadata, upload_url = cognite_client.files.create(file_metadata)
        response_body = mock_file_create_response.calls.last.response.json()
        request_body = jsgz_load(mock_file_create_response.calls.last.request.content)
        assert FileMetadata.load(response_body) == returned_file_metadata
        assert all(
            body["geoLocation"] == mock_geo_location.dump(camel_case=True) for body in [request_body, response_body]
        )

    def test_retrieve_single(self, cognite_client, mock_files_response):
        res = cognite_client.files.retrieve(id=1)
        assert isinstance(res, FileMetadata)
        assert mock_files_response.calls.last.response.json()["items"][0] == res.dump(camel_case=True)

    def test_retrieve_multiple(self, cognite_client, mock_files_response):
        res = cognite_client.files.retrieve_multiple(ids=[1])
        assert isinstance(res, FileMetadataList)
        assert mock_files_response.calls.last.response.json()["items"] == res.dump(camel_case=True)

    def test_list(self, cognite_client, mock_files_response):
        res = cognite_client.files.list(source="bla", limit=10)
        assert isinstance(res, FileMetadataList)
        assert mock_files_response.calls.last.response.json()["items"] == res.dump(camel_case=True)
        assert "bla" == jsgz_load(mock_files_response.calls.last.request.content)["filter"]["source"]
        assert 10 == jsgz_load(mock_files_response.calls.last.request.content)["limit"]

    def test_list_params(self, cognite_client, mock_files_response):
        cognite_client.files.list(data_set_external_ids=["x"], limit=10)
        calls = mock_files_response.calls
        assert 1 == len(calls)
        assert {
            "cursor": None,
            "limit": 10,
            "filter": {
                "dataSetIds": [{"externalId": "x"}],
            },
        } == jsgz_load(calls.last.request.content)

    def test_list_subtrees(self, cognite_client, mock_files_response):
        cognite_client.files.list(asset_subtree_ids=[1], asset_subtree_external_ids=["a"], limit=10)
        calls = mock_files_response.calls
        assert 1 == len(calls)
        assert {
            "cursor": None,
            "limit": 10,
            "filter": {"assetSubtreeIds": [{"id": 1}, {"externalId": "a"}]},
        } == jsgz_load(calls.last.request.content)

    def test_filter_directory(self, cognite_client, mock_files_response):
        cognite_client.files.list(directory_prefix="/test", limit=10)
        calls = mock_files_response.calls
        assert len(calls) == 1
        assert jsgz_load(calls.last.request.content) == {"cursor": None, "filter": {"directoryPrefix": "/test"}, "limit": 10}

    def test_filter_geoLocation(self, cognite_client, mock_files_response):
        cognite_client.files.list(
            geo_location=GeoLocationFilter(relation="within", shape=GeometryFilter(type="Point", coordinates=[35, 10])),
            limit=10,
        )
        calls = mock_files_response.calls
        assert len(calls) == 1
        assert jsgz_load(calls.last.request.content) == {
            "cursor": None,
            "filter": {"geoLocation": {"relation": "within", "shape": {"type": "Point", "coordinates": [35, 10]}}},
            "limit": 10,
        }

    def test_list_with_time_dict(self, cognite_client, mock_files_response):
        cognite_client.files.list(created_time={"min": 20})
        assert 20 == jsgz_load(mock_files_response.calls.last.request.content)["filter"]["createdTime"]["min"]
        assert "max" not in jsgz_load(mock_files_response.calls.last.request.content)["filter"]["createdTime"]

    def test_list_with_timestamp_range(self, cognite_client, mock_files_response):
        cognite_client.files.list(created_time=TimestampRange(min=20))
        assert 20 == jsgz_load(mock_files_response.calls.last.request.content)["filter"]["createdTime"]["min"]
        assert "max" not in jsgz_load(mock_files_response.calls.last.request.content)["filter"]["createdTime"]

    def test_delete_single(self, cognite_client, mock_files_response):
        res = cognite_client.files.delete(id=1)
        assert {"items": [{"id": 1}], "ignoreUnknownIds": False} == jsgz_load(mock_files_response.calls.last.request.content)
        assert res is None

    def test_delete_multiple(self, cognite_client, mock_files_response):
        res = cognite_client.files.delete(id=[1])
        assert {"items": [{"id": 1}], "ignoreUnknownIds": False} == jsgz_load(mock_files_response.calls.last.request.content)
        assert res is None

    def test_update_with_resource_class(self, cognite_client, mock_files_response):
        res = cognite_client.files.update(FileMetadata(id=1, source="bla"))
        assert isinstance(res, FileMetadata)
        assert {"items": [{"id": 1, "update": {"source": {"set": "bla"}}}]} == jsgz_load(
            mock_files_response.calls.last.request.content
        )

    def test_update_with_update_class(self, cognite_client, mock_files_response):
        res = cognite_client.files.update(FileMetadataUpdate(id=1).source.set("bla"))
        assert isinstance(res, FileMetadata)
        assert {"items": [{"id": 1, "update": {"source": {"set": "bla"}}}]} == jsgz_load(
            mock_files_response.calls.last.request.content
        )

    def test_update_with_update_class_using_instance_id(self, cognite_client, mock_files_response):
        res = cognite_client.files.update(FileMetadataUpdate(instance_id=NodeId("foo", "bar")).source.set("bla"))
        assert isinstance(res, FileMetadata)
        assert {
            "items": [{"instanceId": {"space": "foo", "externalId": "bar"}, "update": {"source": {"set": "bla"}}}]
        } == jsgz_load(mock_files_response.calls.last.request.content)

    @pytest.mark.parametrize("extra_identifiers", (dict(id=1), dict(external_id="a"), dict(id=1, external_id="a")))
    def test_update_with_update_class_using_instance_id_and_other_identifier(
        self, extra_identifiers, cognite_client, mock_files_response
    ):
        with pytest.raises(ValueError, match="Exactly one of 'id', 'external_id' or 'instance_id' must be provided."):
            FileMetadataUpdate(instance_id=NodeId("foo", "bar"), **extra_identifiers)

    def test_update_labels_single(self, cognite_client, mock_files_response):
        cognite_client.files.update([FileMetadataUpdate(id=1).labels.add("PUMP").labels.remove("WELL LOG")])
        expected = {"labels": {"add": [{"externalId": "PUMP"}], "remove": [{"externalId": "WELL LOG"}]}}
        assert jsgz_load(mock_files_response.calls.last.request.content)["items"][0]["update"] == expected

    def test_update_labels_multiple(self, cognite_client, mock_files_response):
        cognite_client.files.update(
            [FileMetadataUpdate(id=1).labels.add(["PUMP", "ROTATING_EQUIPMENT"]).labels.remove(["WELL LOG"])]
        )
        expected = {
            "labels": {
                "add": [{"externalId": "PUMP"}, {"externalId": "ROTATING_EQUIPMENT"}],
                "remove": [{"externalId": "WELL LOG"}],
            }
        }
        assert jsgz_load(mock_files_response.calls.last.request.content)["items"][0]["update"] == expected

    def test_update_labels_resource_class(self, cognite_client, mock_files_response):
        cognite_client.files.update(FileMetadata(id=1, labels=[Label(external_id="Pump")], external_id="newId"))
        expected = {"externalId": {"set": "newId"}, "labels": {"set": [{"externalId": "Pump"}]}}
        assert expected == jsgz_load(mock_files_response.calls.last.request.content)["items"][0]["update"]

    def test_labels_filter_contains_all(self, cognite_client, mock_files_response):
        my_label_filter = LabelFilter(contains_all=["WELL LOG", "VERIFIED"])
        cognite_client.files.list(labels=my_label_filter)
        assert jsgz_load(mock_files_response.calls.last.request.content)["filter"]["labels"] == {
            "containsAll": [{"externalId": "WELL LOG"}, {"externalId": "VERIFIED"}]
        }

    def test_labels_filter_contains_any(self, cognite_client, mock_files_response):
        my_label_filter = LabelFilter(contains_any=["WELL LOG", "WELL REPORT"])
        cognite_client.files.list(labels=my_label_filter)
        assert jsgz_load(mock_files_response.calls.last.request.content)["filter"]["labels"] == {
            "containsAny": [{"externalId": "WELL LOG"}, {"externalId": "WELL REPORT"}]
        }

    def test_update_multiple(self, cognite_client, mock_files_response):
        res = cognite_client.files.update(
            [FileMetadataUpdate(id=1).source.set(None), FileMetadata(external_id="2", source="bla")]
        )
        assert isinstance(res, FileMetadataList)
        assert {
            "items": [
                {"id": 1, "update": {"source": {"setNull": True}}},
                {"externalId": "2", "update": {"source": {"set": "bla"}}},
            ]
        } == jsgz_load(mock_files_response.calls.last.request.content)

    def test_iter_single(self, cognite_client, mock_files_response):
        for file in cognite_client.files:
            assert isinstance(file, FileMetadata)
            assert mock_files_response.calls.last.response.json()["items"][0] == file.dump(camel_case=True)

    def test_iter_chunk(self, cognite_client, mock_files_response):
        for file in cognite_client.files(chunk_size=1):
            assert isinstance(file, FileMetadataList)
            assert mock_files_response.calls.last.response.json()["items"] == file.dump(camel_case=True)

    def test_search(self, cognite_client, mock_files_response):
        res = cognite_client.files.search(filter=FileMetadataFilter(external_id_prefix="abc"))
        assert mock_files_response.calls.last.response.json()["items"] == res.dump(camel_case=True)
        assert {"search": {"name": None}, "filter": {"externalIdPrefix": "abc"}, "limit": 25} == jsgz_load(
            mock_files_response.calls.last.request.content
        )

    @pytest.mark.parametrize("filter_field", ["external_id_prefix", "externalIdPrefix"])
    def test_search_dict_filter(self, cognite_client, mock_files_response, filter_field):
        res = cognite_client.files.search(filter={filter_field: "abc"})
        assert mock_files_response.calls.last.response.json()["items"] == res.dump(camel_case=True)
        assert {"search": {"name": None}, "filter": {"externalIdPrefix": "abc"}, "limit": 25} == jsgz_load(
            mock_files_response.calls.last.request.content
        )

    def test_upload(self, cognite_client, mock_file_upload_response):
        dir_path = os.path.join(os.path.dirname(__file__), "files_for_test_upload")
        path = os.path.join(dir_path, "file_for_test_upload_1.txt")
        res = cognite_client.files.upload(path, name="bla", directory=dir_path)
        response_body = mock_file_upload_response.calls.nth(0).response.json()
        del response_body["uploadUrl"]
        assert FileMetadata.load(response_body) == res
        assert "https://upload.here/" == str(mock_file_upload_response.calls.nth(1).request.url)
        assert {"name": "bla", "directory": dir_path} == jsgz_load(mock_file_upload_response.calls.nth(0).request.content)
        assert isinstance(mock_file_upload_response.calls.nth(1).request.content, bytes)

    def test_upload_with_external_id(self, cognite_client, mock_file_upload_response):
        path = os.path.join(os.path.dirname(__file__), "files_for_test_upload", "file_for_test_upload_1.txt")
        cognite_client.files.upload(path, external_id="blabla", name="bla", data_set_id=42)

    def test_upload_with_label(self, cognite_client, mock_file_upload_response):
        path = os.path.join(os.path.dirname(__file__), "files_for_test_upload", "file_for_test_upload_1.txt")
        cognite_client.files.upload(path, name="bla", labels=[Label("PUMP")])

    def test_upload_no_name(self, cognite_client, mock_file_upload_response):
        dir_path = os.path.join(os.path.dirname(__file__), "files_for_test_upload")
        path = os.path.join(dir_path, "file_for_test_upload_1.txt")
        cognite_client.files.upload(path, directory=dir_path)
        assert {"name": "file_for_test_upload_1.txt", "directory": dir_path} == jsgz_load(
            mock_file_upload_response.calls.nth(0).request.content
        )

    def test_upload_set_directory(self, cognite_client, mock_file_upload_response):
        set_dir = "/Some/custom/directory"
        dir_path = os.path.join(os.path.dirname(__file__), "files_for_test_upload")
        path = os.path.join(dir_path, "file_for_test_upload_1.txt")
        cognite_client.files.upload(path, directory=set_dir)
        assert {"name": "file_for_test_upload_1.txt", "directory": set_dir} == jsgz_load(
            mock_file_upload_response.calls.nth(0).request.content
        )

    def test_upload_from_directory(self, cognite_client, mock_file_upload_response):
        path = os.path.join(os.path.dirname(__file__), "files_for_test_upload")
        res = cognite_client.files.upload(path=path, asset_ids=[1, 2])
        response_body = mock_file_upload_response.calls.nth(0).response.json()
        del response_body["uploadUrl"]
        assert FileMetadataList([FileMetadata.load(response_body), FileMetadata.load(response_body)]) == res
        assert 4 == len(mock_file_upload_response.calls)
        for call in mock_file_upload_response.calls:
            if call.request.method == "POST": 
                json_payload = jsgz_load(call.request.content)
                assert [1, 2] == json_payload["assetIds"]
                assert json_payload["name"] in ["file_for_test_upload_1.txt", "file_for_test_upload_2.txt"]
            elif call.request.method == "PUT":
                assert isinstance(call.request.content, bytes)


    def test_upload_from_directory_fails(self, cognite_client, respx_mock): 
        respx_mock.post(cognite_client.files._get_base_url_with_base_path() + "/files").respond(status_code=400, json={})
        path = os.path.join(os.path.dirname(__file__), "files_for_test_upload")
        with pytest.raises(CogniteAPIError) as e:
            cognite_client.files.upload(path=path)
        assert "file_for_test_upload_1.txt" in e.value.failed
        assert "file_for_test_upload_2.txt" in e.value.failed

    def test_upload_from_directory_recursively(self, cognite_client, mock_file_upload_response):
        path = os.path.join(os.path.dirname(__file__), "files_for_test_upload")
        res = cognite_client.files.upload(path=path, recursive=True, asset_ids=[1, 2])
        response_body = mock_file_upload_response.calls.nth(0).response.json()
        del response_body["uploadUrl"]
        assert FileMetadataList([FileMetadata.load(response_body) for _ in range(3)]) == res
        assert 6 == len(mock_file_upload_response.calls)
        for call in mock_file_upload_response.calls:
            if call.request.method == "POST":
                json_payload = jsgz_load(call.request.content)
                assert json_payload["name"] in [
                    "file_for_test_upload_1.txt",
                    "file_for_test_upload_2.txt",
                    "file_for_test_upload_3.txt",
                ]
                assert [1, 2] == json_payload["assetIds"]
            elif call.request.method == "PUT":
                 assert isinstance(call.request.content, bytes)


    def test_upload_from_memory(self, cognite_client, mock_file_upload_response):
        res = cognite_client.files.upload_bytes(content=b"content", name="bla")
        response_body = mock_file_upload_response.calls.nth(0).response.json()
        del response_body["uploadUrl"]
        assert FileMetadata.load(response_body) == res
        assert "https://upload.here/" == str(mock_file_upload_response.calls.nth(1).request.url)
        assert {"name": "bla"} == jsgz_load(mock_file_upload_response.calls.nth(0).request.content)
        assert b"content" == mock_file_upload_response.calls.nth(1).request.content

    def test_upload_with_netloc(self, cognite_client, mock_file_upload_response):
        _ = cognite_client.files.upload_bytes(content=b"content", name="bla")
        assert str(mock_file_upload_response.calls.nth(1).request.url) == "https://upload.here/"

    def test_upload_without_netloc(self, cognite_client, mock_file_upload_response_without_netloc_in_upload_url):
        _ = cognite_client.files.upload_bytes(content=b"content", name="bla")
        expected_upload_url = cognite_client.config.base_url.rstrip('/') + "/upload/here/to/some/path"
        assert str(mock_file_upload_response_without_netloc_in_upload_url.calls.nth(1).request.url) == expected_upload_url

    def test_upload_using_file_handle(self, cognite_client, mock_file_upload_response):
        path = os.path.join(os.path.dirname(__file__), "files_for_test_upload", "file_for_test_upload_1.txt")
        with open(path, "rb") as fh:
            res = cognite_client.files.upload_bytes(fh, name="bla")
        response_body = mock_file_upload_response.calls.nth(0).response.json()
        del response_body["uploadUrl"]
        assert FileMetadata.load(response_body) == res
        assert "https://upload.here/" == str(mock_file_upload_response.calls.nth(1).request.url)
        assert {"name": "bla"} == jsgz_load(mock_file_upload_response.calls.nth(0).request.content)
        assert isinstance(mock_file_upload_response.calls.nth(1).request.content, bytes)

    def test_upload_path_does_not_exist(self, cognite_client):
        with pytest.raises(ValueError, match="does not exist"):
            cognite_client.files.upload(path="/no/such/path")

    def test_download(self, cognite_client, mock_file_download_response):
        with TemporaryDirectory() as dir_path: 
            res = cognite_client.files.download(directory=dir_path, id=[1], external_id=["2"])
            assert {"ignoreUnknownIds": False, "items": [{"id": 1}, {"externalId": "2"}]} == jsgz_load(
                mock_file_download_response.calls.nth(0).request.content
            )
            assert res is None
            fp1 = os.path.join(dir_path, "file1")
            fp2 = os.path.join(dir_path, "file2")
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
            mock_file_download_response_with_folder_structure_same_name.calls.nth(0).request.content
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
            mock_file_download_response_with_folder_structure_same_name.calls.nth(0).request.content
        )
        fp1 = tmp_path / "rootdir/subdir/file_a"
        fp2 = tmp_path / "file_a"
        assert fp1.is_file()
        assert fp2.is_file()
        assert fp1.read_text() == "contentSubDir"
        assert fp2.read_text() == "contentNoDir"

    @pytest.fixture
    def mock_byids_response__file_with_double_dots(self, respx_mock, cognite_client):
        filename = "../file1"
        respx_mock.post(
            cognite_client.files._get_base_url_with_base_path() + "/files/byids"
        ).respond(status_code=200, json={"items": [{"id": 1, "name": filename}]})
        yield respx_mock

    def test_download_file_outside_download_directory(self, cognite_client, mock_byids_response__file_with_double_dots):
        with TemporaryDirectory() as dir_path: 
            with pytest.raises(RuntimeError, match="not inside download directory"):
                cognite_client.files.download(directory=dir_path, id=[1])

    def test_download_one_file_fails(self, cognite_client, mock_file_download_response_one_fails):
        with TemporaryDirectory() as dir_path: 
            with pytest.raises(CogniteAPIError) as e:
                cognite_client.files.download(directory=dir_path, id=[1], external_id="fail")
            assert [FileMetadata(id=1, name="file1", external_id="success")] == e.value.successful
            assert [FileMetadata(id=2, name="file2", external_id="fail")] == e.value.failed
            assert os.path.isfile(os.path.join(dir_path, "file1"))

    def test_download_file_to_path(self, cognite_client, mock_file_download_response):
        with TemporaryDirectory() as dir_path: 
            file_path = os.path.join(dir_path, "my_downloaded_file.txt")
            res = cognite_client.files.download_to_path(path=file_path, id=1)
            assert res is None
            assert os.path.isfile(file_path)
            with open(file_path, "rb") as f:
                assert b"content1" == f.read()

    def test_download_to_memory(self, cognite_client, mock_file_download_response):
        res = cognite_client.files.download_bytes(id=1)
        assert {"items": [{"id": 1}]} == jsgz_load(mock_file_download_response.calls.last.request.content)
        assert res == b"content1"

    def test_download_ids_over_limit(self, cognite_client, mock_file_download_response):
        with set_request_limit(cognite_client.files, 1):
            with TemporaryDirectory() as dir_path: 
                res = cognite_client.files.download(directory=dir_path, id=[1], external_id=["2"])
                bodies = [jsgz_load(mock_file_download_response.calls.nth(i).request.content) for i in range(2)] 
                assert {"ignoreUnknownIds": False, "items": [{"id": 1}]} in bodies
                assert {"ignoreUnknownIds": False, "items": [{"externalId": "2"}]} in bodies
                assert res is None
                assert os.path.isfile(os.path.join(dir_path, "file1"))
                assert os.path.isfile(os.path.join(dir_path, "file2"))

    def test_files_update_object(self, mock_geo_location): 
        assert isinstance(
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
            .source.set(None),
            FileMetadataUpdate,
        )

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
        respx_mock, 
        cognite_client,
        data_set_id: int,
        api_error: CogniteAPIError,
        expected_error: type[CogniteAPIError],
        expected_error_message: str,
    ):
        def raise_api_error_side_effect(request: HttpxRequest): 
            raise api_error

        respx_mock.post(cognite_client.files._get_base_url_with_base_path() + "/files").mock(side_effect=raise_api_error_side_effect)

        with pytest.raises(expected_error) as e:
            cognite_client.files.upload_bytes(content=b"content", name="bla", data_set_id=data_set_id)

        assert e.value.message == expected_error_message
        assert e.value.code == api_error.code
        assert e.value.x_request_id == api_error.x_request_id


@pytest.fixture
def mock_files_empty(respx_mock, cognite_client):
    url_pattern = re.compile(re.escape(cognite_client.files._get_base_url_with_base_path()) + "/.+")
    respx_mock.post(url__regex=url_pattern).respond(status_code=200, json={"items": []})
    yield respx_mock


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
        assert [1] == df.loc["assetIds"][0]
        assert "metadata-value" == df.loc["metadata-key"][0]

[end of tests/tests_unit/test_api/test_files.py]
