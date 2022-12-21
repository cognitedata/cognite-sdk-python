import json
import os
import re
from io import BufferedReader
from tempfile import TemporaryDirectory

import pytest

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
from cognite.client.exceptions import CogniteAPIError
from tests.utils import jsgz_load, set_request_limit


@pytest.fixture
def mock_geo_location():
    geometry = Geometry(type="Point", coordinates=[35, 10])
    yield GeoLocation(type="Feature", geometry=geometry)


@pytest.fixture
def mock_files_response(rsps, cognite_client, mock_geo_location):
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
                "geoLocation": mock_geo_location,
                "id": 1,
                "uploaded": True,
                "uploadedTime": 0,
                "createdTime": 0,
                "lastUpdatedTime": 0,
            }
        ]
    }

    url_pattern = re.compile(re.escape(cognite_client.files._get_base_url_with_base_path()) + "/.+")
    rsps.assert_all_requests_are_fired = False

    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
    yield rsps


@pytest.fixture
def mock_file_upload_response(rsps, cognite_client, mock_geo_location):
    response_body = {
        "externalId": "string",
        "name": "string",
        "source": "string",
        "mimeType": "string",
        "metadata": {},
        "assetIds": [1],
        "labels": [{"externalId": "WELL LOG"}],
        "geoLocation": mock_geo_location,
        "id": 1,
        "uploaded": True,
        "uploadedTime": 0,
        "createdTime": 0,
        "lastUpdatedTime": 0,
        "uploadUrl": "https://upload.here",
    }
    rsps.add(rsps.POST, cognite_client.files._get_base_url_with_base_path() + "/files", status=200, json=response_body)
    rsps.add(rsps.PUT, "https://upload.here", status=200)
    yield rsps


@pytest.fixture
def mock_file_create_response(rsps, cognite_client, mock_geo_location):
    response_body = {
        "externalId": "string",
        "name": "string",
        "source": "string",
        "mimeType": "string",
        "metadata": {},
        "assetIds": [1],
        "labels": [{"externalId": "WELL LOG"}],
        "geoLocation": mock_geo_location,
        "id": 1,
        "uploaded": False,
        "uploadedTime": 0,
        "createdTime": 0,
        "lastUpdatedTime": 0,
        "uploadUrl": "https://upload.here",
    }
    rsps.add(rsps.POST, cognite_client.files._get_base_url_with_base_path() + "/files", status=200, json=response_body)
    yield rsps


@pytest.fixture
def mock_file_download_response(rsps, cognite_client):
    rsps.add(
        rsps.POST,
        cognite_client.files._get_base_url_with_base_path() + "/files/byids",
        status=200,
        json={"items": [{"id": 1, "name": "file1"}, {"externalId": "2", "name": "file2"}]},
    )

    def download_link_callback(request):
        identifier = jsgz_load(request.body)["items"][0]
        response = {}
        if "id" in identifier:
            response = {"items": [{"id": 1, "downloadUrl": "https://download.file1.here"}]}
        elif "externalId" in identifier:
            response = {"items": [{"externalId": "2", "downloadUrl": "https://download.file2.here"}]}
        return 200, {}, json.dumps(response)

    rsps.add_callback(
        rsps.POST,
        cognite_client.files._get_base_url_with_base_path() + "/files/downloadlink",
        callback=download_link_callback,
        content_type="application/json",
    )
    rsps.add(rsps.GET, "https://download.file1.here", status=200, body="content1")
    rsps.add(rsps.GET, "https://download.file2.here", status=200, body="content2")
    yield rsps


@pytest.fixture
def mock_file_download_response_one_fails(rsps, cognite_client):
    rsps.add(
        rsps.POST,
        cognite_client.files._get_base_url_with_base_path() + "/files/byids",
        status=200,
        json={
            "items": [
                {"id": 1, "externalId": "success", "name": "file1"},
                {"externalId": "fail", "id": 2, "name": "file2"},
            ]
        },
    )

    def download_link_callback(request):
        identifier = jsgz_load(request.body)["items"][0]
        if "id" in identifier:
            return 200, {}, json.dumps({"items": [{"id": 1, "downloadUrl": "https://download.file1.here"}]})
        elif "externalId" in identifier:
            return (400, {}, json.dumps({"error": {"message": "User error", "code": 400}}))

    rsps.add_callback(
        rsps.POST,
        cognite_client.files._get_base_url_with_base_path() + "/files/downloadlink",
        callback=download_link_callback,
        content_type="application/json",
    )
    rsps.add(rsps.GET, "https://download.file1.here", status=200, body="content1")
    yield rsps


class TestFilesAPI:
    def test_create(self, cognite_client, mock_file_create_response):
        file_metadata = FileMetadata(name="bla")
        returned_file_metadata, upload_url = cognite_client.files.create(file_metadata)
        response_body = mock_file_create_response.calls[0].response.json()
        assert FileMetadata._load(response_body) == returned_file_metadata
        assert response_body["uploadUrl"] == upload_url

    def test_create_with_label(self, cognite_client, mock_file_create_response):
        file_metadata = FileMetadata(name="bla", labels=[Label(external_id="WELL LOG")])
        returned_file_metadata, upload_url = cognite_client.files.create(file_metadata)
        response_body = mock_file_create_response.calls[0].response.json()
        assert FileMetadata._load(response_body) == returned_file_metadata
        assert response_body["uploadUrl"] == upload_url
        assert response_body["labels"][0]["externalId"] == "WELL LOG"

    def test_create_with_label_request(self, cognite_client, mock_file_create_response):
        file_metadata = FileMetadata(name="bla", labels=[Label(external_id="WELL LOG")])
        returned_file_metadata, upload_url = cognite_client.files.create(file_metadata)
        response_body = mock_file_create_response.calls[0].response.json()
        request_body = jsgz_load(mock_file_create_response.calls[0].request.body)
        assert FileMetadata._load(response_body) == returned_file_metadata
        assert all(body["labels"][0]["externalId"] == "WELL LOG" for body in [request_body, response_body])

    def test_create_with_geoLocation(self, cognite_client, mock_file_create_response, mock_geo_location):
        file_metadata = FileMetadata(name="bla", geo_location=mock_geo_location)
        returned_file_metadata, upload_url = cognite_client.files.create(file_metadata)
        response_body = mock_file_create_response.calls[0].response.json()
        assert FileMetadata._load(response_body) == returned_file_metadata
        assert response_body["geoLocation"] == mock_geo_location

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
        response_body = mock_file_create_response.calls[0].response.json()
        request_body = jsgz_load(mock_file_create_response.calls[0].request.body)
        assert FileMetadata._load(response_body) == returned_file_metadata
        assert all(body["geoLocation"] == mock_geo_location for body in [request_body, response_body])

    def test_retrieve_single(self, cognite_client, mock_files_response):
        res = cognite_client.files.retrieve(id=1)
        assert isinstance(res, FileMetadata)
        assert mock_files_response.calls[0].response.json()["items"][0] == res.dump(camel_case=True)

    def test_retrieve_multiple(self, cognite_client, mock_files_response):
        res = cognite_client.files.retrieve_multiple(ids=[1])
        assert isinstance(res, FileMetadataList)
        assert mock_files_response.calls[0].response.json()["items"] == res.dump(camel_case=True)

    def test_list(self, cognite_client, mock_files_response):
        res = cognite_client.files.list(source="bla", limit=10)
        assert isinstance(res, FileMetadataList)
        assert mock_files_response.calls[0].response.json()["items"] == res.dump(camel_case=True)
        assert "bla" == jsgz_load(mock_files_response.calls[0].request.body)["filter"]["source"]
        assert 10 == jsgz_load(mock_files_response.calls[0].request.body)["limit"]

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
        } == jsgz_load(calls[0].request.body)

    def test_list_subtrees(self, cognite_client, mock_files_response):
        cognite_client.files.list(asset_subtree_ids=[1], asset_subtree_external_ids=["a"], limit=10)
        calls = mock_files_response.calls
        assert 1 == len(calls)
        assert {
            "cursor": None,
            "limit": 10,
            "filter": {"assetSubtreeIds": [{"id": 1}, {"externalId": "a"}]},
        } == jsgz_load(calls[0].request.body)

    def test_filter_directory(self, cognite_client, mock_files_response):
        cognite_client.files.list(directory_prefix="/test", limit=10)
        calls = mock_files_response.calls
        assert len(calls) == 1
        assert jsgz_load(calls[0].request.body) == {"cursor": None, "filter": {"directoryPrefix": "/test"}, "limit": 10}

    def test_filter_geoLocation(self, cognite_client, mock_files_response):
        cognite_client.files.list(
            geo_location=GeoLocationFilter(relation="within", shape=GeometryFilter(type="Point", coordinates=[35, 10])),
            limit=10,
        )
        calls = mock_files_response.calls
        assert len(calls) == 1
        assert jsgz_load(calls[0].request.body) == {
            "cursor": None,
            "filter": {"geoLocation": {"relation": "within", "shape": {"type": "Point", "coordinates": [35, 10]}}},
            "limit": 10,
        }

    def test_list_with_time_dict(self, cognite_client, mock_files_response):
        cognite_client.files.list(created_time={"min": 20})
        assert 20 == jsgz_load(mock_files_response.calls[0].request.body)["filter"]["createdTime"]["min"]
        assert "max" not in jsgz_load(mock_files_response.calls[0].request.body)["filter"]["createdTime"]

    def test_list_with_timestamp_range(self, cognite_client, mock_files_response):
        cognite_client.files.list(created_time=TimestampRange(min=20))
        assert 20 == jsgz_load(mock_files_response.calls[0].request.body)["filter"]["createdTime"]["min"]
        assert "max" not in jsgz_load(mock_files_response.calls[0].request.body)["filter"]["createdTime"]

    def test_delete_single(self, cognite_client, mock_files_response):
        res = cognite_client.files.delete(id=1)
        assert {"items": [{"id": 1}]} == jsgz_load(mock_files_response.calls[0].request.body)
        assert res is None

    def test_delete_multiple(self, cognite_client, mock_files_response):
        res = cognite_client.files.delete(id=[1])
        assert {"items": [{"id": 1}]} == jsgz_load(mock_files_response.calls[0].request.body)
        assert res is None

    def test_update_with_resource_class(self, cognite_client, mock_files_response):
        res = cognite_client.files.update(FileMetadata(id=1, source="bla"))
        assert isinstance(res, FileMetadata)
        assert {"items": [{"id": 1, "update": {"source": {"set": "bla"}}}]} == jsgz_load(
            mock_files_response.calls[0].request.body
        )

    def test_update_with_update_class(self, cognite_client, mock_files_response):
        res = cognite_client.files.update(FileMetadataUpdate(id=1).source.set("bla"))
        assert isinstance(res, FileMetadata)
        assert {"items": [{"id": 1, "update": {"source": {"set": "bla"}}}]} == jsgz_load(
            mock_files_response.calls[0].request.body
        )

    def test_update_labels_single(self, cognite_client, mock_files_response):
        cognite_client.files.update([FileMetadataUpdate(id=1).labels.add("PUMP").labels.remove("WELL LOG")])
        expected = {"labels": {"add": [{"externalId": "PUMP"}], "remove": [{"externalId": "WELL LOG"}]}}
        assert jsgz_load(mock_files_response.calls[0].request.body)["items"][0]["update"] == expected

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
        assert jsgz_load(mock_files_response.calls[0].request.body)["items"][0]["update"] == expected

    # resource.update doesn't support full replacement of labels (set operation)
    def test_ignore_labels_resource_class(self, cognite_client, mock_files_response):
        cognite_client.files.update(FileMetadata(id=1, labels=[Label(external_id="Pump")], external_id="newId"))
        assert jsgz_load(mock_files_response.calls[0].request.body)["items"][0]["update"] == {
            "externalId": {"set": "newId"}
        }

    def test_labels_filter_contains_all(self, cognite_client, mock_files_response):
        my_label_filter = LabelFilter(contains_all=["WELL LOG", "VERIFIED"])
        cognite_client.files.list(labels=my_label_filter)
        assert jsgz_load(mock_files_response.calls[0].request.body)["filter"]["labels"] == {
            "containsAll": [{"externalId": "WELL LOG"}, {"externalId": "VERIFIED"}]
        }

    def test_labels_filter_contains_any(self, cognite_client, mock_files_response):
        my_label_filter = LabelFilter(contains_any=["WELL LOG", "WELL REPORT"])
        cognite_client.files.list(labels=my_label_filter)
        assert jsgz_load(mock_files_response.calls[0].request.body)["filter"]["labels"] == {
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
        } == jsgz_load(mock_files_response.calls[0].request.body)

    def test_iter_single(self, cognite_client, mock_files_response):
        for file in cognite_client.files:
            assert isinstance(file, FileMetadata)
            assert mock_files_response.calls[0].response.json()["items"][0] == file.dump(camel_case=True)

    def test_iter_chunk(self, cognite_client, mock_files_response):
        for file in cognite_client.files(chunk_size=1):
            assert isinstance(file, FileMetadataList)
            assert mock_files_response.calls[0].response.json()["items"] == file.dump(camel_case=True)

    def test_search(self, cognite_client, mock_files_response):
        res = cognite_client.files.search(filter=FileMetadataFilter(external_id_prefix="abc"))
        assert mock_files_response.calls[0].response.json()["items"] == res.dump(camel_case=True)
        assert {"search": {"name": None}, "filter": {"externalIdPrefix": "abc"}, "limit": 100} == jsgz_load(
            mock_files_response.calls[0].request.body
        )

    @pytest.mark.parametrize("filter_field", ["external_id_prefix", "externalIdPrefix"])
    def test_search_dict_filter(self, cognite_client, mock_files_response, filter_field):
        res = cognite_client.files.search(filter={filter_field: "abc"})
        assert mock_files_response.calls[0].response.json()["items"] == res.dump(camel_case=True)
        assert {"search": {"name": None}, "filter": {"externalIdPrefix": "abc"}, "limit": 100} == jsgz_load(
            mock_files_response.calls[0].request.body
        )

    def test_upload(self, cognite_client, mock_file_upload_response):
        dir = os.path.join(os.path.dirname(__file__), "files_for_test_upload")
        path = os.path.join(dir, "file_for_test_upload_1.txt")
        res = cognite_client.files.upload(path, name="bla", directory=dir)
        response_body = mock_file_upload_response.calls[0].response.json()
        del response_body["uploadUrl"]
        assert FileMetadata._load(response_body) == res
        assert "https://upload.here/" == mock_file_upload_response.calls[1].request.url
        assert {"name": "bla", "directory": dir} == jsgz_load(mock_file_upload_response.calls[0].request.body)
        assert isinstance(mock_file_upload_response.calls[1].request.body, BufferedReader)

    def test_upload_with_external_id(self, cognite_client, mock_file_upload_response):
        path = os.path.join(os.path.dirname(__file__), "files_for_test_upload", "file_for_test_upload_1.txt")
        cognite_client.files.upload(path, external_id="blabla", name="bla", data_set_id=42)

    def test_upload_no_name(self, cognite_client, mock_file_upload_response):
        dir = os.path.join(os.path.dirname(__file__), "files_for_test_upload")
        path = os.path.join(dir, "file_for_test_upload_1.txt")
        cognite_client.files.upload(path, directory=dir)
        assert {"name": "file_for_test_upload_1.txt", "directory": dir} == jsgz_load(
            mock_file_upload_response.calls[0].request.body
        )

    def test_upload_set_directory(self, cognite_client, mock_file_upload_response):
        set_dir = "/Some/custom/directory"
        dir = os.path.join(os.path.dirname(__file__), "files_for_test_upload")
        path = os.path.join(dir, "file_for_test_upload_1.txt")
        cognite_client.files.upload(path, directory=set_dir)
        assert {"name": "file_for_test_upload_1.txt", "directory": set_dir} == jsgz_load(
            mock_file_upload_response.calls[0].request.body
        )

    def test_upload_from_directory(self, cognite_client, mock_file_upload_response):
        path = os.path.join(os.path.dirname(__file__), "files_for_test_upload")
        res = cognite_client.files.upload(path=path, asset_ids=[1, 2])
        response_body = mock_file_upload_response.calls[0].response.json()
        del response_body["uploadUrl"]
        assert FileMetadataList([FileMetadata._load(response_body), FileMetadata._load(response_body)]) == res
        assert 4 == len(mock_file_upload_response.calls)
        for call in mock_file_upload_response.calls:
            payload = call.request.body
            if isinstance(payload, BufferedReader):
                continue
            else:
                json = jsgz_load(payload)
                assert [1, 2] == json["assetIds"]
                assert json["name"] in ["file_for_test_upload_1.txt", "file_for_test_upload_2.txt"]

    def test_upload_from_directory_fails(self, cognite_client, rsps):
        rsps.add(rsps.POST, cognite_client.files._get_base_url_with_base_path() + "/files", status=400, json={})
        path = os.path.join(os.path.dirname(__file__), "files_for_test_upload")
        with pytest.raises(CogniteAPIError) as e:
            cognite_client.files.upload(path=path)
        assert "file_for_test_upload_1.txt" in e.value.failed
        assert "file_for_test_upload_2.txt" in e.value.failed

    def test_upload_from_directory_recursively(self, cognite_client, mock_file_upload_response):
        path = os.path.join(os.path.dirname(__file__), "files_for_test_upload")
        res = cognite_client.files.upload(path=path, recursive=True, asset_ids=[1, 2])
        response_body = mock_file_upload_response.calls[0].response.json()
        del response_body["uploadUrl"]
        assert FileMetadataList([FileMetadata._load(response_body) for _ in range(3)]) == res
        assert 6 == len(mock_file_upload_response.calls)
        for call in mock_file_upload_response.calls:
            payload = call.request.body
            if isinstance(payload, BufferedReader):
                continue
            else:
                json = jsgz_load(payload)
                assert json["name"] in [
                    "file_for_test_upload_1.txt",
                    "file_for_test_upload_2.txt",
                    "file_for_test_upload_3.txt",
                ]
            assert [1, 2] == json["assetIds"]

    def test_upload_from_memory(self, cognite_client, mock_file_upload_response):
        res = cognite_client.files.upload_bytes(content=b"content", name="bla")
        response_body = mock_file_upload_response.calls[0].response.json()
        del response_body["uploadUrl"]
        assert FileMetadata._load(response_body) == res
        assert "https://upload.here/" == mock_file_upload_response.calls[1].request.url
        assert {"name": "bla"} == jsgz_load(mock_file_upload_response.calls[0].request.body)
        assert b"content" == mock_file_upload_response.calls[1].request.body

    def test_upload_using_file_handle(self, cognite_client, mock_file_upload_response):
        path = os.path.join(os.path.dirname(__file__), "files_for_test_upload", "file_for_test_upload_1.txt")
        with open(path, "rb") as fh:
            res = cognite_client.files.upload_bytes(fh, name="bla")
        response_body = mock_file_upload_response.calls[0].response.json()
        del response_body["uploadUrl"]
        assert FileMetadata._load(response_body) == res
        assert "https://upload.here/" == mock_file_upload_response.calls[1].request.url
        assert {"name": "bla"} == jsgz_load(mock_file_upload_response.calls[0].request.body)
        assert isinstance(mock_file_upload_response.calls[1].request.body, BufferedReader)

    def test_upload_path_does_not_exist(self, cognite_client):
        with pytest.raises(ValueError, match="does not exist"):
            cognite_client.files.upload(path="/no/such/path")

    def test_download(self, cognite_client, mock_file_download_response):
        with TemporaryDirectory() as dir:
            res = cognite_client.files.download(directory=dir, id=[1], external_id=["2"])
            assert {"ignoreUnknownIds": False, "items": [{"id": 1}, {"externalId": "2"}]} == jsgz_load(
                mock_file_download_response.calls[0].request.body
            )
            assert res is None
            fp1 = os.path.join(dir, "file1")
            fp2 = os.path.join(dir, "file2")
            assert os.path.isfile(fp1)
            assert os.path.isfile(fp2)
            with open(fp1, "rb") as fh:
                assert b"content1" == fh.read()
            with open(fp2, "rb") as fh:
                assert b"content2" == fh.read()

    @pytest.fixture
    def mock_byids_response__file_with_double_dots(self, rsps, cognite_client):
        filename = "../file1"
        rsps.add(
            rsps.POST,
            cognite_client.files._get_base_url_with_base_path() + "/files/byids",
            status=200,
            json={"items": [{"id": 1, "name": filename}]},
        )
        yield rsps

    def test_download_file_outside_download_directory(self, cognite_client, mock_byids_response__file_with_double_dots):
        with TemporaryDirectory() as dir:
            with pytest.raises(RuntimeError, match="not inside download directory"):
                cognite_client.files.download(directory=dir, id=[1])

    def test_download_one_file_fails(self, cognite_client, mock_file_download_response_one_fails):
        with TemporaryDirectory() as dir:
            with pytest.raises(CogniteAPIError) as e:
                cognite_client.files.download(directory=dir, id=[1], external_id="fail")
            assert [FileMetadata(id=1, name="file1", external_id="success")] == e.value.successful
            assert [FileMetadata(id=2, name="file2", external_id="fail")] == e.value.failed
            assert os.path.isfile(os.path.join(dir, "file1"))

    def test_download_file_to_path(self, cognite_client, mock_file_download_response):
        mock_file_download_response.assert_all_requests_are_fired = False
        with TemporaryDirectory() as dir:
            file_path = os.path.join(dir, "my_downloaded_file.txt")
            res = cognite_client.files.download_to_path(path=file_path, id=1)
            assert res is None
            assert os.path.isfile(file_path)
            with open(file_path, "rb") as f:
                assert b"content1" == f.read()

    def test_download_to_memory(self, cognite_client, mock_file_download_response):
        mock_file_download_response.assert_all_requests_are_fired = False
        res = cognite_client.files.download_bytes(id=1)
        assert {"items": [{"id": 1}]} == jsgz_load(mock_file_download_response.calls[0].request.body)
        assert res == b"content1"

    def test_download_ids_over_limit(self, cognite_client, mock_file_download_response):
        with set_request_limit(cognite_client.files, 1):
            with TemporaryDirectory() as dir:
                res = cognite_client.files.download(directory=dir, id=[1], external_id=["2"])
                bodies = [jsgz_load(mock_file_download_response.calls[i].request.body) for i in range(2)]
                assert {"ignoreUnknownIds": False, "items": [{"id": 1}]} in bodies
                assert {"ignoreUnknownIds": False, "items": [{"externalId": "2"}]} in bodies
                assert res is None
                assert os.path.isfile(os.path.join(dir, "file1"))
                assert os.path.isfile(os.path.join(dir, "file2"))

    def test_files_update_object(self):
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


@pytest.fixture
def mock_files_empty(rsps, cognite_client):
    url_pattern = re.compile(re.escape(cognite_client.files._get_base_url_with_base_path()) + "/.+")
    rsps.add(rsps.POST, url_pattern, status=200, json={"items": []})
    yield rsps


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

        df = cognite_client.files.retrieve(id=1).to_pandas(camel_case=True)
        assert isinstance(df, pd.DataFrame)
        assert "metadata" not in df.columns
        assert [1] == df.loc["assetIds"][0]
        assert "metadata-value" == df.loc["metadata-key"][0]
