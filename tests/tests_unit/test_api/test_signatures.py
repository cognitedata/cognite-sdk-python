import inspect

import pytest

from cognite.client.api import assets, events, files


class TestListAndIterSignatures:
    @pytest.mark.parametrize(
        "api, filter",
        [
            (assets.AssetsAPI, assets.AssetFilter),
            (events.EventsAPI, events.EventFilter),
            (files.FilesAPI, files.FileMetadataFilter),
        ],
    )
    def test_list_and_iter_signatures_same_as_filter_signature(self, api, filter):
        iter_parameters = dict(inspect.signature(api.__call__).parameters)
        del iter_parameters["chunk_size"]
        list_parameters = dict(inspect.signature(api.list).parameters)
        del list_parameters["limit"]
        filter_parameters = dict(inspect.signature(filter.__init__).parameters)

        assert iter_parameters == filter_parameters
        assert list_parameters == filter_parameters


class TestFileMetadataUploadSignatures:
    def test_upload_signatures_same_as_file_metadata_signature(self):
        upload_parameters = dict(inspect.signature(files.FilesAPI.upload).parameters)
        del upload_parameters["path"]
        upload_from_memory_parameters = dict(inspect.signature(files.FilesAPI.upload_from_memory).parameters)
        del upload_from_memory_parameters["content"]
        file_metadata_parameters = dict(inspect.signature(files.FileMetadata.__init__).parameters)
        del file_metadata_parameters["id"]
        del file_metadata_parameters["uploaded_at"]
        del file_metadata_parameters["created_time"]
        del file_metadata_parameters["last_updated_time"]
        del file_metadata_parameters["uploaded"]

        assert upload_parameters == file_metadata_parameters
        assert upload_from_memory_parameters == file_metadata_parameters
