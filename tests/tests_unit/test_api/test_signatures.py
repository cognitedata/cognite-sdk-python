import inspect

import pytest

from cognite.client._api import assets, events, files, sequences


class TestListAndIterSignatures:
    @pytest.mark.parametrize(
        "api, filter",
        [
            (assets.AssetsAPI, assets.AssetFilter),
            (events.EventsAPI, events.EventFilter),
            (files.FilesAPI, files.FileMetadataFilter),
            (sequences.SequencesAPI, sequences.SequenceFilter),
        ],
    )
    def test_list_and_iter_signatures_same_as_filter_signature(self, api, filter):
        iter_parameters = dict(inspect.signature(api.__call__).parameters)
        del iter_parameters["chunk_size"]
        del iter_parameters["limit"]
        list_parameters = dict(inspect.signature(api.list).parameters)
        del list_parameters["limit"]
        filter_parameters = dict(inspect.signature(filter.__init__).parameters)
        del filter_parameters["cognite_client"]
        assert iter_parameters == filter_parameters
        assert list_parameters == filter_parameters


class TestFileMetadataUploadSignatures:
    def test_upload_signatures_same_as_file_metadata_signature(self):
        upload_parameters = dict(inspect.signature(files.FilesAPI.upload).parameters)
        del upload_parameters["path"]
        del upload_parameters["recursive"]
        del upload_parameters["overwrite"]
        del upload_parameters["name"]
        upload_from_memory_parameters = dict(inspect.signature(files.FilesAPI.upload_bytes).parameters)
        del upload_from_memory_parameters["content"]
        del upload_from_memory_parameters["overwrite"]
        del upload_from_memory_parameters["name"]
        file_metadata_parameters = dict(inspect.signature(files.FileMetadata.__init__).parameters)
        del file_metadata_parameters["id"]
        del file_metadata_parameters["uploaded_time"]
        del file_metadata_parameters["created_time"]
        del file_metadata_parameters["last_updated_time"]
        del file_metadata_parameters["uploaded"]
        del file_metadata_parameters["cognite_client"]
        del file_metadata_parameters["name"]

        assert upload_parameters == file_metadata_parameters
        assert upload_from_memory_parameters == file_metadata_parameters
