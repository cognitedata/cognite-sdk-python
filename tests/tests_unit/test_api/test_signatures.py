import inspect
import json

import pytest

from cognite.client._api import assets, events, files, sequences


class TestListAndIterSignatures:
    @pytest.mark.parametrize(
        "api, filter, ignore",
        [
            (
                assets.AssetsAPI,
                assets.AssetFilter,
                ["root_external_ids", "asset_subtree_external_ids", "aggregated_properties", "partitions"],
            ),
            (
                events.EventsAPI,
                events.EventFilter,
                ["root_asset_external_ids", "asset_subtree_external_ids", "partitions", "sort"],
            ),
            (files.FilesAPI, files.FileMetadataFilter, ["root_asset_external_ids"]),
            (sequences.SequencesAPI, sequences.SequenceFilter, []),
        ],
    )
    def test_list_and_iter_signatures_same_as_filter_signature(self, api, filter, ignore):
        iter_parameters = dict(inspect.signature(api.__call__).parameters)
        for name in set(ignore + ["chunk_size", "limit"]) - {"partitions"}:
            del iter_parameters[name]

        list_parameters = dict(inspect.signature(api.list).parameters)
        for name in ignore + ["limit"]:
            del list_parameters[name]

        filter_parameters = dict(inspect.signature(filter.__init__).parameters)
        del filter_parameters["cognite_client"]

        iter_parameters = {v.name for _, v in iter_parameters.items()}
        filter_parameters = {v.name for _, v in filter_parameters.items()}
        list_parameters = {v.name for _, v in list_parameters.items()}

        assert iter_parameters == filter_parameters, signature_error_msg(filter_parameters, iter_parameters)
        assert list_parameters == filter_parameters, signature_error_msg(filter_parameters, list_parameters)


def signature_error_msg(expected, actual):
    pretty_expected_params = json.dumps(list(expected), indent=4, sort_keys=True)
    pretty_actual_params = json.dumps(list(actual), indent=4, sort_keys=True)
    return "Signatures don't match. \nexpected: {}\ngot: {}\n diff: {}".format(
        pretty_expected_params, pretty_actual_params, list(expected - actual) + list(actual - expected)
    )


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
