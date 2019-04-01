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
