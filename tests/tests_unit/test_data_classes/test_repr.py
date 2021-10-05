import pytest

from cognite.client.data_classes import *


@pytest.mark.dsl
class TestRepr:
    def test_repr_html(self):
        for cls in [Asset, Datapoints, Sequence, FileMetadata, Row, Table, ThreeDModel]:
            assert len(cls()._repr_html_()) > 0

    def test_repr_html_datapoint(self):
        assert len(Datapoint(timestamp=0, value=0)._repr_html_()) > 0

    def test_repr_html_list(self):
        for cls in [AssetList, DatapointsList, RowList, TableList]:
            assert len(cls([cls._RESOURCE()])._repr_html_()) > 0
