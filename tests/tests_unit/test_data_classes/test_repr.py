import pytest

from cognite.client.data_classes import (
    Asset,
    AssetList,
    Datapoint,
    Datapoints,
    DatapointsList,
    FileMetadata,
    Row,
    RowList,
    Sequence,
    Table,
    TableList,
    ThreeDModel,
)


@pytest.mark.dsl
class TestRepr:
    # TODO: We should auto-create these tests for all subclasses impl. _repr_html_:
    def test_repr_html(self):
        for cls in [Asset, Datapoints, Sequence, FileMetadata, Row, Table, ThreeDModel]:
            assert len(cls()._repr_html_()) > 0

    def test_repr_html_datapoint(self):
        assert len(Datapoint(timestamp=0, value=0)._repr_html_()) > 0

    def test_repr_html_list(self):
        for cls in [AssetList, DatapointsList, TableList]:
            assert len(cls([cls._RESOURCE()])._repr_html_()) > 0
        assert len(RowList([RowList._RESOURCE("row", columns={})])._repr_html_()) > 0
