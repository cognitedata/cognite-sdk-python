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
from cognite.client.data_classes.data_modeling.ids import NodeId
from cognite.client.data_classes.datapoints import DatapointsArray


@pytest.mark.dsl
class TestRepr:
    # TODO: We should auto-create these tests for all subclasses impl. _repr_html_:
    @pytest.mark.parametrize("cls", (Asset, Sequence, FileMetadata, Row, Table, ThreeDModel))
    def test_repr_html(self, cls):
        assert len(cls()._repr_html_()) > 0

    @pytest.mark.parametrize(
        "inst",
        (
            Datapoint(timestamp=0, value=0),
            Datapoints(id=1),
            DatapointsArray(id=1, timestamp=[]),
            Datapoints(instance_id=NodeId("space", "xid")),
            DatapointsArray(instance_id=NodeId("space", "xid"), timestamp=[]),
        ),
    )
    def test_repr_html_dps_classes(self, inst):
        assert len(inst._repr_html_()) > 0

    @pytest.mark.parametrize(
        "lst",
        (
            AssetList([AssetList._RESOURCE()]),
            DatapointsList([DatapointsList._RESOURCE(id=1)]),
            DatapointsList([DatapointsList._RESOURCE(instance_id=NodeId("space", "xid"))]),
            TableList([TableList._RESOURCE()]),
            RowList([RowList._RESOURCE("row", columns={})]),
        ),
    )
    def test_repr_html_list(self, lst):
        assert len(lst._repr_html_()) > 0
