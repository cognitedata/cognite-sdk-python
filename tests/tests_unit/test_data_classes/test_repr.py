from __future__ import annotations

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
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from cognite.client.data_classes.data_modeling.ids import NodeId
from cognite.client.data_classes.datapoints import DatapointsArray
from tests.tests_unit.conftest import DefaultResourceGenerator


@pytest.mark.dsl
class TestRepr:
    # TODO: We should auto-create these tests for all subclasses impl. _repr_html_:
    @pytest.mark.parametrize("cls", (Asset, Sequence, FileMetadata, Row, Table, ThreeDModel))
    def test_repr_html(self, cls: type[CogniteResource]) -> None:
        cls_map: dict[type[CogniteResource], CogniteResource] = {
            Asset: DefaultResourceGenerator.asset(),
            Sequence: DefaultResourceGenerator.sequence(),
            FileMetadata: DefaultResourceGenerator.file_metadata(),
            Row: DefaultResourceGenerator.raw_row(),
            Table: DefaultResourceGenerator.raw_table(),
            ThreeDModel: DefaultResourceGenerator.threed_model(),
        }
        instance = cls_map[cls]
        assert len(instance._repr_html_()) > 0

    @pytest.mark.parametrize(
        "inst",
        (
            Datapoint(timestamp=0, value=0),
            Datapoints(id=1),
            DatapointsArray(id=1, timestamp=[]),  # type: ignore[arg-type]
            Datapoints(instance_id=NodeId("space", "xid")),
            DatapointsArray(instance_id=NodeId("space", "xid"), timestamp=[]),  # type: ignore[arg-type]
        ),
    )
    def test_repr_html_dps_classes(self, inst: CogniteResource) -> None:
        assert len(inst._repr_html_()) > 0

    @pytest.mark.parametrize(
        "lst",
        (
            AssetList(
                [
                    Asset(
                        id=123,
                        created_time=123,
                        last_updated_time=123,
                        name="",
                        external_id="foo",
                        parent_id=None,
                        parent_external_id=None,
                        description=None,
                        data_set_id=None,
                        metadata=None,
                        source=None,
                        geo_location=None,
                        root_id=None,
                        aggregates=None,
                        labels=None,
                        cognite_client=None,
                    )
                ]
            ),
            DatapointsList([Datapoints(id=1)]),
            DatapointsList([Datapoints(instance_id=NodeId("space", "xid"))]),
            TableList([Table(name="bla", created_time=123)]),
            RowList([Row("row", columns={}, last_updated_time=123)]),
        ),
    )
    def test_repr_html_list(self, lst: CogniteResourceList) -> None:
        assert len(lst._repr_html_()) > 0
