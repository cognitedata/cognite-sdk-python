from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime

import pytest
from _pytest.mark import ParameterSet

from cognite.client import CogniteClient
from cognite.client.data_classes.cdm import v1 as cdm
from cognite.client.data_classes.data_modeling import (
    Space,
    SpaceApply,
    TypedEdge,
    TypedEdgeApply,
    TypedNode,
    TypedNodeApply,
)

DATA_SPACE = "python_sdk_core_v1_test_space"


def core_model_v1_node_test_cases() -> Iterable[ParameterSet]:
    today = datetime.now()
    yesterday = today.replace(day=today.day - 1)
    yield pytest.param(
        cdm.AssetApply(
            space=DATA_SPACE,
            external_id="test_asset",
            source_id="SAP",
            source_created_time=today,
            source_updated_time=today,
            source_created_user="Me",
            source_updated_user="Also me",
            name="Test asset",
            description="Test asset for core model v1 tests with Python SDK",
            aliases=["test_asset_alias"],
        ),
        cdm.Asset,
        id="Asset",
    )
    yield pytest.param(
        cdm.ActivityApply(
            space=DATA_SPACE,
            external_id="test_activity",
            assets=[(DATA_SPACE, "test_asset")],
            source_id="SAP",
            source_created_time=yesterday,
            source_updated_time=today,
            source_created_user="Me",
            source_updated_user="Also me",
            name="Test activity",
            description="Test activity for core model v1 tests with Python SDK",
            aliases=["test_activity_alias"],
            start_time=yesterday,
            end_time=today,
            scheduled_start_time=yesterday,
            scheduled_end_time=today,
        ),
        cdm.Activity,
        id="Activity",
    )

    yield pytest.param(
        cdm.AssetTypeApply(
            space=DATA_SPACE,
            external_id="test_asset_type",
            name="Test asset type",
            description="Test asset type for core model v1 tests with Python SDK",
            aliases=["test_asset_type_alias"],
            code="101",
        ),
        cdm.AssetType,
        id="AssetType",
    )
    yield pytest.param(
        cdm.DescribableApply(
            space=DATA_SPACE,
            external_id="test_describable",
            name="Test describable",
            description="Test describable for core model v1 tests with Python SDK",
            aliases=["test_describable_alias"],
        ),
        cdm.Describable,
        id="Describable",
    )
    yield pytest.param(
        cdm.EquipmentApply(
            space=DATA_SPACE,
            external_id="test_equipment",
            name="Test equipment",
            description="Test equipment for core model v1 tests with Python SDK",
            aliases=["test_equipment_alias"],
            source_id="SAP",
            source_created_time=yesterday,
            source_updated_time=today,
            source_created_user="Me",
            source_updated_user="Also me",
        ),
        cdm.Equipment,
        id="Equipment",
    )
    yield pytest.param(
        cdm.Model3DApply(
            space=DATA_SPACE,
            external_id="test_model_3d",
            name="Test model 3D",
            description="Test model 3D for core model v1 tests with Python SDK",
            aliases=["test_model_3d_alias"],
            source_id="SAP",
            source_created_time=yesterday,
            source_updated_time=today,
            source_created_user="Me",
            source_updated_user="Also me",
        ),
        cdm.Model3D,
        id="Model3D",
    )
    yield pytest.param(
        cdm.Object3DApply(
            space=DATA_SPACE,
            external_id="test_object_3d",
            name="Test object 3D",
            description="Test object 3D for core model v1 tests with Python SDK",
            aliases=["test_object_3d_alias"],
            source_id="SAP",
            source_created_time=yesterday,
            source_updated_time=today,
            source_created_user="Me",
            source_updated_user="Also me",
        ),
        cdm.Object3D,
        id="Object3D",
    )

    yield pytest.param(
        cdm.SchedulableApply(
            space=DATA_SPACE,
            external_id="test_schedulable",
            start_time=yesterday,
            end_time=today,
            scheduled_start_time=yesterday,
            scheduled_end_time=today,
        ),
        cdm.Schedulable,
        id="Schedulable",
    )

    yield pytest.param(
        cdm.SourceableApply(
            space=DATA_SPACE,
            external_id="test_sourceable",
            source_id="SAP",
            source_created_time=yesterday,
            source_updated_time=today,
            source_created_user="Me",
            source_updated_user="Also me",
        ),
        cdm.Sourceable,
        id="Sourceable",
    )

    yield pytest.param(
        cdm.TimesSeriesBaseApply(
            space=DATA_SPACE,
            external_id="test_time_series_base",
            is_step=False,
            is_string=False,
            name="Test time series base",
            description="Test time series base for core model v1 tests with Python SDK",
            aliases=["test_time_series_base_alias"],
            source_id="SAP",
            source_created_time=yesterday,
            source_updated_time=today,
            source_created_user="Me",
            source_updated_user="Also me",
        ),
        cdm.TimeSeriesBase,
        id="TimeSeriesBase",
    )


def core_model_v1_edge_test_cases() -> Iterable[ParameterSet]:
    yield pytest.param(
        cdm.Connection3DApply(
            space=DATA_SPACE,
            external_id="test_connection_3d",
            type=(DATA_SPACE, "test_asset_type"),
            start_node=(DATA_SPACE, "test_object_3d_start"),
            end_node=(DATA_SPACE, "test_object_3d_end"),
            revision_id=4,
            revision_node_id=42,
        ),
        cdm.Connection3D,
        id="Connection3D",
    )


@pytest.fixture(scope="session")
def data_space(cognite_client_alpha: CogniteClient) -> Space:
    space = cognite_client_alpha.data_modeling.spaces.apply(
        SpaceApply(space=DATA_SPACE, description="Test space for core model v1 tests with Python SDK", name=DATA_SPACE)
    )
    return space


class TestCoreModelv1:
    @pytest.mark.usefixtures("data_space")
    @pytest.mark.parametrize("write_instance, read_type", list(core_model_v1_node_test_cases()))
    def test_write_read_node(
        self, write_instance: TypedNodeApply, read_type: type[TypedNode], cognite_client_alpha: CogniteClient
    ) -> None:
        created = cognite_client_alpha.data_modeling.instances.apply(write_instance)

        assert len(created.nodes) == 1
        assert created.nodes[0].as_id() == write_instance.as_id()

        read = cognite_client_alpha.data_modeling.instances.retrieve_nodes(write_instance.as_id(), node_cls=read_type)

        assert isinstance(read, read_type)
        assert read.as_id() == write_instance.as_id()

        read_dumped = read.as_write().dump()
        write_instance_dumped = write_instance.dump()
        # Existing version will be bumped by the server
        # so we need to remove it from the comparison
        read_dumped.pop("existingVersion", None)
        write_instance_dumped.pop("existingVersion", None)
        for key, value in read_dumped["sources"][0]["properties"].items():
            if isinstance(value, str) and value.endswith("+00:00"):
                # Server sets timezone to UTC, but expects the client to send it without
                read_dumped["sources"][0]["properties"][key] = value[:-6]
        assert write_instance_dumped == read_dumped

    @pytest.mark.usefixtures("data_space")
    @pytest.mark.parametrize("write_instance, read_type", list(core_model_v1_edge_test_cases()))
    def test_write_read_edge(
        self, write_instance: TypedEdgeApply, read_type: type[TypedEdge], cognite_client_alpha: CogniteClient
    ) -> None:
        created = cognite_client_alpha.data_modeling.instances.apply(
            edges=write_instance, auto_create_start_nodes=True, auto_create_end_nodes=True
        )

        assert len(created.edges) == 1
        assert created.edges[0].as_id() == write_instance.as_id()

        read = cognite_client_alpha.data_modeling.instances.retrieve_edges(write_instance.as_id(), edge_cls=read_type)

        assert isinstance(read, read_type)
        assert read.as_id() == write_instance.as_id()

        read_dumped = read.as_write().dump()
        write_instance_dumped = write_instance.dump()
        # Existing version will be bumped by the server
        # so we need to remove it from the comparison
        read_dumped.pop("existingVersion", None)
        write_instance_dumped.pop("existingVersion", None)
        for key, value in read_dumped["sources"][0]["properties"].items():
            if isinstance(value, str) and value.endswith("+00:00"):
                # Server sets timezone to UTC, but expects the client to send it without
                read_dumped["sources"][0]["properties"][key] = value[:-6]
        assert write_instance_dumped == read_dumped
