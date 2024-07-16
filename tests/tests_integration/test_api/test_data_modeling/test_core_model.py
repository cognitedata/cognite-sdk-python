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
    TypedNode,
    TypedNodeApply,
)

DATA_SPACE = "python_sdk_core_v1_test_space"


def core_model_v1_node_test_cases() -> Iterable[ParameterSet]:
    today = datetime.now()
    yesterday = today.replace(day=today.day - 1)
    yield pytest.param(
        cdm.CogniteAssetApply(
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
        cdm.CogniteAsset,
        id="Asset",
    )
    yield pytest.param(
        cdm.CogniteActivityApply(
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
        cdm.CogniteActivity,
        id="Activity",
    )

    yield pytest.param(
        cdm.CogniteAssetTypeApply(
            space=DATA_SPACE,
            external_id="test_asset_type",
            name="Test asset type",
            description="Test asset type for core model v1 tests with Python SDK",
            aliases=["test_asset_type_alias"],
            code="101",
        ),
        cdm.CogniteAssetType,
        id="AssetType",
    )
    yield pytest.param(
        cdm.CogniteDescribableApply(
            space=DATA_SPACE,
            external_id="test_describable",
            name="Test describable",
            description="Test describable for core model v1 tests with Python SDK",
            aliases=["test_describable_alias"],
        ),
        cdm.CogniteDescribable,
        id="Describable",
    )
    yield pytest.param(
        cdm.CogniteEquipmentApply(
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
        cdm.CogniteEquipment,
        id="Equipment",
    )
    yield pytest.param(
        cdm.CogniteModel3DApply(
            space=DATA_SPACE,
            external_id="test_model_3d",
            name="Test model 3D",
            description="Test model 3D for core model v1 tests with Python SDK",
            aliases=["test_model_3d_alias"],
        ),
        cdm.CogniteModel3D,
        id="Model3D",
    )
    yield pytest.param(
        cdm.CogniteObject3DApply(
            space=DATA_SPACE,
            external_id="test_object_3d",
            name="Test object 3D",
            description="Test object 3D for core model v1 tests with Python SDK",
            aliases=["test_object_3d_alias"],
        ),
        cdm.CogniteObject3D,
        id="Object3D",
    )

    yield pytest.param(
        cdm.CogniteSchedulableApply(
            space=DATA_SPACE,
            external_id="test_schedulable",
            start_time=yesterday,
            end_time=today,
            scheduled_start_time=yesterday,
            scheduled_end_time=today,
        ),
        cdm.CogniteSchedulable,
        id="Schedulable",
    )

    yield pytest.param(
        cdm.CogniteSourceableApply(
            space=DATA_SPACE,
            external_id="test_sourceable",
            source_id="SAP",
            source_created_time=yesterday,
            source_updated_time=today,
            source_created_user="Me",
            source_updated_user="Also me",
        ),
        cdm.CogniteSourceable,
        id="Sourceable",
    )

    yield pytest.param(
        cdm.CogniteTimeSeriesApply(
            space=DATA_SPACE,
            external_id="test_time_series_base",
            is_step=False,
            name="Test time series base",
            description="Test time series base for core model v1 tests with Python SDK",
            aliases=["test_time_series_base_alias"],
            source_id="SAP",
            source_created_time=yesterday,
            source_updated_time=today,
            source_created_user="Me",
            source_updated_user="Also me",
        ),
        cdm.CogniteTimeSeries,
        id="TimeSeriesBase",
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
