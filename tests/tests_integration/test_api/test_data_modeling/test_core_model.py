from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from _pytest.mark import ParameterSet

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    Space,
    SpaceApply,
    TypedEdge,
    TypedEdgeApply,
    TypedNode,
    TypedNodeApply,
)
from cognite.client.data_classes.data_modeling.cdm import v1 as cdm
from cognite.client.utils._time import datetime_to_ms_iso_timestamp

DATA_SPACE = "python_sdk_core_v1_test_space"
TODAY = datetime.now(timezone.utc)
TODAY_STR = datetime_to_ms_iso_timestamp(TODAY)
YESTERDAY = TODAY - timedelta(days=1)
YESTERDAY_STR = datetime_to_ms_iso_timestamp(YESTERDAY)


def asset_apply() -> cdm.CogniteAssetApply:
    return cdm.CogniteAssetApply(
        space=DATA_SPACE,
        external_id="test_asset",
        source_id="SAP",
        source_created_time=TODAY,
        source_updated_time=TODAY,
        source_created_user="Me",
        source_updated_user="Also me",
        name="Test asset",
        description="Test asset for core model v1 tests with Python SDK",
        aliases=["test_asset_alias"],
    )


def activity_apply() -> cdm.CogniteActivityApply:
    return cdm.CogniteActivityApply(
        space=DATA_SPACE,
        external_id="test_activity",
        assets=[(DATA_SPACE, "test_asset")],
        source_id="SAP",
        source_created_time=YESTERDAY,
        source_updated_time=TODAY,
        source_created_user="Me",
        source_updated_user="Also me",
        name="Test activity",
        description="Test activity for core model v1 tests with Python SDK",
        aliases=["test_activity_alias"],
        start_time=YESTERDAY,
        end_time=TODAY,
        scheduled_start_time=YESTERDAY,
        scheduled_end_time=TODAY,
    )


def asset_class_apply() -> cdm.CogniteAssetClassApply:
    return cdm.CogniteAssetClassApply(
        space=DATA_SPACE,
        external_id="test_asset_type",
        name="Test asset type",
        description="Test asset type for core model v1 tests with Python SDK",
        aliases=["test_asset_type_alias"],
        code="101",
        standard="ISO",
    )


def describable_node_apply() -> cdm.CogniteDescribableNodeApply:
    return cdm.CogniteDescribableNodeApply(
        space=DATA_SPACE,
        external_id="test_describable",
        name="Test describable",
        description="Test describable for core model v1 tests with Python SDK",
        aliases=["test_describable_alias"],
    )


def equipment_apply() -> cdm.CogniteEquipmentApply:
    return cdm.CogniteEquipmentApply(
        space=DATA_SPACE,
        external_id="test_equipment",
        name="Test equipment",
        description="Test equipment for core model v1 tests with Python SDK",
        aliases=["test_equipment_alias"],
        source_id="SAP",
        source_created_time=YESTERDAY,
        source_updated_time=TODAY,
        source_created_user="Me",
        source_updated_user="Also me",
    )


def threed_model_apply() -> cdm.Cognite3DModelApply:
    return cdm.Cognite3DModelApply(
        space=DATA_SPACE,
        external_id="test_model_3d",
        name="Test model 3D",
        description="Test model 3D for core model v1 tests with Python SDK",
        aliases=["test_model_3d_alias"],
        model_type="PointCloud",  # TODO: Should be nullable, returns as 400 if not set
    )


def threed_object_apply() -> cdm.Cognite3DObjectApply:
    return cdm.Cognite3DObjectApply(
        space=DATA_SPACE,
        external_id="test_object_3d",
        name="Test object 3D",
        description="Test object 3D for core model v1 tests with Python SDK",
        aliases=["test_object_3d_alias"],
    )


def schedulable_apply() -> cdm.CogniteSchedulableApply:
    return cdm.CogniteSchedulableApply(
        space=DATA_SPACE,
        external_id="test_schedulable",
        start_time=YESTERDAY,
        end_time=TODAY,
        scheduled_start_time=YESTERDAY,
        scheduled_end_time=TODAY,
    )


def sourceable_node_apply() -> cdm.CogniteSourceableNodeApply:
    return cdm.CogniteSourceableNodeApply(
        space=DATA_SPACE,
        external_id="test_sourceable",
        source_id="SAP",
        source_created_time=YESTERDAY,
        source_updated_time=TODAY,
        source_created_user="Me",
        source_updated_user="Also me",
    )


def time_series_apply() -> cdm.CogniteTimeSeriesApply:
    return cdm.CogniteTimeSeriesApply(
        space=DATA_SPACE,
        external_id="test_time_series_base",
        time_series_type="numeric",
        is_step=False,
        name="Test time series base",
        description="Test time series base for core model v1 tests with Python SDK",
        aliases=["test_time_series_base_alias"],
        source_id="SAP",
        source_created_time=YESTERDAY,
        source_updated_time=TODAY,
        source_created_user="Me",
        source_updated_user="Also me",
    )


def annotation_apply() -> cdm.CogniteAnnotationApply:
    return cdm.CogniteAnnotationApply(
        space=DATA_SPACE,
        external_id="test_connection",
        type=(DATA_SPACE, "Flow"),
        start_node=(DATA_SPACE, "source"),
        end_node=(DATA_SPACE, "target"),
        status="Suggested",
        source_context="SAP",
        name="Test connection",
    )


def sourceable_edge_apply() -> cdm.CogniteSourceableEdgeApply:
    return cdm.CogniteSourceableEdgeApply(
        space=DATA_SPACE,
        external_id="test_sourceable_edge",
        type=(DATA_SPACE, "Flow"),
        start_node=(DATA_SPACE, "source"),
        end_node=(DATA_SPACE, "target"),
        source_context="SAP",
    )


def exp_asset_apply_dump() -> dict[str, Any]:
    return {
        "properties": {
            "aliases": ["test_asset_alias"],
            "description": "Test asset for core model v1 tests with Python SDK",
            "name": "Test asset",
            "sourceCreatedTime": TODAY_STR,
            "sourceCreatedUser": "Me",
            "sourceId": "SAP",
            "sourceUpdatedTime": TODAY_STR,
            "sourceUpdatedUser": "Also me",
        },
        "source": {
            "externalId": "CogniteAsset",
            "space": "cdf_cdm",
            "type": "view",
            "version": "v1",
        },
    }


def exp_activity_apply_dump() -> dict[str, Any]:
    return {
        "properties": {
            "name": "Test activity",
            "description": "Test activity for core model v1 tests with Python SDK",
            "aliases": ["test_activity_alias"],
            "sourceId": "SAP",
            "sourceCreatedTime": YESTERDAY_STR,
            "sourceUpdatedTime": TODAY_STR,
            "sourceCreatedUser": "Me",
            "sourceUpdatedUser": "Also me",
            "startTime": YESTERDAY_STR,
            "endTime": TODAY_STR,
            "scheduledStartTime": YESTERDAY_STR,
            "scheduledEndTime": TODAY_STR,
            "assets": [{"space": "python_sdk_core_v1_test_space", "externalId": "test_asset"}],
        },
        "source": {"space": "cdf_cdm", "externalId": "CogniteActivity", "version": "v1", "type": "view"},
    }


def exp_asset_class_apply_dump() -> dict[str, Any]:
    return {
        "properties": {
            "name": "Test asset type",
            "description": "Test asset type for core model v1 tests with Python SDK",
            "aliases": ["test_asset_type_alias"],
            "code": "101",
            "standard": "ISO",
        },
        "source": {"space": "cdf_cdm", "externalId": "CogniteAssetClass", "version": "v1", "type": "view"},
    }


def exp_describable_node_apply_dump() -> dict[str, Any]:
    return {
        "properties": {
            "name": "Test describable",
            "description": "Test describable for core model v1 tests with Python SDK",
            "aliases": ["test_describable_alias"],
        },
        "source": {"space": "cdf_cdm", "externalId": "CogniteDescribable", "version": "v1", "type": "view"},
    }


def exp_equipment_apply_dump() -> dict[str, Any]:
    return {
        "properties": {
            "name": "Test equipment",
            "description": "Test equipment for core model v1 tests with Python SDK",
            "aliases": ["test_equipment_alias"],
            "sourceId": "SAP",
            "sourceCreatedTime": YESTERDAY_STR,
            "sourceUpdatedTime": TODAY_STR,
            "sourceCreatedUser": "Me",
            "sourceUpdatedUser": "Also me",
        },
        "source": {"space": "cdf_cdm", "externalId": "CogniteEquipment", "version": "v1", "type": "view"},
    }


def exp_threed_model_apply_dump() -> dict[str, Any]:
    return {
        "properties": {
            "name": "Test model 3D",
            "description": "Test model 3D for core model v1 tests with Python SDK",
            "aliases": ["test_model_3d_alias"],
            "type": "PointCloud",
        },
        "source": {"space": "cdf_cdm", "externalId": "Cognite3DModel", "version": "v1", "type": "view"},
    }


def exp_threed_object_apply_dump() -> dict[str, Any]:
    return {
        "properties": {
            "name": "Test object 3D",
            "description": "Test object 3D for core model v1 tests with Python SDK",
            "aliases": ["test_object_3d_alias"],
        },
        "source": {"space": "cdf_cdm", "externalId": "Cognite3DObject", "version": "v1", "type": "view"},
    }


def exp_schedulable_apply_dump() -> dict[str, Any]:
    return {
        "properties": {
            "startTime": YESTERDAY_STR,
            "endTime": TODAY_STR,
            "scheduledStartTime": YESTERDAY_STR,
            "scheduledEndTime": TODAY_STR,
        },
        "source": {"space": "cdf_cdm", "externalId": "CogniteSchedulable", "version": "v1", "type": "view"},
    }


def exp_sourceable_node_apply_dump() -> dict[str, Any]:
    return {
        "properties": {
            "sourceId": "SAP",
            "sourceCreatedTime": YESTERDAY_STR,
            "sourceUpdatedTime": TODAY_STR,
            "sourceCreatedUser": "Me",
            "sourceUpdatedUser": "Also me",
        },
        "source": {"space": "cdf_cdm", "externalId": "CogniteSourceable", "version": "v1", "type": "view"},
    }


def exp_time_series_apply_dump() -> dict[str, Any]:
    return {
        "properties": {
            "isStep": False,
            "type": "numeric",
            "name": "Test time series base",
            "description": "Test time series base for core model v1 tests with Python SDK",
            "aliases": ["test_time_series_base_alias"],
            "sourceId": "SAP",
            "sourceCreatedTime": YESTERDAY_STR,
            "sourceUpdatedTime": TODAY_STR,
            "sourceCreatedUser": "Me",
            "sourceUpdatedUser": "Also me",
        },
        "source": {"space": "cdf_cdm", "externalId": "CogniteTimeSeries", "version": "v1", "type": "view"},
    }


def exp_annotation_apply_dump() -> dict[str, Any]:
    return {
        "properties": {"name": "Test connection", "status": "Suggested", "sourceContext": "SAP"},
        "source": {"space": "cdf_cdm", "externalId": "CogniteAnnotation", "version": "v1", "type": "view"},
    }


def exp_sourceable_edge_apply_dump() -> dict[str, Any]:
    return {
        "properties": {"sourceContext": "SAP"},
        "source": {"space": "cdf_cdm", "externalId": "CogniteSourceable", "version": "v1", "type": "view"},
    }


def core_model_v1_node_test_cases() -> Iterable[ParameterSet]:
    yield pytest.param(asset_apply(), exp_asset_apply_dump(), cdm.CogniteAsset, id="CogniteAsset")
    yield pytest.param(activity_apply(), exp_activity_apply_dump(), cdm.CogniteActivity, id="CogniteActivity")
    yield pytest.param(asset_class_apply(), exp_asset_class_apply_dump(), cdm.CogniteAssetClass, id="CogniteAssetClass")
    yield pytest.param(
        describable_node_apply(),
        exp_describable_node_apply_dump(),
        cdm.CogniteDescribableNode,
        id="CogniteDescribableNode",
    )
    yield pytest.param(equipment_apply(), exp_equipment_apply_dump(), cdm.CogniteEquipment, id="CogniteEquipment")
    yield pytest.param(threed_model_apply(), exp_threed_model_apply_dump(), cdm.Cognite3DModel, id="CogniteModel3D")
    yield pytest.param(threed_object_apply(), exp_threed_object_apply_dump(), cdm.Cognite3DObject, id="CogniteObject3D")
    yield pytest.param(
        schedulable_apply(), exp_schedulable_apply_dump(), cdm.CogniteSchedulable, id="CogniteSchedulable"
    )
    yield pytest.param(
        sourceable_node_apply(), exp_sourceable_node_apply_dump(), cdm.CogniteSourceableNode, id="CogniteSourceableNode"
    )
    yield pytest.param(time_series_apply(), exp_time_series_apply_dump(), cdm.CogniteTimeSeries, id="CogniteTimeSeries")


def core_model_v1_edge_test_cases() -> Iterable[ParameterSet]:
    yield pytest.param(annotation_apply(), exp_annotation_apply_dump(), cdm.CogniteAnnotation, id="CogniteAnnotation")
    yield pytest.param(
        sourceable_edge_apply(), exp_sourceable_edge_apply_dump(), cdm.CogniteSourceableEdge, id="CogniteSourceableEdge"
    )


@pytest.fixture(scope="session")
def data_space(cognite_client: CogniteClient) -> Space:
    space = cognite_client.data_modeling.spaces.apply(
        SpaceApply(space=DATA_SPACE, description="Test space for core model v1 tests with Python SDK", name=DATA_SPACE)
    )
    return space


class TestCoreModelv1:
    @pytest.mark.usefixtures("data_space")
    @pytest.mark.parametrize("write_instance, exp_prop_dump, read_type", list(core_model_v1_node_test_cases()))
    def test_write_read_node(
        self,
        write_instance: TypedNodeApply,
        exp_prop_dump: dict[str, Any],
        read_type: type[TypedNode],
        cognite_client: CogniteClient,
    ) -> None:
        created = cognite_client.data_modeling.instances.apply(write_instance)

        assert len(created.nodes) == 1
        assert created.nodes[0].as_id() == write_instance.as_id()

        read = cognite_client.data_modeling.instances.retrieve_nodes(write_instance.as_id(), node_cls=read_type)

        assert isinstance(read, read_type)
        assert read.as_id() == write_instance.as_id()

        write_instance_dumped = write_instance.dump()
        exp_dump = {
            "externalId": write_instance.external_id,
            "instanceType": "node",
            "sources": [exp_prop_dump],
            "space": "python_sdk_core_v1_test_space",
            "type": None,
        }
        assert write_instance_dumped == exp_dump

        read_dumped = read.as_write().dump()
        # The existing version will be bumped by the server, so we need to remove it from the comparison
        read_dumped.pop("existingVersion", None)

        non_null_properties = exp_prop_dump["properties"]
        for k, v in read_dumped["sources"][0]["properties"].items():
            # On the read-version, we expect all properties to be present, but those we didn't specify in
            # the apply-class should be None or empty list:
            if k not in non_null_properties:
                assert v in (None, [])
            else:
                assert v == non_null_properties[k]

    @pytest.mark.usefixtures("data_space")
    @pytest.mark.parametrize("write_instance, exp_prop_dump, read_type", list(core_model_v1_edge_test_cases()))
    def test_write_read_edge(
        self,
        write_instance: TypedEdgeApply,
        exp_prop_dump: dict[str, Any],
        read_type: type[TypedEdge],
        cognite_client: CogniteClient,
    ) -> None:
        created = cognite_client.data_modeling.instances.apply(
            edges=write_instance, auto_create_start_nodes=True, auto_create_end_nodes=True
        )

        assert len(created.edges) == 1
        assert created.edges[0].as_id() == write_instance.as_id()

        read = cognite_client.data_modeling.instances.retrieve_edges(write_instance.as_id(), edge_cls=read_type)

        assert isinstance(read, read_type)
        assert read.as_id() == write_instance.as_id()

        write_instance_dumped = write_instance.dump()
        exp_dump = {
            "externalId": write_instance.external_id,
            "instanceType": "edge",
            "sources": [exp_prop_dump],
            "space": "python_sdk_core_v1_test_space",
            "startNode": {"externalId": "source", "space": "python_sdk_core_v1_test_space"},
            "endNode": {"externalId": "target", "space": "python_sdk_core_v1_test_space"},
            "type": {"externalId": "Flow", "space": "python_sdk_core_v1_test_space"},
        }
        assert write_instance_dumped == exp_dump

        read_dumped = read.as_write().dump()
        # The existing version will be bumped by the server, so we need to remove it from the comparison
        read_dumped.pop("existingVersion", None)

        non_null_properties = exp_prop_dump["properties"]
        for k, v in read_dumped["sources"][0]["properties"].items():
            # On the read-version, we expect all properties to be present, but those we didn't specify in
            # the apply-class should be None or empty list:
            if k not in non_null_properties:
                assert v in (None, [])
            else:
                assert v == non_null_properties[k]
