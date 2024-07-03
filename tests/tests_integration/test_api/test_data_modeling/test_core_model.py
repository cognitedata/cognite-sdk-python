from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime

import pytest
from _pytest.mark import ParameterSet

from cognite.client import CogniteClient
from cognite.client.data_classes import cdm
from cognite.client.data_classes.data_modeling import Space, SpaceApply, TypedNode, TypedNodeApply

DATA_SPACE = "python_sdk_core_v1_test_space"


def core_model_v1_node_test_cases() -> Iterable[ParameterSet]:
    today = datetime.now()
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


@pytest.fixture(scope="session")
def data_space(cognite_client_alpha: CogniteClient) -> Space:
    space = cognite_client_alpha.data_modeling.spaces.apply(
        SpaceApply(space=DATA_SPACE, description="Test space for core model v1 tests with Python SDK", name=DATA_SPACE)
    )
    return space


class TestCoreModelv1:
    @pytest.mark.usefixtures("data_space")
    @pytest.mark.parametrize("write_instance, read_type", list(core_model_v1_node_test_cases()))
    def test_write_read_delete_node(
        self, write_instance: TypedNodeApply, read_type: type[TypedNode], cognite_client_alpha: CogniteClient
    ) -> None:
        try:
            created = cognite_client_alpha.data_modeling.instances.apply(write_instance)

            assert len(created.nodes) == 1
            assert created.nodes[0].as_id() == write_instance.as_id()

            read = cognite_client_alpha.data_modeling.instances.retrieve_nodes(
                write_instance.as_id(), node_cls=read_type
            )

            assert isinstance(read, read_type)
            assert read.as_id() == write_instance.as_id()

            read_dumped = read.as_write().dump()
            write_instance_dumped = write_instance.dump()
            read_dumped.pop("existingVersion", None)
            write_instance_dumped.pop("existingVersion", None)
            assert write_instance_dumped == read_dumped
        finally:
            cognite_client_alpha.data_modeling.instances.delete(write_instance.as_id())
