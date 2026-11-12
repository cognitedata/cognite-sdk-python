from __future__ import annotations

from datetime import datetime

import pytest

from cognite.client.data_classes.data_modeling.cdm.v1 import (
    Cognite3DModelApply,
    CogniteSourceableNodeApply,
    CogniteStateSet,
    CogniteStateSetApply,
)
from cognite.client.data_classes.data_modeling.data_types import StateSetEntry


class TestSourceable:
    def test_dump_load(self) -> None:
        today = datetime.today()
        source = CogniteSourceableNodeApply(
            "sp_data_space",
            "my_source",
            source_id="source_id",
            source=("sp_data_space", "sap"),
            source_context="imagination",
            source_created_time=today,
            source_updated_time=today,
            source_created_user="Anders",
            source_updated_user="Anders",
        )

        assert source.dump() == {
            "space": "sp_data_space",
            "externalId": "my_source",
            "instanceType": "node",
            "sources": [
                {
                    "source": {
                        "space": "cdf_cdm",
                        "externalId": "CogniteSourceable",
                        "version": "v1",
                        "type": "view",
                    },
                    "properties": {
                        "source": {"space": "sp_data_space", "externalId": "sap"},
                        "sourceContext": "imagination",
                        "sourceId": "source_id",
                        "sourceCreatedTime": today.isoformat(timespec="milliseconds"),
                        "sourceUpdatedTime": today.isoformat(timespec="milliseconds"),
                        "sourceCreatedUser": "Anders",
                        "sourceUpdatedUser": "Anders",
                    },
                }
            ],
        }


class TestModel3D:
    def test_dump(self) -> None:
        my_model = Cognite3DModelApply(
            "sp_data_space",
            "my_model",
            name="The model",
            description="A model",
            model_type="PointCloud",
            aliases=["alias1", "alias2"],
            tags=["tag1", "tag2"],
        )

        dumped = my_model.dump()
        assert dumped == {
            "space": "sp_data_space",
            "externalId": "my_model",
            "instanceType": "node",
            "sources": [
                {
                    "source": {
                        "space": "cdf_cdm",
                        "externalId": "Cognite3DModel",
                        "version": "v1",
                        "type": "view",
                    },
                    "properties": {
                        "name": "The model",
                        "description": "A model",
                        "type": "PointCloud",
                        "aliases": ["alias1", "alias2"],
                        "tags": ["tag1", "tag2"],
                    },
                }
            ],
        }
        dumped_and_loaded = Cognite3DModelApply.load(dumped)
        assert dumped_and_loaded == my_model


@pytest.fixture
def state_set_entries() -> list[StateSetEntry | dict[str, str | int]]:
    return [
        StateSetEntry(-1, "ON", "Power on"),
        StateSetEntry(numeric_value=0, string_value="ALSO ON"),
        {"numeric_value": 2, "string_value": "OFF"},
    ]


class TestStateSet:
    # StateSet has a customer property type (replacement for list[json]), so we have extra test coverage:
    def test_apply_version_dump_load(self, state_set_entries: list[StateSetEntry | dict]) -> None:
        state_set = CogniteStateSetApply(
            "sp_data_space",
            "my_state_set",
            states=state_set_entries,
            name="State set list of state list set entry member yes very",
            type=("just a", "test"),
        )
        expected_dump = {
            "space": "sp_data_space",
            "externalId": "my_state_set",
            "instanceType": "node",
            "type": {"externalId": "test", "space": "just a"},
            "sources": [
                {
                    "source": {
                        "space": "cdf_cdm",
                        "externalId": "CogniteStateSet",
                        "version": "v1",
                        "type": "view",
                    },
                    "properties": {
                        "states": [
                            {"numericValue": -1, "stringValue": "ON", "description": "Power on"},
                            {"numericValue": 0, "stringValue": "ALSO ON"},
                            {"numericValue": 2, "stringValue": "OFF"},
                        ],
                        "name": "State set list of state list set entry member yes very",
                    },
                }
            ],
        }

        dumped = state_set.dump()
        assert expected_dump == dumped

        dumped_and_loaded = CogniteStateSetApply.load(dumped)
        assert dumped_and_loaded == state_set

    def test_read_as_write_preserves_state_entries(self, state_set_entries: list[StateSetEntry | dict]) -> None:
        state_set = CogniteStateSet(
            "sp_data_space",
            "my_state_set",
            version=7,
            last_updated_time=1,
            created_time=1,
            states=state_set_entries,
            name="Valve states",
        )

        write = state_set.as_write()
        assert len(write.states) == 3
        assert write.states[1] == StateSetEntry(numeric_value=0, string_value="ALSO ON")

        dumped = write.dump()
        assert dumped["sources"][0]["properties"]["states"][1] == {"numericValue": 0, "stringValue": "ALSO ON"}
