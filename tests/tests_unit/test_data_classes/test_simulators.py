import re

import pytest

from cognite.client.data_classes.simulators.models import (
    SimulatorExternalDependencyFileInternalId,
    SimulatorModelRevisionExternalDependency,
)
from cognite.client.data_classes.simulators.runs import SimulationRunWrite


class TestSimulatorModelRevisionExternalDependency:
    @pytest.mark.parametrize(
        ["input_data"],
        [
            (
                [
                    {"file": {"id": 1111}, "arguments": {"fieldA": "valueA"}},
                    {"file": {"id": 2222}, "arguments": {"fieldB": "valueB"}},
                ],
            )
        ],
    )
    def test_load_list(self, input_data):
        result = SimulatorModelRevisionExternalDependency._load_list(input_data)
        assert isinstance(result, list)
        assert all(isinstance(item, SimulatorModelRevisionExternalDependency) for item in result)
        assert len(result) == 2
        assert isinstance(result[0].file, SimulatorExternalDependencyFileInternalId)
        assert result[0].file.id == 1111
        assert result[0].arguments == {"fieldA": "valueA"}
        assert isinstance(result[0].file, SimulatorExternalDependencyFileInternalId)
        assert result[1].file.id == 2222


class TestSimulationRunWrite:
    def test_error_handling(self):
        with pytest.raises(
            ValueError,
            match=re.escape(
                "Cannot specify both 'routine_external_id' and revision-based parameters ('routine_revision_external_id', 'model_revision_external_id'). Use either routine_external_id alone, or both routine_revision_external_id and model_revision_external_id."
            ),
        ):
            SimulationRunWrite(
                routine_external_id="routine_external_id_1",
                routine_revision_external_id="routine_revision_external_id_1",
                model_revision_external_id="model_revision_external_id_1",
                run_type="external",
            )

        with pytest.raises(
            ValueError,
            match=re.escape(
                "Must specify either 'routine_external_id' alone, or both 'routine_revision_external_id' and 'model_revision_external_id' together."
            ),
        ):
            SimulationRunWrite(
                routine_revision_external_id="routine_revision_external_id_1",
                run_type="external",
            )
