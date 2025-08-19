import pytest

from cognite.client.data_classes.simulators.models import (
    SimulatorExternalDependencyFileInternalId,
    SimulatorModelRevisionExternalDependency,
)


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
