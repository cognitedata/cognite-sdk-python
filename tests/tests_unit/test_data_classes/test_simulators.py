from __future__ import annotations

import pytest

from cognite.client.data_classes.simulators.routine_revisions import (
    SimulationValueUnitInput,
    SimulatorRoutineConfiguration,
    SimulatorRoutineInputConstant,
    SimulatorRoutineInputTimeseries,
    SimulatorRoutineOutput,
    SimulatorRoutineRevisionWrite,
    SimulatorRoutineStage,
    SimulatorRoutineStep,
    SimulatorRoutineStepArguments,
)
from cognite.client.data_classes.simulators.runs import (
    SimulationInput,
    SimulationOutput,
    SimulationRunDataItem,
    SimulationRunDataList,
    SimulationValueUnitName,
)
from cognite.client.utils._importing import local_import


class TestSimulatorRoutineRevisionCore:
    @pytest.fixture
    def sample_configuration(self) -> SimulatorRoutineConfiguration:
        """Create a sample configuration with inputs and outputs."""
        inputs = [
            SimulatorRoutineInputConstant(
                name="Temperature",
                reference_id="temp_001",
                value=25.5,
                value_type="DOUBLE",
                unit=SimulationValueUnitInput(name="°C", quantity="temperature"),
                save_timeseries_external_id="ts_temp_001",
            ),
            SimulatorRoutineInputTimeseries(
                name="Pressure",
                reference_id="pressure_001",
                source_external_id="ts_pressure_source",
                aggregate="average",
                unit=SimulationValueUnitInput(name="bar", quantity="pressure"),
            ),
        ]

        outputs = [
            SimulatorRoutineOutput(
                name="Flow Rate",
                reference_id="flow_001",
                value_type="DOUBLE",
                unit=SimulationValueUnitInput(name="m³/h", quantity="flow_rate"),
                save_timeseries_external_id="ts_flow_001",
            ),
            SimulatorRoutineOutput(
                name="Efficiency",
                reference_id="eff_001",
                value_type="DOUBLE",
                unit=SimulationValueUnitInput(name="%", quantity="percentage"),
            ),
        ]

        return SimulatorRoutineConfiguration(inputs=inputs, outputs=outputs)

    @pytest.fixture
    def sample_routine_revision(
        self, sample_configuration: SimulatorRoutineConfiguration
    ) -> SimulatorRoutineRevisionWrite:
        """Create a sample routine revision with script stages and steps."""
        steps_stage1 = [
            SimulatorRoutineStep(
                step_type="Get",
                order=1,
                arguments=SimulatorRoutineStepArguments({"referenceId": "temp_001", "unitId": "°C"}),
                description="Get temperature value",
            ),
            SimulatorRoutineStep(
                step_type="Set",
                order=2,
                arguments=SimulatorRoutineStepArguments({"referenceId": "pressure_001", "value": "1.5"}),
                description="Set pressure value",
            ),
        ]

        steps_stage2 = [
            SimulatorRoutineStep(
                step_type="Command",
                order=1,
                arguments=SimulatorRoutineStepArguments({"command": "run_simulation", "timeout": "300"}),
                description="Run simulation",
            ),
            SimulatorRoutineStep(
                step_type="Get",
                order=2,
                arguments=SimulatorRoutineStepArguments({"referenceId": "flow_001"}),
                description="Get flow rate",
            ),
        ]

        # Create stages
        stages = [
            SimulatorRoutineStage(order=1, steps=steps_stage1, description="Initialization stage"),
            SimulatorRoutineStage(order=2, steps=steps_stage2, description="Execution stage"),
        ]

        return SimulatorRoutineRevisionWrite(
            external_id="test_revision",
            routine_external_id="test_routine",
            script=stages,
            configuration=sample_configuration,
        )

    def test_inputs_to_pandas(self, sample_routine_revision: SimulatorRoutineRevisionWrite) -> None:
        pd = local_import("pandas")

        df = sample_routine_revision.inputs_to_pandas()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "name" in df.columns
        assert "reference_id" in df.columns
        assert "unit_name" in df.columns
        assert "unit_quantity" in df.columns
        assert df.iloc[0]["name"] == "Temperature"
        assert df.iloc[1]["name"] == "Pressure"
        assert df.iloc[0]["unit_name"] == "°C"
        assert df.iloc[0]["unit_quantity"] == "temperature"
        assert df.iloc[1]["unit_name"] == "bar"
        assert df.iloc[1]["unit_quantity"] == "pressure"

    def test_outputs_to_pandas(self, sample_routine_revision: SimulatorRoutineRevisionWrite) -> None:
        pd = local_import("pandas")

        df = sample_routine_revision.outputs_to_pandas()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "name" in df.columns
        assert "reference_id" in df.columns
        assert "value_type" in df.columns
        assert "unit_name" in df.columns
        assert "unit_quantity" in df.columns
        assert df.iloc[0]["name"] == "Flow Rate"
        assert df.iloc[1]["name"] == "Efficiency"
        assert df.iloc[0]["unit_name"] == "m³/h"
        assert df.iloc[0]["unit_quantity"] == "flow_rate"
        assert df.iloc[1]["unit_name"] == "%"
        assert df.iloc[1]["unit_quantity"] == "percentage"

    def test_script_to_pandas(self, sample_routine_revision: SimulatorRoutineRevisionWrite) -> None:
        pd = local_import("pandas")

        df = sample_routine_revision.script_to_pandas()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 4  # 2 stages with 2 steps each

        # Check required columns
        assert "stage_order" in df.columns
        assert "stage_description" in df.columns
        assert "step_order" in df.columns
        assert "step_type" in df.columns
        assert "step_description" in df.columns

        # Check argument columns are present (now in snake_case)
        assert "arg_reference_id" in df.columns
        assert "arg_unit_id" in df.columns
        assert "arg_value" in df.columns
        assert "arg_command" in df.columns
        assert "arg_timeout" in df.columns

        # Check values
        assert df.iloc[0]["stage_order"] == 1
        assert df.iloc[0]["stage_description"] == "Initialization stage"
        assert df.iloc[0]["step_order"] == 1
        assert df.iloc[0]["step_type"] == "Get"
        assert df.iloc[0]["step_description"] == "Get temperature value"
        assert df.iloc[0]["arg_reference_id"] == "temp_001"
        assert df.iloc[0]["arg_unit_id"] == "°C"

        assert df.iloc[1]["stage_order"] == 1
        assert df.iloc[1]["step_type"] == "Set"
        assert df.iloc[1]["arg_value"] == "1.5"

        assert df.iloc[2]["stage_order"] == 2
        assert df.iloc[2]["stage_description"] == "Execution stage"
        assert df.iloc[2]["step_type"] == "Command"
        assert df.iloc[2]["arg_command"] == "run_simulation"
        assert df.iloc[2]["arg_timeout"] == "300"

        assert df.iloc[3]["step_type"] == "Get"
        assert df.iloc[3]["arg_reference_id"] == "flow_001"


class TestSimulationRunData:
    @pytest.fixture
    def sample_simulation_inputs(self) -> list[SimulationInput]:
        """Create sample simulation inputs."""
        return [
            SimulationInput(
                reference_id="temp_input",
                value=25.5,
                value_type="DOUBLE",
                unit=SimulationValueUnitName(name="°C"),
                overridden=False,
            ),
            SimulationInput(
                reference_id="pressure_input",
                value=1.5,
                value_type="DOUBLE",
                unit=SimulationValueUnitName(name="bar"),
                simulator_object_reference={"objectName": "Tank1", "objectProperty": "Pressure"},
                overridden=True,
            ),
        ]

    @pytest.fixture
    def sample_simulation_outputs(self) -> list[SimulationOutput]:
        """Create sample simulation outputs."""
        return [
            SimulationOutput(
                reference_id="flow_output",
                value=150.3,
                value_type="DOUBLE",
                unit=SimulationValueUnitName(name="m³/h"),
                timeseries_external_id="ts_flow_001",
            ),
            SimulationOutput(
                reference_id="status_output",
                value="OK",
                value_type="STRING",
            ),
        ]

    @pytest.fixture
    def sample_run_data_item(
        self, sample_simulation_inputs: list[SimulationInput], sample_simulation_outputs: list[SimulationOutput]
    ) -> SimulationRunDataItem:
        """Create a sample SimulationRunDataItem."""
        return SimulationRunDataItem(
            run_id=12345,
            inputs=sample_simulation_inputs,
            outputs=sample_simulation_outputs,
        )

    def test_simulation_run_data_item_to_pandas(self, sample_run_data_item: SimulationRunDataItem) -> None:
        pd = local_import("pandas")

        df = sample_run_data_item.to_pandas()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 4  # 2 inputs + 2 outputs

        # Check required columns
        assert "run_id" in df.columns
        assert "type" in df.columns
        assert "reference_id" in df.columns
        assert "value" in df.columns
        assert "value_type" in df.columns
        assert "unit_name" in df.columns
        assert "overridden" in df.columns
        assert "timeseries_external_id" in df.columns

        # Check values
        assert all(df["run_id"] == 12345)
        assert list(df["type"]) == ["Input", "Input", "Output", "Output"]
        assert df.iloc[0]["reference_id"] == "temp_input"
        assert df.iloc[0]["value"] == 25.5
        assert df.iloc[0]["unit_name"] == "°C"
        assert not df.iloc[0]["overridden"]

        # Check simulator object reference columns (snake_case)
        assert "object_name" in df.columns
        assert "object_property" in df.columns
        assert df.iloc[1]["object_name"] == "Tank1"
        assert df.iloc[1]["object_property"] == "Pressure"

    def test_simulation_run_data_list_to_pandas(
        self, sample_simulation_inputs: list[SimulationInput], sample_simulation_outputs: list[SimulationOutput]
    ) -> None:
        pd = local_import("pandas")

        # Create multiple run data items
        items = [
            SimulationRunDataItem(
                run_id=12345,
                inputs=sample_simulation_inputs[:1],
                outputs=sample_simulation_outputs[:1],
            ),
            SimulationRunDataItem(
                run_id=12346,
                inputs=sample_simulation_inputs[1:],
                outputs=sample_simulation_outputs[1:],
            ),
        ]

        data_list = SimulationRunDataList(items)
        df = data_list.to_pandas()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 4  # 2 items with 2 rows each
        assert list(df["run_id"].unique()) == [12345, 12346]
