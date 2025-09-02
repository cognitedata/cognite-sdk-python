from __future__ import annotations

import pytest

from cognite.client.data_classes.simulators.models import (
    SimulatorModelDependencyFileId,
    SimulatorModelRevisionDependency,
)
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

from cognite.client.data_classes.simulators.simulators import (
    Simulator,
    SimulatorQuantity,
    SimulatorUnitEntry,
)


class TestSimulatorModelRevisionDependency:
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
        result = SimulatorModelRevisionDependency._load_list(input_data)
        assert isinstance(result, list)
        assert all(isinstance(item, SimulatorModelRevisionDependency) for item in result)
        assert len(result) == 2
        assert isinstance(result[0].file, SimulatorModelDependencyFileId)
        assert result[0].file.id == 1111
        assert result[0].arguments == {"fieldA": "valueA"}
        assert isinstance(result[0].file, SimulatorModelDependencyFileId)
        assert result[1].file.id == 2222


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

    @pytest.mark.dsl
    def test_inputs_to_pandas(self, sample_routine_revision: SimulatorRoutineRevisionWrite) -> None:
        pd = local_import("pandas")

        if (
            sample_routine_revision.configuration is not None
            and sample_routine_revision.configuration.inputs is not None
        ):
            df = sample_routine_revision.configuration.inputs.to_pandas()
        else:
            df = pd.DataFrame()

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

    @pytest.mark.dsl
    def test_outputs_to_pandas(self, sample_routine_revision: SimulatorRoutineRevisionWrite) -> None:
        pd = local_import("pandas")

        if (
            sample_routine_revision.configuration is not None
            and sample_routine_revision.configuration.outputs is not None
        ):
            df = sample_routine_revision.configuration.outputs.to_pandas()
        else:
            df = pd.DataFrame()

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

    @pytest.mark.dsl
    def test_script_to_pandas(self, sample_routine_revision: SimulatorRoutineRevisionWrite) -> None:
        pd = local_import("pandas")

        if sample_routine_revision.script is None:
            df = pd.DataFrame()
        else:
            df = sample_routine_revision.script.to_pandas()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 4  # 2 stages with 2 steps each

        # Check MultiIndex structure
        assert isinstance(df.index, pd.MultiIndex)
        assert df.index.names == ["stage_order", "step_order"]

        # Check required columns
        assert "stage_description" in df.columns
        assert "step_type" in df.columns
        assert "step_description" in df.columns

        # Check argument columns are present (now in snake_case)
        assert "arg_reference_id" in df.columns
        assert "arg_unit_id" in df.columns
        assert "arg_value" in df.columns
        assert "arg_command" in df.columns
        assert "arg_timeout" in df.columns

        # Check values
        assert df.index[0] == (1, 1)  # stage_order=1, step_order=1
        assert df.iloc[0]["stage_description"] == "Initialization stage"
        assert df.iloc[0]["step_type"] == "Get"
        assert df.iloc[0]["step_description"] == "Get temperature value"
        assert df.iloc[0]["arg_reference_id"] == "temp_001"
        assert df.iloc[0]["arg_unit_id"] == "°C"

        assert df.index[1] == (1, 2)  # stage_order=1, step_order=2
        assert df.iloc[1]["step_type"] == "Set"
        assert df.iloc[1]["arg_value"] == "1.5"

        assert df.index[2] == (2, 1)  # stage_order=2, step_order=1
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
                simulator_object_reference={
                    "objectName": "Tank1",
                    "objectProperty": "Pressure",
                },
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
        self,
        sample_simulation_inputs: list[SimulationInput],
        sample_simulation_outputs: list[SimulationOutput],
    ) -> SimulationRunDataItem:
        """Create a sample SimulationRunDataItem."""
        return SimulationRunDataItem(
            run_id=12345,
            inputs=sample_simulation_inputs,
            outputs=sample_simulation_outputs,
        )

    @pytest.mark.dsl
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

    @pytest.mark.dsl
    def test_simulation_run_data_list_to_pandas(
        self,
        sample_simulation_inputs: list[SimulationInput],
        sample_simulation_outputs: list[SimulationOutput],
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

class TestSimulator:
    @pytest.fixture
    def simulator_with_quantities(self):
        """Create a simulator with unit quantities for testing."""
        unit_m = SimulatorUnitEntry(label="Meters", name="m")
        unit_ft = SimulatorUnitEntry(label="Feet", name="ft")
        unit_kg = SimulatorUnitEntry(label="Kilograms", name="kg")
        unit_lb = SimulatorUnitEntry(label="Pounds", name="lb")
        unit_c = SimulatorUnitEntry(label="Celsius", name="C")
        unit_f = SimulatorUnitEntry(label="Fahrenheit", name="F")

        quantity_length = SimulatorQuantity(name="length", label="Length", units=[unit_m, unit_ft])
        quantity_mass = SimulatorQuantity(name="mass", label="Mass", units=[unit_kg, unit_lb])
        quantity_temp = SimulatorQuantity(name="temperature", label="Temperature", units=[unit_c, unit_f])

        return Simulator(
            external_id="test-simulator",
            id=1,
            name="Test Simulator",
            file_extension_types=[".sim"],
            unit_quantities=[quantity_length, quantity_mass, quantity_temp],
        )

    @pytest.fixture
    def simulator_without_quantities(self):
        """Create a simulator without unit quantities for testing."""
        return Simulator(
            external_id="empty-simulator",
            id=2,
            name="Empty Simulator",
            file_extension_types=[".sim"],
            unit_quantities=None,
        )

    def test_get_quantities_with_data(self, simulator_with_quantities):
        """Test get_quantities returns correct list when quantities exist."""
        quantities = simulator_with_quantities.get_quantities()
        assert quantities == ["length", "mass", "temperature"]
        assert len(quantities) == 3
        assert all(isinstance(q, str) for q in quantities)

    def test_get_quantities_empty(self, simulator_without_quantities):
        """Test get_quantities returns empty list when no quantities exist."""
        quantities = simulator_without_quantities.get_quantities()
        assert quantities == []
        assert len(quantities) == 0

    def test_get_units_valid_quantity(self, simulator_with_quantities):
        """Test get_units returns correct units for valid quantities."""
        # Test length units
        length_units = simulator_with_quantities.get_units("length")
        assert length_units == ["m", "ft"]
        assert len(length_units) == 2

        # Test mass units
        mass_units = simulator_with_quantities.get_units("mass")
        assert mass_units == ["kg", "lb"]
        assert len(mass_units) == 2

        # Test temperature units
        temp_units = simulator_with_quantities.get_units("temperature")
        assert temp_units == ["C", "F"]
        assert len(temp_units) == 2

    def test_get_units_invalid_quantity(self, simulator_with_quantities):
        """Test get_units raises ValueError for non-existent quantity."""
        with pytest.raises(ValueError) as exc_info:
            simulator_with_quantities.get_units("pressure")

        error_msg = str(exc_info.value)
        assert "Quantity 'pressure' not found" in error_msg
        assert "Available quantities: length, mass, temperature" in error_msg

    def test_get_units_no_quantities(self, simulator_without_quantities):
        """Test get_units raises ValueError when simulator has no quantities."""
        with pytest.raises(ValueError) as exc_info:
            simulator_without_quantities.get_units("any_quantity")

        error_msg = str(exc_info.value)
        assert "Quantity 'any_quantity' not found" in error_msg
        assert "This simulator has no unit quantities defined" in error_msg

    def test_get_units_empty_units_list(self):
        """Test get_units with quantity that has empty units list."""
        empty_quantity = SimulatorQuantity(name="empty", label="Empty Quantity", units=[])

        simulator = Simulator(
            external_id="test-sim",
            id=3,
            name="Test",
            file_extension_types=[".sim"],
            unit_quantities=[empty_quantity],
        )

        units = simulator.get_units("empty")
        assert units == []
        assert len(units) == 0
