import pytest

from cognite.client.data_classes.simulators.models import (
    SimulatorModelDependencyFileId,
    SimulatorModelRevisionDependency,
)
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
    def test_load_list(
        self,
        input_data: list[dict[str, dict]],
    ) -> None:
        result = SimulatorModelRevisionDependency._load_list(input_data)
        assert isinstance(result, list)
        assert all(isinstance(item, SimulatorModelRevisionDependency) for item in result)
        assert len(result) == 2
        assert isinstance(result[0].file, SimulatorModelDependencyFileId)
        assert result[0].file.id == 1111
        assert result[0].arguments == {"fieldA": "valueA"}
        assert isinstance(result[1].file, SimulatorModelDependencyFileId)
        assert result[1].file.id == 2222


class TestSimulator:
    @pytest.fixture
    def simulator_with_quantities(self) -> Simulator:
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
    def simulator_without_quantities(self) -> Simulator:
        """Create a simulator without unit quantities for testing."""
        return Simulator(
            external_id="empty-simulator",
            id=2,
            name="Empty Simulator",
            file_extension_types=[".sim"],
            unit_quantities=None,
        )

    def test_get_quantities_with_data(self, simulator_with_quantities: Simulator) -> None:
        """Test get_quantities returns correct list when quantities exist."""
        quantities = simulator_with_quantities.get_quantities()
        assert quantities == ["length", "mass", "temperature"]
        assert len(quantities) == 3
        assert all(isinstance(q, str) for q in quantities)

    def test_get_quantities_empty(self, simulator_without_quantities: Simulator) -> None:
        """Test get_quantities returns empty list when no quantities exist."""
        quantities = simulator_without_quantities.get_quantities()
        assert quantities == []
        assert len(quantities) == 0

    def test_get_units_valid_quantity(self, simulator_with_quantities: Simulator) -> None:
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

    def test_get_units_invalid_quantity(self, simulator_with_quantities: Simulator) -> None:
        """Test get_units raises ValueError for non-existent quantity."""
        with pytest.raises(ValueError) as exc_info:
            simulator_with_quantities.get_units("pressure")

        error_msg = str(exc_info.value)
        assert "Quantity 'pressure' not found" in error_msg
        assert "Available quantities: length, mass, temperature" in error_msg

    def test_get_units_no_quantities(self, simulator_without_quantities: Simulator) -> None:
        """Test get_units raises ValueError when simulator has no quantities."""
        with pytest.raises(ValueError) as exc_info:
            simulator_without_quantities.get_units("any_quantity")

        error_msg = str(exc_info.value)
        assert "Quantity 'any_quantity' not found" in error_msg
        assert "This simulator has no unit quantities defined" in error_msg

    def test_get_units_empty_units_list(self) -> None:
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
