import pandas as pd
import pytest

from cognite.client.data_classes.simulators.runs import (
    SimulationInput,
    SimulationOutput,
    SimulationRunDataItem,
    SimulationValueUnitName,
)


@pytest.mark.dsl
class TestSimulationRunDataItemPandasIntegration:
    def test_to_pandas(self):
        # Create sample data
        unit = SimulationValueUnitName(name="C")
        inputs = [
            SimulationInput(
                reference_id="CWT",
                value=11.0,
                value_type="DOUBLE",
                overridden=True,
                unit=unit,
            ),
            SimulationInput(
                reference_id="CWP",
                value=[5.0],
                value_type="DOUBLE_ARRAY",
                overridden=True,
                unit=SimulationValueUnitName(name="bar"),
            ),
        ]
        outputs = [
            SimulationOutput(
                reference_id="ST",
                simulator_object_reference={"address": "test_out"},
                value=18.5,
                value_type="DOUBLE",
                unit=unit,
            )
        ]

        # Create the SimulationRunDataItem
        data_item = SimulationRunDataItem(run_id=123, inputs=inputs, outputs=outputs)

        # Convert to pandas DataFrame
        df = data_item.to_pandas()

        # Assertions
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (3, 9)  # 3 rows (2 inputs + 1 output), 9 columns

        # Check that the data is correctly formatted
        assert list(df.columns) == [
            "run_id",
            "type",
            "reference_id",
            "value",
            "unit_name",
            "value_type",
            "overridden",
            "timeseries_external_id",
            "address",
        ]

        expected_data_rows = [
            {
                "run_id": 123,
                "type": "Input",
                "reference_id": "CWT",
                "value": 11.0,
                "unit_name": "C",
                "value_type": "DOUBLE",
                "overridden": True,
                "timeseries_external_id": None,
            },
            {
                "run_id": 123,
                "type": "Input",
                "reference_id": "CWP",
                "value": [5.0],
                "unit_name": "bar",
                "value_type": "DOUBLE_ARRAY",
                "overridden": True,
                "timeseries_external_id": None,
            },
            {
                "run_id": 123,
                "type": "Output",
                "reference_id": "ST",
                "value": 18.5,
                "unit_name": "C",
                "value_type": "DOUBLE",
                "overridden": None,
                "timeseries_external_id": None,
                "address": "test_out",
            },
        ]

        expected_df = pd.DataFrame(expected_data_rows)
        pd.testing.assert_frame_equal(df, expected_df)
