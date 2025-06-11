import pytest

from cognite.client.data_classes.simulators.runs import (
    SimulationInput,
    SimulationOutput,
    SimulationRun,
    SimulationRunDataItem,
    SimulationValueUnitName,
    SimulatorRunList,
)


@pytest.mark.dsl
class TestSimulationRunDataItemPandasIntegration:
    def test_to_pandas(self):
        import pandas as pd

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


class TestRunsPandasIntegration:
    def test_run_and_run_list_to_pandas(self):
        import pandas as pd

        # Create a sample SimulationRun
        expected_data = {
            "run_type": "external",
            "run_time": 1760000000000,
            "routine_external_id": "routine",
            "id": 123,
            "created_time": 1700000000000,
            "last_updated_time": 1800000000000,
            "simulator_external_id": "simulator",
            "simulator_integration_external_id": "sim_integration",
            "model_external_id": "model",
            "model_revision_external_id": "model_revision",
            "routine_revision_external_id": "routine_revision",
            "simulation_time": 1750000000000,
            "status": "ready",
            "data_set_id": 456,
            "user_id": "user",
            "log_id": 789,
        }
        run = SimulationRun(**expected_data)
        run_df = run.to_pandas()
        run_list_df = SimulatorRunList([run]).to_pandas()

        # Assertions
        for key in ["run_time", "created_time", "last_updated_time", "simulation_time"]:
            expected_data[key] = pd.to_datetime(expected_data[key], unit="ms")
        expected_df = pd.DataFrame(expected_data, index=["value"])
        pd.testing.assert_frame_equal(run_df, expected_df.T)

        expected_df.index = pd.Index([0])
        expected_df.data_set_id = expected_df.data_set_id.astype("Int64")
        pd.testing.assert_frame_equal(run_list_df, expected_df)
