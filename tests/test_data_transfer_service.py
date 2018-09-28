import json
import pprint
from io import BytesIO

import pandas as pd

import pytest
from cognite.data_transfer_service import DataSpec, DataTransferService, FilesDataSpec, TimeSeries, TimeSeriesDataSpec


@pytest.fixture
def ts_data_spec_dtos():
    time_series = [
        TimeSeries(name="constant", aggregates=["step"], missing_data_strategy="ffill"),
        TimeSeries(name="sinus"),
    ]

    ts_data_spec1 = TimeSeriesDataSpec(
        time_series=time_series,
        aggregates=["avg"],
        granularity="10m",
        label="ds1",
        start=1522188000000,
        end=1522620000000,
    )
    ts_data_spec2 = TimeSeriesDataSpec(
        time_series=time_series,
        aggregates=["avg"],
        granularity="1h",
        start=1522188000000,
        end=1522620000000,
        missing_data_strategy="linearInterpolation",
        label="ds2",
    )

    yield [ts_data_spec1, ts_data_spec2]


@pytest.fixture
def ts_data_spec_dicts():
    time_series = [{"name": "constant", "aggregates": ["step"], "missingDataStrategy": "ffill"}, {"name": "sinus"}]

    ts_data_spec1 = {
        "timeSeries": time_series,
        "aggregates": ["avg"],
        "granularity": "10m",
        "label": "ds1",
        "start": 1522188000000,
        "end": 1522620000000,
    }
    ts_data_spec2 = {
        "timeSeries": time_series,
        "aggregates": ["avg"],
        "granularity": "1h",
        "start": 1522188000000,
        "end": 1522620000000,
        "missingDataStrategy": "linearInterpolation",
        "label": "ds2",
    }
    yield [ts_data_spec1, ts_data_spec2]


class TestDataTransferService:
    def test_instantiate_data_spec(self, ts_data_spec_dtos):
        DataSpec(ts_data_spec_dtos, files_data_spec=FilesDataSpec(file_ids={"name": 123}))

    def test_instantiate_ts_data_spec_invalid_type(self):
        with pytest.raises(TypeError):
            DataSpec(time_series_data_specs=["str"])

    def test_instantiate_ts_data_spec_duplicate_labels(self):
        with pytest.raises(AssertionError):
            DataSpec(
                time_series_data_specs=[
                    TimeSeriesDataSpec(time_series=[TimeSeries("ts1")], aggregates=["avg"], granularity=["1s"]),
                    TimeSeriesDataSpec(
                        time_series=[TimeSeries("ts1")], aggregates=["avg"], granularity=["1s"], label="default"
                    ),
                ]
            )

    def test_instantiate_ts_data_spec_time_series_not_list(self):
        with pytest.raises(TypeError):
            DataSpec(
                time_series_data_specs=[
                    TimeSeriesDataSpec(time_series=TimeSeries(name="ts1"), aggregates=["avg"], granularity=["1s"])
                ]
            )

    def test_instantiate_ts_data_spec_no_time_series(self):
        with pytest.raises(AssertionError):
            DataSpec(
                time_series_data_specs=[TimeSeriesDataSpec(time_series=[], aggregates=["avg"], granularity=["1s"])]
            )

    def test_instantiate_ts_data_spec_invalid_time_series_types(self):
        with pytest.raises(AssertionError):
            DataSpec(
                time_series_data_specs=[
                    TimeSeriesDataSpec(time_series=[{"name": "ts1"}], aggregates=["avg"], granularity=["1s"])
                ]
            )

    def test_instantiate_files_data_spec_invalid_type(self):
        with pytest.raises(TypeError):
            DataSpec(files_data_spec={"file_ids": [1, 2, 3]})

    def test_instantiate_files_data_spec_file_ids_invalid_type(self):
        with pytest.raises(TypeError):
            DataSpec(files_data_spec=FilesDataSpec(file_ids=[1, 2, 3]))

    def test_instantiate_files_data_spec_file_name_invalid_type(self):
        with pytest.raises(TypeError):
            DataSpec(files_data_spec=FilesDataSpec(file_ids={"f1": 123, 2: 456}))

    def test_instantiate_files_data_spec_file_id_invalid_type(self):
        with pytest.raises(TypeError):
            DataSpec(files_data_spec=FilesDataSpec(file_ids={"f1": 123, "f2": "456"}))

    def test_json_dumps_loads(self, ts_data_spec_dtos, ts_data_spec_dicts):
        data_spec = DataSpec(time_series_data_specs=ts_data_spec_dtos)
        json_repr = data_spec.to_JSON()
        ds = DataSpec.from_JSON(json_repr)
        assert ds.__eq__(data_spec)

    def test_get_dataframes(self, ts_data_spec_dtos):
        data_spec = DataSpec(time_series_data_specs=ts_data_spec_dtos)
        service = DataTransferService(data_spec)
        dataframes = service.get_dataframes()

        assert isinstance(dataframes.get("ds1"), pd.DataFrame)
        assert isinstance(dataframes.get("ds2"), pd.DataFrame)

    def test_get_files(self):
        data_spec = DataSpec(files_data_spec=FilesDataSpec(file_ids={"test": 7725800487412823}))

        dts = DataTransferService(data_spec)
        data = dts.get_file("test")
        assert isinstance(data, BytesIO)
        assert (
            data.getvalue()
            == b'import os\n\nfrom cognite.config import configure_session\nfrom cognite.v05 import files\n\nconfigure_session(os.getenv("COGNITE_TEST_API_KEY"), "mltest")\n\n\nres = files.upload_file("test.py", "./test.py")\n\nprint(res)\n'
        )
