import json
import pprint
from io import BytesIO

import pandas as pd
import pytest

from cognite.data_transfer_service import (
    DataSpec,
    DataSpecValidationError,
    DataTransferService,
    FilesDataSpec,
    TimeSeries,
    TimeSeriesDataSpec,
)


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
        with pytest.raises(DataSpecValidationError):
            DataSpec(time_series_data_specs=["str"])

    def test_instantiate_ts_data_spec_duplicate_labels(self):
        with pytest.raises(DataSpecValidationError):
            DataSpec(
                time_series_data_specs=[
                    TimeSeriesDataSpec(time_series=[TimeSeries("ts1")], aggregates=["avg"], granularity="1s"),
                    TimeSeriesDataSpec(
                        time_series=[TimeSeries("ts1")], aggregates=["avg"], granularity="1s", label="default"
                    ),
                ]
            )

    def test_instantiate_ts_data_spec_time_series_not_list(self):
        with pytest.raises(DataSpecValidationError):
            DataSpec(
                time_series_data_specs=[
                    TimeSeriesDataSpec(time_series=TimeSeries(name="ts1"), aggregates=["avg"], granularity="1s")
                ]
            )

    def test_instantiate_ts_data_spec_no_time_series(self):
        with pytest.raises(DataSpecValidationError):
            DataSpec(time_series_data_specs=[TimeSeriesDataSpec(time_series=[], aggregates=["avg"], granularity="1s")])

    def test_instantiate_ts_data_spec_invalid_time_series_types(self):
        with pytest.raises(DataSpecValidationError):
            DataSpec(
                time_series_data_specs=[
                    TimeSeriesDataSpec(time_series=[{"name": "ts1"}], aggregates=["avg"], granularity="1s")
                ]
            )

    def test_instantiate_files_data_spec_invalid_type(self):
        with pytest.raises(DataSpecValidationError):
            DataSpec(files_data_spec={"file_ids": [1, 2, 3]})

    def test_instantiate_files_data_spec_file_ids_invalid_type(self):
        with pytest.raises(DataSpecValidationError):
            DataSpec(files_data_spec=FilesDataSpec(file_ids=[1, 2, 3]))

    def test_instantiate_files_data_spec_file_name_invalid_type(self):
        with pytest.raises(DataSpecValidationError):
            DataSpec(files_data_spec=FilesDataSpec(file_ids={"f1": 123, 2: 456}))

    def test_instantiate_files_data_spec_file_id_invalid_type(self):
        with pytest.raises(DataSpecValidationError):
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

    @pytest.fixture
    def data_spec(self):
        ts1 = TimeSeries(name="constant", aggregates=["avg", "min"], label="ts1")
        ts2 = TimeSeries(name="constant", aggregates=["cv"], label="ts2")
        ts3 = TimeSeries(name="constant", aggregates=["max", "count"], label="ts3")
        ts4 = TimeSeries(name="constant", aggregates=["step"], label="ts4")

        tsds = TimeSeriesDataSpec(
            time_series=[ts1, ts2, ts3, ts4], aggregates=["avg"], granularity="1h", start="300d-ago"
        )
        ds = DataSpec(time_series_data_specs=[tsds])
        yield ds

    def test_get_dataframes_w_column_mapping(self):
        ts1 = TimeSeries(name="constant", aggregates=["avg"], label="cavg")
        ts2 = TimeSeries(name="constant", aggregates=["cv"], label="ccv")
        ts3 = TimeSeries(name="sinus", aggregates=["avg"], label="sinavg")

        tsds = TimeSeriesDataSpec(time_series=[ts1, ts2, ts3], aggregates=["avg"], granularity="1h", start="300d-ago")

        dts = DataTransferService(DataSpec([tsds]))
        dfs = dts.get_dataframes()
        assert list(dfs["default"].columns.values) == ["timestamp", "cavg", "ccv", "sinavg"]

    def test_get_dataframes_w_column_mapping_and_global_aggregates(self):
        ts1 = TimeSeries(name="constant", aggregates=["avg"], label="cavg")
        ts2 = TimeSeries(name="constant", aggregates=["cv"], label="ccv")
        ts3 = TimeSeries(name="sinus", label="sinavg")

        tsds = TimeSeriesDataSpec(time_series=[ts1, ts2, ts3], aggregates=["avg"], granularity="1h", start="300d-ago")

        dts = DataTransferService(DataSpec([tsds]))
        dfs = dts.get_dataframes()
        assert list(dfs["default"].columns.values) == ["timestamp", "cavg", "ccv", "sinavg"]

    def test_get_dataframes_column_mapping_drop_agg_suffixes(self, data_spec):
        dts = DataTransferService(data_spec, num_of_processes=3)

        dfs = dts.get_dataframes(drop_agg_suffix=True)
        assert list(dfs["default"].columns.values) == [
            "timestamp",
            "ts1|average",
            "ts1|min",
            "ts2",
            "ts3|max",
            "ts3|count",
            "ts4",
        ]

    def test_get_dataframes_column_mapping_no_drop_agg_suffix(self, data_spec):
        dts = DataTransferService(data_spec, num_of_processes=3)

        dfs = dts.get_dataframes(drop_agg_suffix=False)
        assert list(dfs["default"].columns.values) == [
            "timestamp",
            "ts1|average",
            "ts1|min",
            "ts2|continuousvariance",
            "ts3|max",
            "ts3|count",
            "ts4|stepinterpolation",
        ]
