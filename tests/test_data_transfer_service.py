import pandas as pd
from cognite.data_transfer_service import DataSpec, DataTransferService, TimeSeries, TimeSeriesDataSpec

import pytest


@pytest.fixture
def ts_data_spec_dtos():
    time_series = [TimeSeries(name="constant", aggregates=["step"], missing_data_strategy="ffill"), {"name": "sinus"}]

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
    def test_get_dataframes_dto_spec(self, ts_data_spec_dtos):
        data_spec = DataSpec(time_series_data_specs=ts_data_spec_dtos)
        service = DataTransferService(data_spec)
        dataframes = service.get_dataframes()

        assert isinstance(dataframes.get("ds1"), pd.DataFrame)
        assert isinstance(dataframes.get("ds2"), pd.DataFrame)

    def test_get_dataframes_dict_spec(self, ts_data_spec_dicts):
        data_spec = DataSpec(time_series_data_specs=ts_data_spec_dicts)
        service = DataTransferService(data_spec)
        dataframes = service.get_dataframes()

        assert isinstance(dataframes.get("ds1"), pd.DataFrame)
        assert isinstance(dataframes.get("ds2"), pd.DataFrame)

    def test_dict_dto_equal(self, ts_data_spec_dicts, ts_data_spec_dtos):
        data_spec_dtos = DataSpec(time_series_data_specs=ts_data_spec_dtos)
        data_spec_dicts = DataSpec(time_series_data_specs=ts_data_spec_dicts)
        service = DataTransferService(data_spec_dicts)
        service2 = DataTransferService(data_spec_dtos)
        dataframes_by_dicts = service.get_dataframes()
        dataframes_by_dtos = service2.get_dataframes()

        for df1, df2 in zip(dataframes_by_dtos.values(), dataframes_by_dicts.values()):
            pd.testing.assert_frame_equal(df1, df2)
