import json
from copy import deepcopy
from io import BytesIO
from typing import Dict, List

import pandas as pd

from cognite import config
from cognite._utils import InputError
from cognite.v05 import files, timeseries


class TimeSeries:
    """Object for specifying a specific time series from the TimeSeries API when using a data spec."""

    def __init__(self, name, aggregates=None, missing_data_strategy=None):
        self.name = name
        self.aggregates = aggregates
        self.missing_data_strategy = missing_data_strategy


class TimeSeriesDataSpec:
    """Object for specifying data from the TimeSeries API when using a data spec."""

    def __init__(
        self, time_series, aggregates, granularity, missing_data_strategy=None, start=None, end=None, label=None
    ):
        self.time_series = time_series
        self.aggregates = aggregates
        self.granularity = granularity
        self.missing_data_strategy = missing_data_strategy
        self.start = start
        self.end = end
        self.label = label or "default"


class FilesDataSpec:
    """Object for specifying data from the Files API when using a data spec.

    Attributes:
        file_ids (Dict):    Dictionary of fileNames -> fileIds
    """

    def __init__(self, file_ids):
        self.file_ids = file_ids


class DataSpec:
    """Object for specifying data when querying CDP.

    Attributes:
        time_series_data_specs (List[TimeSeriesDataSpec]):  Time Series data specs
        files_data_spec (FilesDataSpec):                    Files data spec
    """

    def __init__(self, time_series_data_specs: List[TimeSeriesDataSpec] = None, files_data_spec: FilesDataSpec = None):
        self.time_series_data_specs = time_series_data_specs
        self.files_data_spec = files_data_spec
        self.__validate_time_series_data_specs()
        self.__validate_files_data_spec()

    def __validate_time_series_data_specs(self):
        if self.time_series_data_specs:
            labels = []
            for tsds in self.time_series_data_specs:
                if not isinstance(tsds, TimeSeriesDataSpec):
                    raise DataSpecValidationError("Time series data specs must be TimeSeriesDataSpec objects.")

                label = tsds.label or "default"
                if label in labels:
                    raise DataSpecValidationError("Unique labels for each dataspec must be used")
                labels.append(label)

                if not isinstance(tsds.time_series, list):
                    raise DataSpecValidationError("Time series must be a list of time series objects.")

                if not len(tsds.time_series) > 0:
                    raise DataSpecValidationError("A time series data spec does not contain any time series")

                for ts in tsds.time_series:
                    if not isinstance(ts, TimeSeries):
                        raise DataSpecValidationError("Time series must be a TimeSeries object")

    def __validate_files_data_spec(self):
        if self.files_data_spec:
            if not isinstance(self.files_data_spec, FilesDataSpec):
                raise DataSpecValidationError("files_data_spec must be a FilesDataSpec object")

            if not isinstance(self.files_data_spec.file_ids, Dict):
                raise DataSpecValidationError("file_ids must be a dict of form {name: id}")

            for name, id in self.files_data_spec.file_ids.items():
                if not isinstance(name, str):
                    raise DataSpecValidationError("File names must be a strings")
                if not isinstance(id, int):
                    raise DataSpecValidationError("File ids must be integers")

    def to_JSON(self):
        return json.dumps(self.__dict__, cls=self.__DataSpecEncoder)

    @classmethod
    def from_JSON(cls, json_repr):
        json_repr = json.loads(json_repr)
        time_series_data_specs_json = json_repr.get("time_series_data_specs")
        time_series_data_specs = []
        if time_series_data_specs_json:
            for tsds_json in time_series_data_specs_json:
                time_series_json = tsds_json.get("time_series")
                if time_series_json:
                    tsds = TimeSeriesDataSpec(**tsds_json)
                    tsds.time_series = [TimeSeries(**ts) for ts in time_series_json]
                    time_series_data_specs.append(tsds)

        files_data_spec_json = json_repr.get("files_data_spec")
        files_data_spec = None
        if files_data_spec_json:
            files_data_spec = FilesDataSpec(**files_data_spec_json)
        return DataSpec(time_series_data_specs, files_data_spec)

    class __DataSpecEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (TimeSeries, TimeSeriesDataSpec)):
                dict_copy = deepcopy(obj.__dict__)
                for key, value in obj.__dict__.items():
                    if not value:
                        del dict_copy[key]
                return dict_copy
            elif obj is None:
                del obj
            return super(self, self).default(obj)


class DataSpecValidationError(Exception):
    pass


class DataTransferService:
    """Create a Data Transfer Service object.

    Fetch timeseries from the api.
    """

    def __init__(
        self,
        data_spec: DataSpec,
        project: str = None,
        api_key: str = None,
        cookies: Dict = None,
        num_of_processes: int = 10,
    ):
        """
        Args:
            data_spec (data_transfer_service.DataSpec):   Data Spec.
            project (str):          Project name.
            api_key (str):          Api key.
            cookies (dict):         Cookies.
        """
        config_api_key, config_project = config.get_config_variables(api_key, project)

        if not isinstance(data_spec, DataSpec):
            raise InputError("DataTransferService accepts a DataSpec instance.")
        self.data_spec = data_spec
        self.ts_data_specs = data_spec.time_series_data_specs
        self.files_data_spec = data_spec.files_data_spec
        self.api_key = api_key or config_api_key
        self.project = project or config_project
        self.cookies = cookies
        self.num_of_processes = num_of_processes

    def get_dataframes(self):
        """Return a dictionary of dataframes indexed by label - one per data spec."""
        if len(self.ts_data_specs) == 0:
            return {}
        dataframes = {}
        for tsds in self.ts_data_specs:
            ts_list = []
            for ts in tsds.time_series:
                ts_dict = dict(name=ts.name, aggregates=ts.aggregates, missingDataStrategy=ts.missing_data_strategy)
                ts_list.append(ts_dict)

            df = timeseries.get_datapoints_frame(
                ts_list,
                tsds.aggregates,
                tsds.granularity,
                tsds.start,
                tsds.end,
                api_key=self.api_key,
                project=self.project,
                cookies=self.cookies,
                processes=self.num_of_processes,
            )
            df = self.__apply_missing_data_strategies(df, ts_list, tsds.missing_data_strategy)
            dataframes[tsds.label] = df
        return dataframes

    def __apply_missing_data_strategies(self, df, ts_list, global_missing_data_strategy):
        """Applies missing data strategies to dataframe.

        Local strategies have precedence over global strategy.
        """
        new_df = df["timestamp"]
        for ts in ts_list:
            name = ts["name"]
            colnames = [colname for colname in df.columns.values if colname.startswith(name)]

            missing_data_strategy = ts.get("missingDataStrategy", global_missing_data_strategy)
            partial_df = df[colnames]

            if missing_data_strategy == "ffill":
                partial_df = df[colnames].fillna(method="pad")
            elif missing_data_strategy and missing_data_strategy.endswith("Interpolation"):
                method = missing_data_strategy[:-13].lower()
                partial_df = df[colnames].interpolate(method=method, axis=0)

            new_df = pd.concat([new_df, partial_df], axis=1)
        return new_df

    def get_file(self, name):
        """Return files by name as specified in the DataSpec

        Args:
            name (str): Name of file
        """
        if not self.files_data_spec:
            raise InputError("Data spec does not contain a FilesDataSpec")
        if isinstance(self.files_data_spec, FilesDataSpec):
            id = self.files_data_spec.file_ids.get(name)
            if id:
                file_bytes = files.download_file(id, get_contents=True)
                return BytesIO(file_bytes)
            raise InputError("Invalid name")
