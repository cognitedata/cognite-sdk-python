import json
from copy import deepcopy
from datetime import datetime
from io import BytesIO
from typing import Dict, List, Union

import pandas as pd
from cognite import config
from cognite._utils import InputError, get_aggregate_func_return_name, to_camel_case, to_snake_case
from cognite.v05 import files
from cognite.v05 import timeseries as time_series_v05
from cognite.v06 import time_series as time_series_v06


class TimeSeries:
    """Object for specifying a specific time series from the TimeSeries API when using a data spec.

    Args:
        id (int): id of time series to retrieve
        aggregates (List[str]): Local aggregate functions to apply
        missing_data_strategy(str): Missing data strategy to apply
        label (str): name of the column in the resulting data frame when passed to data transfer service.

    Examples:
        When you supply a label the resulting data frames produced by data transfer service
        will use the label as column names.
    """

    def __init__(self, id: int, aggregates: List[str] = None, missing_data_strategy: str = None, label: str = None):
        self.id = id
        self.aggregates = aggregates
        self.missing_data_strategy = missing_data_strategy
        self.label = label
        self._name = None


class TimeSeriesDataSpec:
    """Object for specifying data from the TimeSeries API when using a data spec.

    Args:
        time_series (List[data_transfer_service.TimeSeries]):  Time series
        aggregates (List[str]): List of aggregate functions
        granularity (str): Granularity of aggregates
        missing_data_strategy (str): Missing data strategy to apply, can be "linearInterpolation" or "ffill"
        start (Union[str, int, datetime]): Start time
        end (Union[str, int, datetime]): end time
        label (str): Label for this data spec

    Examples:
        When you specify a label, data transfer service will exhibit the following behaviour::

            ts_data_spec = TimeSeriesDataSpec(..., label="some_label")
            data_spec = DataSpec([ts_data_spec], ...)
            dts = DataTransferService(data_spec)
            dataframes = dts.get_dataframes
            my_df = dataframes["some_label"]
    """

    def __init__(
        self,
        time_series: List[TimeSeries],
        aggregates: List[str],
        granularity: str,
        missing_data_strategy: str = None,
        start: Union[str, int, datetime] = None,
        end: Union[str, int, datetime] = None,
        label: str = None,
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

    Args:
        file_ids (Dict[str, int]):    Dictionary of fileNames -> fileIds
    """

    def __init__(self, file_ids: Dict[str, int]):
        self.file_ids = file_ids


class DataSpec:
    """Object for specifying data when querying CDP.

    Args:
        time_series_data_specs (List[TimeSeriesDataSpec]):  Time Series data specs
        files_data_spec (FilesDataSpec):                    Files data spec

    Raises:
        DataSpecValidationError: An error occurred while validating the data spec

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

                ts_labels = []
                for ts in tsds.time_series:
                    if not isinstance(ts, TimeSeries):
                        raise DataSpecValidationError("Time series must be a TimeSeries object")
                    if ts.label and ts.label in ts_labels:
                        raise DataSpecValidationError("Time series labels must be unique")
                    ts_labels.append(ts.label)

    def __validate_files_data_spec(self):
        if self.files_data_spec:
            if not isinstance(self.files_data_spec, FilesDataSpec):
                raise DataSpecValidationError("files_data_spec must be a FilesDataSpec object")

            if not isinstance(self.files_data_spec.file_ids, dict):
                raise DataSpecValidationError("file_ids must be a dict of form {name: id}")

            for name, id in self.files_data_spec.file_ids.items():
                if not isinstance(name, str):
                    raise DataSpecValidationError("File names must be a strings")
                if not isinstance(id, int):
                    raise DataSpecValidationError("File ids must be integers")

    def to_JSON(self):
        return DataSpec._to_json(self)

    def __str__(self):
        return json.dumps(self.to_JSON(), indent=4)

    @staticmethod
    def _to_json(obj):
        if isinstance(obj, (DataSpec, TimeSeries, FilesDataSpec, TimeSeriesDataSpec)):
            return DataSpec._to_json(obj.__dict__)
        if isinstance(obj, dict):
            return {to_camel_case(key): DataSpec._to_json(value) for key, value in obj.items() if (value is not None)}
        elif isinstance(obj, list):
            new_list = []
            for el in obj:
                new_list.append(DataSpec._to_json(el))
            return new_list
        elif isinstance(obj, (str, int, float, bool)) or obj is None:
            return obj
        raise AssertionError("Data spec does not accept type {}".format(type(obj)))

    @classmethod
    def from_JSON(cls, json_repr):
        if isinstance(json_repr, str):
            json_repr = json.loads(json_repr)

        if not isinstance(json_repr, dict):
            raise DataSpecValidationError("from_JSON accepts a dict")

        time_series_data_specs_json = json_repr.get("timeSeriesDataSpecs")
        files_data_spec_json = json_repr.get("filesDataSpec")

        if not (time_series_data_specs_json or files_data_spec_json):
            raise DataSpecValidationError("Data spec must include at least one of [timeSeriesDataSpec, filesDataSpec]")

        time_series_data_specs = []
        if time_series_data_specs_json:
            for tsds_json in time_series_data_specs_json:
                time_series_json = tsds_json.get("timeSeries")
                if time_series_json:
                    tsds = TimeSeriesDataSpec(**{to_snake_case(key): value for key, value in tsds_json.items()})
                    tsds.time_series = [
                        TimeSeries(**{to_snake_case(key): value for key, value in ts.items()})
                        for ts in time_series_json
                    ]
                    time_series_data_specs.append(tsds)

        files_data_spec = None
        if files_data_spec_json:
            files_data_spec = FilesDataSpec(
                **{to_snake_case(key): value for key, value in files_data_spec_json.items()}
            )
        return DataSpec(time_series_data_specs, files_data_spec)


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

        if isinstance(data_spec, DataSpec):
            self.data_spec = deepcopy(data_spec)
        elif isinstance(data_spec, dict):
            self.data_spec = DataSpec.from_JSON(data_spec)
        else:
            raise InputError("DataTransferService accepts a DataSpec instance or a json object representation of it.")
        self.ts_data_specs = self.data_spec.time_series_data_specs
        self.files_data_spec = self.data_spec.files_data_spec
        self.api_key = api_key or config_api_key
        self.project = project or config_project
        self.cookies = cookies
        self.num_of_processes = num_of_processes

    def get_time_series_name(self, ts_label: str, dataframe_label: str = "default"):
        if self.ts_data_specs is None:
            raise InputError("Data spec does not contain any TimeSeriesDataSpecs")

        tsds = None
        for ts_data_spec in self.ts_data_specs:
            if ts_data_spec.label == dataframe_label:
                tsds = ts_data_spec

        if tsds:
            # Temporary workaround that you cannot use get_datapoints_frame with ts id.
            ts_res = time_series_v06.get_multiple_time_series_by_id(
                ids=list(set([ts.id for ts in tsds.time_series])), api_key=self.api_key, project=self.project
            )
            id_to_name = {ts["id"]: ts["name"] for ts in ts_res.to_json()}

            for ts in tsds.time_series:
                if ts.label == ts_label:
                    return id_to_name[ts.id]
            raise InputError("Invalid time series label")
        raise InputError("Invalid dataframe label")

    def get_dataframes(self, drop_agg_suffix: bool = True):
        """Return a dictionary of dataframes indexed by label - one per data spec.

        Args:
            drop_agg_suffix (bool): If a time series has only one aggregate, drop the `|<agg-func>` suffix on
                                    those column names.
        Returns:
            Dict[str, pd.DataFrame]: A label-indexed dictionary of data frames.
        """
        if len(self.ts_data_specs) == 0:
            return {}
        if self.ts_data_specs is None:
            raise InputError("Data spec does not contain any TimeSeriesDataSpecs")

        dataframes = {}
        for tsds in self.ts_data_specs:
            df = self.get_dataframe(tsds.label, drop_agg_suffix=drop_agg_suffix)
            dataframes[tsds.label] = df
        return dataframes

    def get_dataframe(self, label: str = "default", drop_agg_suffix: bool = True):
        if self.ts_data_specs is None:
            raise InputError("Data spec does not contain any TimeSeriesDataSpecs")

        tsds = None
        for ts_data_spec in self.ts_data_specs:
            if ts_data_spec.label == label:
                tsds = ts_data_spec
        if tsds:
            ts_list = []
            # Temporary workaround that you cannot use get_datapoints_frame with ts id.
            ts_res = time_series_v06.get_multiple_time_series_by_id(
                ids=list(set([ts.id for ts in tsds.time_series])), api_key=self.api_key, project=self.project
            )
            id_to_name = {ts["id"]: ts["name"] for ts in ts_res.to_json()}

            for ts in tsds.time_series:
                ts._name = id_to_name[ts.id]
                if not ts.label:
                    ts.label = id_to_name[ts.id]
                ts_dict = dict(
                    name=id_to_name[ts.id],
                    aggregates=ts.aggregates or tsds.aggregates,
                    missingDataStrategy=ts.missing_data_strategy,
                )
                ts_list.append(ts_dict)

            df = time_series_v05.get_datapoints_frame(
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
            df = self.__convert_ts_names_to_labels(df, tsds, drop_agg_suffix)
            return df
        raise InputError("Invalid label")

    def get_file(self, name):
        """Return files by name as specified in the DataSpec

        Args:
            name (str): Name of file
        """
        if not self.files_data_spec or not isinstance(self.files_data_spec, FilesDataSpec):
            raise InputError("Data spec does not contain a FilesDataSpec")
        id = self.files_data_spec.file_ids.get(name)
        if id:
            file_bytes = files.download_file(id, get_contents=True, api_key=self.api_key, project=self.project)
            return BytesIO(file_bytes)
        raise InputError("Invalid name")

    def __convert_ts_names_to_labels(self, df: pd.DataFrame, tsds: TimeSeriesDataSpec, drop_agg_suffix: bool):
        name_to_label = {}
        for ts in tsds.time_series:
            for agg in ts.aggregates or tsds.aggregates:
                agg_return_name = get_aggregate_func_return_name(agg)
                if len(ts.aggregates or tsds.aggregates) > 1 or not drop_agg_suffix:
                    name_to_label[ts._name + "|" + agg_return_name] = ts.label + "|" + agg_return_name
                else:
                    name_to_label[ts._name + "|" + agg_return_name] = ts.label
        return df.rename(columns=name_to_label)

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
            new_df = new_df.iloc[:, ~new_df.columns.duplicated()]
        return new_df
