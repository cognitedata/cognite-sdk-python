from typing import List, Optional, Union

import pandas as pd
from cognite.client import CogniteClient

from python.cognite_units.unit_converter import preprocess_unit, unit_convert


def retrieve_datapoints(
    client: CogniteClient,
    external_id: str,
    start_time: Union[int, pd.Timestamp, str],
    end_time: Union[int, pd.Timestamp, str],
    aggregates: List[str],
    granularity: str,
    input_unit: Optional[str] = None,
    output_unit: Optional[str] = None,
    retrieve_original_units: bool = False,
) -> pd.DataFrame:
    """
    Retrieve datapoints from a time series and convert them to the
    specified unit.

    Args:
        client (CogniteClient): The CDF client to use
        external_id (str): The time series external id
        start_time (Union[int, pd.Timestamp, str]): The start time
        of the datapoints to retrieve
        end_time (Union[int, pd.Timestamp, str]): The end time of
        the datapoints to retrieve
        aggregates (List[str]): The aggregates to retrieve
        granularity (str): The granularity of the datapoints to retrieve
        input_unit (Optional[str]): The unit to convert from
        output_unit (Optional[str]): The unit to convert to
        retrieve_original_units (bool): Whether to retrieve the original
        units of the datapoints

    Returns:
        (pd.DataFrame): The retrieved datapoints
    """
    ts = client.time_series.retrieve(external_id=external_id)
    if ts.unit is not None:
        if input_unit is None:
            input_unit = ts.unit
    else:
        if input_unit is None:
            msg = "No unit specified and no unit found in the timeseries"
            raise ValueError(msg)

    if output_unit is None:
        output_unit = input_unit

    input_unit_init = input_unit
    output_unit_init = output_unit
    input_unit, __, __, __ = preprocess_unit(input_unit, None, False)
    output_unit, __, __, __ = preprocess_unit(output_unit, None, False)

    dps: pd.Series = client.datapoints.retrieve_dataframe(
        external_id=external_id,
        start=start_time,
        end=end_time,
        aggregates=aggregates,
        granularity=granularity,
    ).squeeze()

    input_unit = input_unit_init
    output_unit = output_unit_init
    dps_converted: pd.Series = dps.apply(
        lambda x: unit_convert(x, input_unit, output_unit)
    )
    dps_converted.name = f"{dps.name} ({output_unit_init})"

    if retrieve_original_units and input_unit != output_unit:
        dps.name = f"{dps.name} ({input_unit_init})"
        return pd.DataFrame(dps).join(dps_converted)
    else:
        return pd.DataFrame(dps_converted)
