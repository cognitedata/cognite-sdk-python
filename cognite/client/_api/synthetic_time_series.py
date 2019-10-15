from datetime import datetime
from typing import *

import cognite.client.utils._time
from cognite.client._api_client import APIClient
from cognite.client.data_classes import Datapoints, DatapointsList, DatapointsQuery
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._experimental_warning import experimental_api


@experimental_api(api_name="Synthetic Timeseries")
class SyntheticDatapointsAPI(APIClient):
    _SYNTHETIC_RESOURCE_PATH = "/timeseries/synthetic"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._DPS_LIMIT = 100000

    def retrieve(
        self, function: str, start: Union[int, str, datetime], end: Union[int, str, datetime], limit: int = None
    ) -> Datapoints:
        """Calculate the result of a function on time series.

        Args:
            start (Union[int, str, datetime]): Inclusive start.
            end (Union[int, str, datetime]): Exclusive end.
            function (str): Function to be calculated.

        Returns:
            Datapoints: A Datapoints object containing the calculated data.

        Examples:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> dps = c.datapoints.synthetic.retrieve(function="TS{id:123} + TS{externalId:'abc''}", start="2w-ago", end="now")
            """
        if limit is None or limit == -1:
            limit = float("inf")
        query = {
            "function": function,
            "start": cognite.client.utils._time.timestamp_to_ms(start),
            "end": cognite.client.utils._time.timestamp_to_ms(end),
        }
        datapoints = Datapoints()
        while True:
            query["limit"] = min(limit, self._DPS_LIMIT)
            resp = self._post(url_path=self._SYNTHETIC_RESOURCE_PATH, json={"items": [query]})
            data = resp.json()["items"][0]
            datapoints._extend(Datapoints._load(data, expected_fields=["value"]))
            limit -= len(data["datapoints"])
            if len(data["datapoints"]) < self._DPS_LIMIT or limit <= 0:
                break
            query["start"] = data["datapoints"][-1]["timestamp"] + 1
        return datapoints
