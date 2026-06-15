import re
from datetime import datetime, timezone
from typing import Any

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes import MeteringData, MeteringDataList
from cognite.client.data_classes.filters import Prefix
from cognite.client.utils._time import timestamp_to_ms
from tests.utils import get_url, jsgz_load

ATLAS_METER: dict = {"meterId": "atlas.monthly_ai_tokens", "datapoints": []}
ATLAS_METER_WITH_DATA: dict = {
    "meterId": "atlas.monthly_ai_tokens",
    "datapoints": [
        {"timestamp": 1764547200000, "average": 42000.0},
        {"timestamp": 1765411200000, "average": 38500.5},
    ],
}
NONEXISTENT_ID = "nonexistent.meter.id"


@pytest.fixture
def metering_url(async_client: AsyncCogniteClient) -> str:
    return get_url(async_client.metering) + "/metering/meters"


@pytest.fixture
def single_meter_url_pattern(metering_url: str) -> re.Pattern:
    return re.compile(re.escape(f"{metering_url}/{ATLAS_METER['meterId']}") + r"\?.*")


class TestMeteringAPI:
    def test_retrieve_single(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        async_client: AsyncCogniteClient,
        metering_url: str,
    ) -> None:
        httpx_mock.add_response(
            method="GET",
            url=f"{metering_url}/{ATLAS_METER['meterId']}",
            status_code=200,
            json=ATLAS_METER,
        )

        meter_id = ATLAS_METER["meterId"]
        res = cognite_client.metering.retrieve(id=meter_id)

        assert isinstance(res, MeteringData)
        assert res.meter_id == meter_id
        assert res.datapoints == []

        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        assert requests[0].method == "GET"
        assert f"metering/meters/{meter_id}" in str(requests[0].url)

    def test_retrieve_single_with_time_range(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        single_meter_url_pattern: re.Pattern,
    ) -> None:
        httpx_mock.add_response(
            method="GET",
            url=single_meter_url_pattern,
            status_code=200,
            json=ATLAS_METER_WITH_DATA,
        )

        res = cognite_client.metering.retrieve(
            id=ATLAS_METER["meterId"],
            start=1764547200000,
            end=1767225599000,
            number_of_datapoints=2,
        )

        assert isinstance(res, MeteringData)
        assert len(res.datapoints) == 2
        assert res.datapoints[0].timestamp == 1764547200000
        assert res.datapoints[0].average == 42000.0

        request = httpx_mock.get_requests()[0]
        assert "start=1764547200000" in str(request.url)
        assert "numberOfDatapoints=2" in str(request.url)

    def test_retrieve_single_not_found(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        metering_url: str,
    ) -> None:
        httpx_mock.add_response(
            method="GET",
            url=f"{metering_url}/{NONEXISTENT_ID}",
            status_code=404,
            json={"error": {"message": "Not Found"}},
        )

        res = cognite_client.metering.retrieve(id=NONEXISTENT_ID)

        assert res is None

    def test_retrieve_list_of_ids(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        metering_url: str,
    ) -> None:
        meters_data = [
            {"meterId": "atlas.monthly_ai_tokens", "datapoints": []},
            {"meterId": "files.storage_bytes", "datapoints": []},
        ]
        httpx_mock.add_response(
            method="POST",
            url=f"{metering_url}/byids",
            status_code=200,
            json={"items": meters_data},
        )

        res = cognite_client.metering.retrieve(id=["atlas.monthly_ai_tokens", "files.storage_bytes"])

        assert isinstance(res, MeteringDataList)
        assert len(res) == 2
        assert res[0].meter_id == "atlas.monthly_ai_tokens"
        assert res[1].meter_id == "files.storage_bytes"

        request = httpx_mock.get_requests()[0]
        assert request.method == "POST"
        body = jsgz_load(request.content)
        assert body["items"] == [
            {"meterId": "atlas.monthly_ai_tokens"},
            {"meterId": "files.storage_bytes"},
        ]

    def test_retrieve_list_of_ids_with_time_range(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        metering_url: str,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=f"{metering_url}/byids",
            status_code=200,
            json={"items": [ATLAS_METER_WITH_DATA]},
        )

        res = cognite_client.metering.retrieve(
            id=["atlas.monthly_ai_tokens"],
            start=1764547200000,
            end=1767225599000,
            number_of_datapoints=2,
        )

        assert isinstance(res, MeteringDataList)
        assert len(res[0].datapoints) == 2

        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body["start"] == 1764547200000
        assert body["numberOfDatapoints"] == 2

    def test_list(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        metering_url: str,
    ) -> None:
        meters_data = [
            {"meterId": "atlas.monthly_ai_tokens", "datapoints": []},
            {"meterId": "files.storage_bytes", "datapoints": []},
        ]
        httpx_mock.add_response(
            method="POST",
            url=f"{metering_url}/list",
            status_code=200,
            json={"items": meters_data},
        )

        res = cognite_client.metering.list()

        assert isinstance(res, MeteringDataList)
        assert len(res) == 2

    def test_list_with_filter(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        metering_url: str,
    ) -> None:
        meters_data = [
            {"meterId": "atlas.monthly_ai_tokens", "datapoints": []},
            {"meterId": "atlas.monthly_ai_prompts", "datapoints": []},
        ]
        httpx_mock.add_response(
            method="POST",
            url=f"{metering_url}/list",
            status_code=200,
            json={"items": meters_data},
        )

        prefix_filter = Prefix("meterId", "atlas.")
        res = cognite_client.metering.list(filter=prefix_filter)

        assert isinstance(res, MeteringDataList)
        assert len(res) == 2

        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        assert requests[0].method == "POST"
        assert f"{metering_url}/list" in str(requests[0].url)

        body = jsgz_load(requests[0].content)
        assert "filter" in body
        assert body["filter"]["prefix"]["property"] == ["meterId"]
        assert body["filter"]["prefix"]["value"] == "atlas."

    def test_list_with_time_range(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        metering_url: str,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=f"{metering_url}/list",
            status_code=200,
            json={"items": [ATLAS_METER_WITH_DATA]},
        )

        res = cognite_client.metering.list(start=1764547200000, number_of_datapoints=2)

        assert isinstance(res, MeteringDataList)
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body["start"] == 1764547200000
        assert body["numberOfDatapoints"] == 2

    @pytest.mark.parametrize(
        "start,end",
        [
            (datetime(2025, 1, 1, tzinfo=timezone.utc), datetime(2025, 2, 1, tzinfo=timezone.utc)),
            ("4w-ago", None),
        ],
    )
    def test_retrieve_start_type_conversion(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        single_meter_url_pattern: re.Pattern,
        start: datetime | str,
        end: datetime | None,
    ) -> None:
        httpx_mock.add_response(method="GET", url=single_meter_url_pattern, status_code=200, json=ATLAS_METER_WITH_DATA)

        cognite_client.metering.retrieve(
            id=ATLAS_METER["meterId"],
            start=start,
            end=end,
            number_of_datapoints=10,
        )

        request_url = str(httpx_mock.get_requests()[0].url)
        assert "start=" in request_url
        start_val = int(request_url.split("start=")[1].split("&")[0])
        assert start_val > 0
        if isinstance(start, datetime):
            assert start_val == timestamp_to_ms(start)
            assert end is not None
            assert f"end={timestamp_to_ms(end)}" in request_url

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"id": "atlas.monthly_ai_tokens", "start": 1764547200000},
            {"id": "atlas.monthly_ai_tokens", "number_of_datapoints": 10},
        ],
    )
    def test_retrieve_missing_paired_param_raises(
        self,
        cognite_client: CogniteClient,
        kwargs: dict[str, Any],
    ) -> None:
        with pytest.raises(ValueError, match="must be provided together"):
            cognite_client.metering.retrieve(**kwargs)

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"start": 1764547200000},
            {"number_of_datapoints": 10},
        ],
    )
    def test_list_missing_paired_param_raises(
        self,
        cognite_client: CogniteClient,
        kwargs: dict[str, Any],
    ) -> None:
        with pytest.raises(ValueError, match="must be provided together"):
            cognite_client.metering.list(**kwargs)

    def test_dump_snake_case(self) -> None:
        from cognite.client.data_classes.metering import MeteringData

        meter = MeteringData(meter_id="atlas.monthly_ai_tokens", datapoints=[])
        dumped = meter.dump(camel_case=False)
        assert "meter_id" in dumped
        assert dumped["meter_id"] == "atlas.monthly_ai_tokens"

    def test_metering_data_list_as_ids(self) -> None:
        from cognite.client.data_classes.metering import MeteringData, MeteringDataList

        items = [
            MeteringData(meter_id="atlas.monthly_ai_tokens"),
            MeteringData(meter_id="files.storage_bytes"),
        ]
        lst = MeteringDataList(items)
        assert lst.as_ids() == ["atlas.monthly_ai_tokens", "files.storage_bytes"]
