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
FILES_METER: dict = {"meterId": "files.storage_bytes", "datapoints": []}
NONEXISTENT_ID = "nonexistent.meter.id"


@pytest.fixture
def metering_url(async_client: AsyncCogniteClient) -> str:
    return get_url(async_client.metering) + "/metering/meters"


@pytest.fixture
def mock_byids_with_data(httpx_mock: HTTPXMock, metering_url: str) -> None:
    httpx_mock.add_response(
        method="POST",
        url=f"{metering_url}/byids",
        status_code=200,
        json={"items": [ATLAS_METER_WITH_DATA]},
    )


@pytest.fixture
def mock_list(httpx_mock: HTTPXMock, metering_url: str) -> None:
    httpx_mock.add_response(
        method="POST",
        url=f"{metering_url}/list",
        status_code=200,
        json={"items": [ATLAS_METER, FILES_METER]},
    )


class TestMeteringAPI:
    def test_retrieve_single(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        metering_url: str,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=f"{metering_url}/byids",
            status_code=200,
            json={"items": [ATLAS_METER]},
        )

        meter_id = ATLAS_METER["meterId"]
        res = cognite_client.metering.retrieve(id=meter_id)

        assert isinstance(res, MeteringData)
        assert res.meter_id == meter_id
        assert res.datapoints == []

        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        assert requests[0].method == "POST"
        body = jsgz_load(requests[0].content)
        assert body["items"] == [{"meterId": meter_id}]

    def test_retrieve_single_with_time_range(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_byids_with_data: None,
    ) -> None:
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

        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body["start"] == 1764547200000
        assert body["numberOfDatapoints"] == 2

    def test_retrieve_single_not_found(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        metering_url: str,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=f"{metering_url}/byids",
            status_code=404,
            json={"error": {"message": "Meters not found", "code": 404}},
        )

        res = cognite_client.metering.retrieve(id=NONEXISTENT_ID)

        assert res is None

    def test_retrieve_list_of_ids(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        metering_url: str,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=f"{metering_url}/byids",
            status_code=200,
            json={"items": [ATLAS_METER, FILES_METER]},
        )

        res = cognite_client.metering.retrieve(id=[ATLAS_METER["meterId"], FILES_METER["meterId"]])

        assert isinstance(res, MeteringDataList)
        assert len(res) == 2
        assert res[0].meter_id == ATLAS_METER["meterId"]
        assert res[1].meter_id == FILES_METER["meterId"]

        request = httpx_mock.get_requests()[0]
        assert request.method == "POST"
        body = jsgz_load(request.content)
        assert body["items"] == [
            {"meterId": ATLAS_METER["meterId"]},
            {"meterId": FILES_METER["meterId"]},
        ]

    def test_retrieve_list_of_ids_with_time_range(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_byids_with_data: None,
    ) -> None:
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
        mock_list: None,
    ) -> None:
        res = cognite_client.metering.list()

        assert isinstance(res, MeteringDataList)
        assert len(res) == 2

    def test_list_with_filter(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        metering_url: str,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=f"{metering_url}/list",
            status_code=200,
            json={
                "items": [
                    {"meterId": "atlas.monthly_ai_tokens", "datapoints": []},
                    {"meterId": "atlas.monthly_ai_prompts", "datapoints": []},
                ]
            },
        )

        prefix_filter = Prefix("meter_id", "atlas.")
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
        mock_list: None,
    ) -> None:
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
        mock_byids_with_data: None,
        start: datetime | str,
        end: datetime | None,
    ) -> None:
        cognite_client.metering.retrieve(
            id=ATLAS_METER["meterId"],
            start=start,
            end=end,
            number_of_datapoints=10,
        )

        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert "start" in body
        assert body["start"] > 0
        if isinstance(start, datetime):
            assert body["start"] == timestamp_to_ms(start)
            assert end is not None
            assert body["end"] == timestamp_to_ms(end)

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
