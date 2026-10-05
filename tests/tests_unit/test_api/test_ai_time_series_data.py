from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

import pytest

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client._api.ai.time_series import AITimeSeriesAPI
from cognite.client._api.datapoints import DatapointsAPI
from cognite.client.data_classes import Datapoints, DatapointsList
from cognite.client.data_classes.ai import (
    ForecastResult,
    ForecastResultList,
    ImputeResult,
    ImputeResultList,
    InputTimeSeries,
    QuantileDatapoint,
    TimeSeriesForecast,
    TimeSeriesForecastList,
    TimeSeriesImpute,
    TimeSeriesImputeList,
)
from cognite.client.data_classes.data_modeling import NodeId

MIN = 60_000
HOUR = 60 * MIN
NODE = NodeId("north_sea_asset", "21-PT-1039")


def make_dps(
    timestamps: list[int],
    values: list[float],
    *,
    id: int = 1,
    external_id: str | None = None,
    instance_id: NodeId | None = None,
    aggregate: Literal["average"] | None = None,
) -> Datapoints:
    return Datapoints(
        id=id,
        external_id=external_id,
        instance_id=instance_id,
        is_string=False,
        is_step=False,
        type="numeric",
        timestamp=timestamps,
        value=values if aggregate is None else None,
        average=values if aggregate == "average" else None,
    )


class FakeCDF:
    """Stands in for datapoints retrieval and the forecast endpoint, recording what the method sends."""

    def __init__(self) -> None:
        self.retrieved: Datapoints | DatapointsList | None = None
        self.retrieve_kwargs: dict[str, Any] = {}
        self.sent: list[InputTimeSeries] = []

    async def retrieve(self, **kwargs: Any) -> Datapoints | DatapointsList | None:
        self.retrieve_kwargs = kwargs
        return self.retrieved

    async def forecast(self, time_series: Sequence[InputTimeSeries]) -> ForecastResultList:
        self.sent = list(time_series)
        results = [
            ForecastResult(s.label, [QuantileDatapoint(10**12, {0.05: 1.0, 0.5: 2.0, 0.95: 3.0})], s.cohort)
            for s in self.sent
        ]
        return ForecastResultList(results, quantile_levels=[0.05, 0.5, 0.95])

    async def impute(self, time_series: Sequence[InputTimeSeries]) -> ImputeResultList:
        self.sent = list(time_series)
        results = [
            ImputeResult(
                s.label,
                [QuantileDatapoint(dp["timestamp"], {0.5: 0.0}) for dp in s.dump()["datapoints"] if dp.get("missing")],
                s.cohort,
            )
            for s in self.sent
        ]
        return ImputeResultList(results, quantile_levels=[0.5])


@pytest.fixture
def cdf(monkeypatch: pytest.MonkeyPatch) -> FakeCDF:
    fake = FakeCDF()
    monkeypatch.setattr(DatapointsAPI, "retrieve", fake.retrieve)
    monkeypatch.setattr(AITimeSeriesAPI, "forecast", fake.forecast)
    monkeypatch.setattr(AITimeSeriesAPI, "impute", fake.impute)
    return fake


def sent_datapoints(cdf: FakeCDF, i: int = 0) -> list[dict[str, Any]]:
    return cdf.sent[i].dump()["datapoints"]


class TestForecast:
    def test_single_identifier_returns_single_forecast(self, cognite_client: CogniteClient, cdf: FakeCDF) -> None:
        cdf.retrieved = make_dps([0, HOUR, 3 * HOUR], [1.0, 2.0, 3.0], external_id="21-PT-1019", aggregate="average")

        fc = cognite_client.ai.time_series.data.forecast(
            external_id="21-PT-1019", start="30d-ago", granularity="1h", aggregate="average"
        )

        assert isinstance(fc, TimeSeriesForecast)
        assert fc.external_id == "21-PT-1019"
        assert fc.forecast[0].quantiles == {0.05: 1.0, 0.5: 2.0, 0.95: 3.0}
        assert cdf.retrieve_kwargs | {"external_id": "21-PT-1019"} == {
            "id": None,
            "external_id": "21-PT-1019",
            "instance_id": None,
            "start": "30d-ago",
            "end": None,
            "aggregates": "average",
            "granularity": "1h",
        }

    def test_empty_aggregate_buckets_are_sent_as_missing(self, cognite_client: CogniteClient, cdf: FakeCDF) -> None:
        cdf.retrieved = make_dps([0, HOUR, 3 * HOUR], [1.0, 2.0, 3.0], external_id="21-PT-1019", aggregate="average")

        cognite_client.ai.time_series.data.forecast(
            external_id="21-PT-1019", start="30d-ago", granularity="1h", aggregate="average"
        )

        assert [dp.get("missing", False) for dp in sent_datapoints(cdf)] == [False, False, True, False]

    def test_several_identifiers_return_a_list_keyed_by_cdf_identity(
        self, cognite_client: CogniteClient, cdf: FakeCDF
    ) -> None:
        cdf.retrieved = DatapointsList(
            [
                make_dps([0, MIN], [1.0, 2.0], id=123),
                make_dps([0, MIN], [1.0, 2.0], id=2, external_id="21-PT-1029"),
                make_dps([0, MIN], [1.0, 2.0], id=3, instance_id=NODE),
            ]
        )

        res = cognite_client.ai.time_series.data.forecast(
            id=123, external_id="21-PT-1029", instance_id=NODE, start="1d-ago"
        )

        assert isinstance(res, TimeSeriesForecastList)
        assert res.quantile_levels == [0.05, 0.5, 0.95]
        assert res.get(id=123) is res[0]
        assert res.get(external_id="21-PT-1029") is res[1]
        assert res.get(instance_id=NODE) is res[2]
        assert [s.label for s in cdf.sent] == ["123", "21-PT-1029", "north_sea_asset:21-PT-1039"]

    def test_cohort_aligns_all_series_on_one_grid(self, cognite_client: CogniteClient, cdf: FakeCDF) -> None:
        cdf.retrieved = DatapointsList(
            [
                make_dps([0, MIN, 2 * MIN], [1.0, 2.0, 3.0], external_id="23-PT-1101"),
                make_dps([MIN, 2 * MIN, 3 * MIN], [1.0, 2.0, 3.0], external_id="23-PT-1201"),
            ]
        )

        res = cognite_client.ai.time_series.data.forecast(
            external_id=["23-PT-1101", "23-PT-1201"], start="1d-ago", cohort="compression-train-a"
        )

        assert [dp["timestamp"] for dp in sent_datapoints(cdf, 0)] == [
            dp["timestamp"] for dp in sent_datapoints(cdf, 1)
        ]
        assert {s.cohort for s in cdf.sent} == {"compression-train-a"}
        assert isinstance(res, TimeSeriesForecastList)
        assert {fc.cohort for fc in res} == {"compression-train-a"}

    def test_irregular_raw_datapoints_raise_with_time_series_advice(
        self, cognite_client: CogniteClient, cdf: FakeCDF
    ) -> None:
        cdf.retrieved = make_dps([0, 2 * MIN, 3 * MIN + 7_000], [1.0, 2.0, 3.0], external_id="21-PT-1019")

        with pytest.raises(
            ValueError, match=r"'21-PT-1019' is not evenly spaced\. Pass `granularity=` and `aggregate=`"
        ):
            cognite_client.ai.time_series.data.forecast(external_id="21-PT-1019", start="1d-ago")

    def test_empty_window_raises_with_advice(self, cognite_client: CogniteClient, cdf: FakeCDF) -> None:
        cdf.retrieved = make_dps([], [], external_id="21-PT-1019", aggregate="average")

        with pytest.raises(ValueError, match=r"has 0 datapoints .* Widen `start`/`end`"):
            cognite_client.ai.time_series.data.forecast(
                external_id="21-PT-1019", start="30d-ago", granularity="1h", aggregate="average"
            )

    def test_history_over_the_model_limit_raises_instead_of_truncating(
        self, cognite_client: CogniteClient, cdf: FakeCDF
    ) -> None:
        cdf.retrieved = make_dps([i * MIN for i in range(5000)], [1.0] * 5000, external_id="21-PT-1019")

        with pytest.raises(ValueError, match="spans 5000 grid points"):
            cognite_client.ai.time_series.data.forecast(external_id="21-PT-1019", start="10d-ago")

    def test_string_time_series_are_rejected(self, cognite_client: CogniteClient, cdf: FakeCDF) -> None:
        cdf.retrieved = Datapoints(
            id=1, external_id="21-ZS-1019", is_string=True, is_step=False, type="string", timestamp=[0], value=["on"]
        )

        with pytest.raises(ValueError, match="string time series"):
            cognite_client.ai.time_series.data.forecast(external_id="21-ZS-1019", start="1d-ago")

    def test_granularity_and_aggregate_go_together(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(ValueError, match="together"):
            cognite_client.ai.time_series.data.forecast(external_id="21-PT-1019", start="1d-ago", granularity="1h")

    async def test_async_client(self, async_client: AsyncCogniteClient, cdf: FakeCDF) -> None:
        cdf.retrieved = make_dps([0, MIN], [1.0, 2.0], external_id="21-PT-1019")

        fc = await async_client.ai.time_series.data.forecast(external_id="21-PT-1019", start="1d-ago")

        assert isinstance(fc, TimeSeriesForecast)


class TestImpute:
    def test_requires_saying_what_to_reconstruct(self, cognite_client: CogniteClient, cdf: FakeCDF) -> None:
        with pytest.raises(ValueError, match="fill_gaps=True"):
            cognite_client.ai.time_series.data.impute(external_id="21-PT-1019", start="7d-ago")
        assert cdf.retrieve_kwargs == {}  # fails before retrieving anything

    def test_fill_gaps_reconstructs_every_empty_bucket(self, cognite_client: CogniteClient, cdf: FakeCDF) -> None:
        cdf.retrieved = make_dps([0, HOUR, 3 * HOUR], [1.0, 2.0, 4.0], external_id="21-PT-1019", aggregate="average")

        res = cognite_client.ai.time_series.data.impute(
            external_id="21-PT-1019", start="7d-ago", granularity="1h", aggregate="average", fill_gaps=True
        )

        assert isinstance(res, TimeSeriesImpute)
        assert res.external_id == "21-PT-1019"
        assert [p.timestamp for p in res.imputed] == [2 * HOUR]

    def test_mask_hides_existing_values_and_unmasked_gaps_are_not_returned(
        self, cognite_client: CogniteClient, cdf: FakeCDF
    ) -> None:
        cdf.retrieved = make_dps([0, MIN, 3 * MIN, 4 * MIN], [1.0, 2.0, 4.0, 5.0], external_id="21-PT-1019")

        res = cognite_client.ai.time_series.data.impute(external_id="21-PT-1019", start="1d-ago", mask=[(MIN, MIN)])

        assert sent_datapoints(cdf)[1:3] == [
            {"timestamp": MIN, "value": 2.0, "missing": True},
            {"timestamp": 2 * MIN, "value": None},
        ]
        assert isinstance(res, TimeSeriesImpute)
        assert [p.timestamp for p in res.imputed] == [MIN]

    def test_mask_accepts_datetimes_and_single_timestamps(self, cognite_client: CogniteClient, cdf: FakeCDF) -> None:
        cdf.retrieved = make_dps([0, MIN, 2 * MIN, 3 * MIN], [1.0, 2.0, 3.0, 4.0], external_id="21-PT-1019")
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)

        cognite_client.ai.time_series.data.impute(
            external_id="21-PT-1019", start="1d-ago", mask=[(epoch, epoch + timedelta(minutes=1)), 3 * MIN]
        )

        assert [dp.get("missing", False) for dp in sent_datapoints(cdf)] == [True, True, False, True]

    def test_mask_outside_the_window_raises(self, cognite_client: CogniteClient, cdf: FakeCDF) -> None:
        cdf.retrieved = make_dps([0, MIN, 2 * MIN], [1.0, 2.0, 3.0], external_id="21-PT-1019")

        with pytest.raises(ValueError, match="doesn't cover any point"):
            cognite_client.ai.time_series.data.impute(external_id="21-PT-1019", start="1d-ago", mask=[10 * MIN])

    def test_several_identifiers_return_a_list(self, cognite_client: CogniteClient, cdf: FakeCDF) -> None:
        cdf.retrieved = DatapointsList(
            [
                make_dps([0, MIN, 3 * MIN], [1.0, 2.0, 4.0], id=1, external_id="23-PT-1101"),
                make_dps([0, MIN, 2 * MIN, 3 * MIN], [1.0, 2.0, 3.0, 4.0], id=2, external_id="23-PT-1201"),
            ]
        )

        res = cognite_client.ai.time_series.data.impute(
            external_id=["23-PT-1101", "23-PT-1201"], start="1d-ago", fill_gaps=True
        )

        assert isinstance(res, TimeSeriesImputeList)
        first, second = res.get(external_id="23-PT-1101"), res.get(external_id="23-PT-1201")
        assert first is not None and [p.timestamp for p in first.imputed] == [2 * MIN]
        assert second is not None and second.imputed == []
