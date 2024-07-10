from datetime import datetime
from unittest import mock

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeries, TimeSeriesFilter, TimeSeriesList, TimeSeriesUpdate, filters
from cognite.client.data_classes.cdm.v1 import TimesSeriesBaseApply
from cognite.client.data_classes.data_modeling import Space
from cognite.client.data_classes.time_series import TimeSeriesProperty
from cognite.client.utils._time import MAX_TIMESTAMP_MS, MIN_TIMESTAMP_MS
from tests.utils import set_request_limit


@pytest.fixture(scope="class")
def new_ts(cognite_client):
    ts = cognite_client.time_series.create(TimeSeries(name="any", metadata={"a": "b"}))
    yield ts
    cognite_client.time_series.delete(id=ts.id)
    assert cognite_client.time_series.retrieve(ts.id) is None


@pytest.fixture
def post_spy(cognite_client):
    with mock.patch.object(cognite_client.time_series, "_post", wraps=cognite_client.time_series._post) as _:
        yield


@pytest.fixture(scope="session")
def test_tss(cognite_client):
    return cognite_client.time_series.retrieve_multiple(
        external_ids=[
            "PYSDK integration test 003: weekly values, 1950-2000, numeric",
            "PYSDK integration test 073: weekly values, 1950-2000, string",
            "PYSDK integration test 117: single dp at 1900-01-01 00:00:00, numeric",
            "PYSDK integration test 118: single dp at 2099-12-31 23:59:59.999, numeric",
        ],
    )


@pytest.fixture
def test_ts_numeric(test_tss):
    return test_tss[0]


@pytest.fixture
def test_ts_string(test_tss):
    return test_tss[1]


@pytest.fixture
def time_series_list(cognite_client: CogniteClient) -> TimeSeriesList:
    prefix = "integration_test:"
    time_series = TimeSeriesList(
        [
            TimeSeries(
                external_id=f"{prefix}timeseries1",
                unit="$",
                metadata={"market": "Nordpool", "timezone": "Europe/Oslo"},
            ),
            TimeSeries(
                external_id=f"{prefix}timeseries2",
                metadata={"market": "Balancing", "timezone": "Europe/London"},
            ),
        ]
    )
    retrieved = cognite_client.time_series.retrieve_multiple(
        external_ids=time_series.as_external_ids(), ignore_unknown_ids=True
    )
    if len(retrieved) == len(time_series):
        return retrieved
    return cognite_client.time_series.upsert(time_series, mode="replace")


class TestTimeSeriesAPI:
    def test_retrieve(self, cognite_client):
        listed_asset = cognite_client.time_series.list(limit=1)[0]
        retrieved_asset = cognite_client.time_series.retrieve(listed_asset.id)
        retrieved_asset.external_id = listed_asset.external_id
        assert retrieved_asset == listed_asset

    def test_retrieve_multiple(self, cognite_client):
        res = cognite_client.time_series.list(limit=2)
        retrieved_assets = cognite_client.time_series.retrieve_multiple([t.id for t in res])
        for listed_asset, retrieved_asset in zip(res, retrieved_assets):
            retrieved_asset.external_id = listed_asset.external_id
        assert res == retrieved_assets

    def test_retrieve_multiple_ignore_unknown(self, cognite_client):
        res = cognite_client.time_series.list(limit=2)
        retrieved_assets = cognite_client.time_series.retrieve_multiple(
            [t.id for t in res] + [1, 2, 3], ignore_unknown_ids=True
        )
        for listed_asset, retrieved_asset in zip(res, retrieved_assets):
            retrieved_asset.external_id = listed_asset.external_id
        assert res == retrieved_assets

    def test_list(self, cognite_client, post_spy):
        with set_request_limit(cognite_client.time_series, 10):
            res = cognite_client.time_series.list(limit=20)

        assert 20 == len(res)
        assert 2 == cognite_client.time_series._post.call_count

    def test_list_with_filters(self, cognite_client, post_spy):
        res = cognite_client.time_series.list(
            is_step=True,
            is_string=False,
            metadata={"a": "b"},
            last_updated_time={"min": 45},
            created_time={"max": 123},
            asset_ids=[1, 2],
        )
        assert 0 == len(res)
        assert 1 == cognite_client.time_series._post.call_count

    def test_list_timeseries_with_target_unit(self, cognite_client: CogniteClient) -> None:
        ts1 = TimeSeries(external_id="test_list_timeseries_with_target_unit:1", unit_external_id="temperature:deg_c")
        ts2 = TimeSeries(external_id="test_list_timeseries_with_target_unit:2", unit_external_id="temperature:deg_f")
        new_ts = TimeSeriesList([ts1, ts2])
        retrieved = cognite_client.time_series.retrieve_multiple(
            external_ids=new_ts.as_external_ids(), ignore_unknown_ids=True
        )
        if not retrieved:
            cognite_client.time_series.upsert(new_ts, mode="replace")

        listed = cognite_client.time_series.list(
            unit_external_id="temperature:deg_c",
            external_id_prefix="test_list_timeseries_with_target_unit",
            limit=2,
        )
        assert len(listed) == 1
        assert listed[0].unit_external_id == "temperature:deg_c"
        assert listed[0].external_id == "test_list_timeseries_with_target_unit:1"

    def test_partitioned_list(self, cognite_client, post_spy):
        mintime = datetime(2019, 1, 1).timestamp() * 1000
        maxtime = datetime(2019, 5, 15).timestamp() * 1000
        res_flat = cognite_client.time_series.list(limit=None, created_time={"min": mintime, "max": maxtime})
        res_part = cognite_client.time_series.list(
            partitions=8, limit=None, created_time={"min": mintime, "max": maxtime}
        )
        assert len(res_flat) > 0
        assert len(res_flat) == len(res_part)
        assert {a.id for a in res_flat} == {a.id for a in res_part}

    def test_aggregate(self, cognite_client, new_ts):
        res = cognite_client.time_series.aggregate(filter=TimeSeriesFilter(name="any"))
        assert res[0].count > 0

    def test_search(self, cognite_client):
        res = cognite_client.time_series.search(
            name="test__timestamp_multiplied", filter=TimeSeriesFilter(created_time={"min": 0})
        )
        assert len(res) > 0

    def test_update(self, cognite_client, new_ts):
        assert new_ts.metadata == {"a": "b"}
        update_ts = TimeSeriesUpdate(new_ts.id).name.set("newname").metadata.set({})
        res = cognite_client.time_series.update(update_ts)
        assert "newname" == res.name
        assert res.metadata == {}

    def test_update_target_unit(self, cognite_client: CogniteClient, new_ts: TimeSeries) -> None:
        update_ts = TimeSeriesUpdate(new_ts.id).unit_external_id.set("temperature:deg_c")

        res = cognite_client.time_series.update(update_ts)
        retrieved = cognite_client.time_series.retrieve(id=new_ts.id)
        assert "temperature:deg_c" == res.unit_external_id
        assert "temperature:deg_c" == retrieved.unit_external_id

    def test_delete_with_nonexisting(self, cognite_client):
        a = cognite_client.time_series.create(TimeSeries(name="any"))
        cognite_client.assets.delete(id=a.id, external_id="this ts does not exist", ignore_unknown_ids=True)
        assert cognite_client.assets.retrieve(id=a.id) is None

    def test_upsert_2_time_series_one_preexisting(self, cognite_client: CogniteClient) -> None:
        new_times_series = TimeSeries(
            external_id="test_upsert_2_time_series_one_preexisting:new", name="my new time series"
        )
        preexisting = TimeSeries(
            external_id="test_upsert_2_time_series_one_preexisting:preexisting",
            name="my preexisting time series",
        )
        preexisting_update = TimeSeries.load(preexisting.dump(camel_case=True))
        preexisting_update.name = "my preexisting time series updated"

        try:
            created_existing = cognite_client.time_series.create(preexisting)
            assert created_existing.id is not None

            res = cognite_client.time_series.upsert([new_times_series, preexisting_update], mode="replace")

            assert len(res) == 2
            assert new_times_series.external_id == res[0].external_id
            assert preexisting.external_id == res[1].external_id
            assert new_times_series.name == res[0].name
            assert preexisting_update.name == res[1].name
        finally:
            cognite_client.time_series.delete(
                external_id=[new_times_series.external_id, preexisting.external_id], ignore_unknown_ids=True
            )

    def test_filter_is_numeric(self, cognite_client: CogniteClient, test_tss: TimeSeriesList) -> None:
        f = filters
        is_integration_test = f.Prefix(TimeSeriesProperty.external_id, "PYSDK integration test")
        is_numeric = f.Equals(TimeSeriesProperty.is_string, False)

        result = cognite_client.time_series.filter(
            f.And(is_integration_test, is_numeric), sort=TimeSeriesProperty.external_id
        )
        assert result, "There should be at least one numeric time series"

    def test_list_with_advanced_filter(self, cognite_client: CogniteClient, test_tss: TimeSeriesList) -> None:
        f = filters
        is_numeric = f.Equals(TimeSeriesProperty.is_string, False)

        result = cognite_client.time_series.list(
            external_id_prefix="PYSDK integration", advanced_filter=is_numeric, sort=TimeSeriesProperty.external_id
        )
        assert result, "There should be at least one numeric time series"

    def test_filter_without_sort(self, cognite_client: CogniteClient, test_tss: TimeSeriesList) -> None:
        f = filters
        is_integration_test = f.Prefix(TimeSeriesProperty.external_id, "PYSDK integration test")
        is_numeric = f.Equals(TimeSeriesProperty.is_string, False)

        result = cognite_client.time_series.filter(f.And(is_integration_test, is_numeric), sort=None)
        assert result, "There should be at least one numeric time series"

    def test_aggregate_count(self, cognite_client: CogniteClient, time_series_list: TimeSeriesList) -> None:
        f = filters
        is_integration_test = f.Prefix("externalId", "integration_test:")

        count = cognite_client.time_series.aggregate_count(advanced_filter=is_integration_test)
        assert count >= len(time_series_list)

    def test_aggregate_unit(self, cognite_client: CogniteClient, time_series_list: TimeSeriesList) -> None:
        f = filters
        is_integration_test = f.Prefix("externalId", "integration_test:")

        count = cognite_client.time_series.aggregate_cardinality_values(TimeSeriesProperty.unit, is_integration_test)
        assert count >= len({t.unit for t in time_series_list if t.unit})

    def test_aggregate_metadata_keys_count(
        self, cognite_client: CogniteClient, time_series_list: TimeSeriesList
    ) -> None:
        f = filters
        is_integration_test = f.Prefix("externalId", "integration_test:")

        count = cognite_client.time_series.aggregate_cardinality_properties(
            TimeSeriesProperty.metadata, advanced_filter=is_integration_test
        )
        assert count >= len({k for t in time_series_list for k in t.metadata.keys()})

    def test_aggregate_unique_units(self, cognite_client: CogniteClient, time_series_list: TimeSeriesList) -> None:
        f = filters
        is_integration_test = f.Prefix("externalId", "integration_test:")

        result = cognite_client.time_series.aggregate_unique_values(TimeSeriesProperty.unit, is_integration_test)
        assert result
        assert set(result.unique) >= {t.unit for t in time_series_list if t.unit}

    def test_aggregate_unique_metadata_keys(
        self, cognite_client: CogniteClient, time_series_list: TimeSeriesList
    ) -> None:
        f = filters
        is_integration_test = f.Prefix("externalId", "integration_test:")

        result = cognite_client.time_series.aggregate_unique_properties(
            TimeSeriesProperty.metadata, advanced_filter=is_integration_test
        )
        assert result
        assert {tuple(item.value["property"]) for item in result} >= {
            ("metadata", key.casefold()) for a in time_series_list for key in a.metadata or []
        }

    def test_create_retrieve_delete_with_instance_id(
        self, cognite_client_alpha: CogniteClient, alpha_test_space: Space
    ) -> None:
        my_ts = TimesSeriesBaseApply(
            space=alpha_test_space.space,
            external_id="ts_python_sdk_instance_id_tests",
            is_string=False,
            is_step=False,
            source_unit="pressure:psi",
            name="Create Retrieve Delete with instance_id",
            description="This time series was created by the Python SDK",
        )
        update = TimeSeriesUpdate(instance_id=my_ts.as_id()).metadata.add({"a": "b"})

        try:
            created = cognite_client_alpha.data_modeling.instances.apply(my_ts)
            assert len(created.nodes) == 1
            assert created.nodes[0].as_id() == my_ts.as_id()

            retrieved = cognite_client_alpha.time_series.retrieve(instance_id=my_ts.as_id())
            assert retrieved is not None
            assert retrieved.instance_id == my_ts.as_id()

            updated = cognite_client_alpha.time_series.update(update)
            assert updated.metadata == {"a": "b"}

            retrieved = cognite_client_alpha.time_series.retrieve_multiple(instance_ids=[my_ts.as_id()])
            assert retrieved.dump() == [updated.dump()]
        finally:
            cognite_client_alpha.data_modeling.instances.delete(nodes=my_ts.as_id())


class TestTimeSeriesHelperMethods:
    @pytest.mark.parametrize(
        "ts_idx, exp_count",
        [(0, 2609), (2, 1), (3, 1)],
    )
    def test_get_count__numeric(self, test_tss, ts_idx, exp_count):
        assert test_tss[ts_idx].is_string is False
        count = test_tss[ts_idx].count()
        assert count == exp_count

    def test_get_count__string_fails(self, test_ts_string):
        assert test_ts_string.is_string is True
        with pytest.raises(RuntimeError, match="String time series does not support count aggregate."):
            test_ts_string.count()

    def test_get_latest(self, test_ts_numeric, test_ts_string):
        res1 = test_ts_numeric.latest()
        res2 = test_ts_string.latest()
        assert res1.timestamp == 946166400000
        assert res1.value == 946166400003.0
        assert res2.timestamp == 946166400000
        assert res2.value == "946166400073"  # this value should probably be more string-like ;P

    def test_get_latest_before(self, test_ts_numeric, test_ts_string):
        res1 = test_ts_numeric.latest(before=0)
        res2 = test_ts_string.latest(before=0)
        assert res1.timestamp == -345600000
        assert res1.value == -345599997.0
        assert res2.timestamp == -345600000
        assert res2.value == "-345599927"

    @pytest.mark.parametrize(
        "ts_idx, exp_ts, exp_val",
        [
            (0, -631152000000, -631151999997.0),
            (1, -631152000000, "-631151999927"),
            (2, MIN_TIMESTAMP_MS, MIN_TIMESTAMP_MS),
            (3, MAX_TIMESTAMP_MS, MAX_TIMESTAMP_MS),
        ],
    )
    def test_get_first_datapoint(self, test_tss, ts_idx, exp_ts, exp_val):
        res = test_tss[ts_idx].first()
        assert res.timestamp == exp_ts
        assert res.value == exp_val
