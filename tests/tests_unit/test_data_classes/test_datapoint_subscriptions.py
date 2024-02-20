import pytest

from cognite.client.data_classes import filters
from cognite.client.data_classes.datapoints import Datapoint
from cognite.client.data_classes.datapoints_subscriptions import (
    DataDeletion,
    DatapointList,
    DataPointSubscriptionWrite,
    DatapointsUpdate,
)


class TestDataPointSubscription:
    def test_raises_value_error_on_invalid_filter(self):
        f = filters
        nested_fileter = f.Nested(
            scope=("some", "direct_relation", "property"),
            filter=f.Equals(property=["node", "name"], value="ACME"),
        )
        with pytest.raises(ValueError) as e:
            DataPointSubscriptionWrite(
                external_id="MySubscription",
                partition_count=10,
                name="MySubscription",
                filter=nested_fileter,
            )
        assert "Nested" in str(e.value) and "not supported" in str(e.value)

    def test_datapoint_iterator(self):
        datapoints = [
            Datapoint(timestamp=1700000000000, value=23.12),
            Datapoint(timestamp=1700000000001, value=25.12),
            Datapoint(timestamp=1700000000002, value=28.12),
        ]
        wrapper = DatapointList(datapoints)

        # Items
        assert datapoints[0] == wrapper[0]
        assert datapoints[-1] == wrapper[-1]

        # Slices
        sliced = wrapper[0:2]
        assert len(sliced) == 2
        assert datapoints[0] == sliced[0]

        # Iterator
        for i, dp in enumerate(wrapper):
            assert datapoints[i] == dp

        df = wrapper.to_pandas()
        assert df.index.name == "timestamp"
        assert list(df.columns) == [""]

        assert wrapper.dump() == [
            {"timestamp": 1700000000000, "value": 23.12},
            {"timestamp": 1700000000001, "value": 25.12},
            {"timestamp": 1700000000002, "value": 28.12},
        ]

        # _repre_html_()
        wrapper._repr_html_()

    def test_datapoint_updates(self):
        input = {
            "timeSeries": {"id": 192871023},
            "upserts": [
                {"timestamp": 1700000000002, "value": 28.12},
            ],
            "deletes": [{"inclusiveBegin": 123, "exclusiveEnd": 456}],
        }
        update = DatapointsUpdate.load(input)
        assert update.id == 192871023
        assert update.external_id is None
        assert update.upserts[0] == Datapoint(timestamp=1700000000002, value=28.12)
        assert update.deletes[0] == DataDeletion(123, 456)

        assert update.dump() == input

    def test_datapoint_updates_with_external_id(self):
        input = {
            "timeSeries": {"id": 192871023, "externalId": "testing"},
            "upserts": [
                {"timestamp": 1700000000002, "value": 28.12},
            ],
            "deletes": [{"inclusiveBegin": 123, "exclusiveEnd": 456}],
        }
        update = DatapointsUpdate.load(input)
        assert update.id == 192871023
        assert update.external_id == "testing"
        assert update.upserts[0] == Datapoint(timestamp=1700000000002, value=28.12)
        assert update.deletes[0] == DataDeletion(123, 456)

        assert update.dump() == input

        re_loaded = DatapointsUpdate.load(update.dump())
        assert re_loaded == update
