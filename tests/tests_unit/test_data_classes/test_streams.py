from __future__ import annotations

from cognite.client.data_classes.streams import (
    Record,
    RecordsFilterResponse,
    RecordsSyncResponse,
    Stream,
    StreamList,
    StreamWrite,
    SyncRecord,
)


def test_stream_roundtrip() -> None:
    raw = {
        "externalId": "s1",
        "createdTime": 1,
        "createdFromTemplate": "ImmutableTestStream",
        "type": "Immutable",
        "settings": {
            "lifecycle": {"retainedAfterSoftDelete": "P1D"},
            "limits": {
                "maxRecordsTotal": {"provisioned": 1000.0},
                "maxGigaBytesTotal": {"provisioned": 1.0, "consumed": 0.5},
            },
        },
    }
    s = Stream._load(raw)
    back = s.dump(camel_case=True)
    assert back["externalId"] == "s1"
    assert back["settings"]["limits"]["maxRecordsTotal"]["provisioned"] == 1000.0


def test_stream_list_load() -> None:
    raw = {
        "items": [
            {
                "externalId": "s1",
                "createdTime": 1,
                "createdFromTemplate": "ImmutableTestStream",
                "type": "Immutable",
                "settings": {
                    "lifecycle": {"retainedAfterSoftDelete": "P1D"},
                    "limits": {
                        "maxRecordsTotal": {"provisioned": 1000.0},
                        "maxGigaBytesTotal": {"provisioned": 1.0},
                    },
                },
            }
        ]
    }
    lst = StreamList._load(raw["items"])
    assert len(lst) == 1
    assert lst[0].external_id == "s1"


def test_stream_write_dump() -> None:
    w = StreamWrite("abc", {"template": {"name": "ImmutableTestStream"}})
    assert w.dump()["externalId"] == "abc"
    assert w.dump()["settings"]["template"]["name"] == "ImmutableTestStream"


def test_record_load() -> None:
    raw = {
        "space": "sp",
        "externalId": "r1",
        "createdTime": 2,
        "lastUpdatedTime": 3,
        "properties": {"sp": {"c": {"p": 1}}},
    }
    r = Record._load(raw)
    assert r.space == "sp"
    assert r.properties["sp"]["c"]["p"] == 1


def test_records_filter_response() -> None:
    raw = {
        "items": [
            {
                "space": "sp",
                "externalId": "r1",
                "createdTime": 1,
                "lastUpdatedTime": 2,
                "properties": {},
            }
        ]
    }
    fr = RecordsFilterResponse._load(raw)
    assert len(fr.items) == 1
    assert isinstance(fr.items[0], Record)


def test_records_sync_response() -> None:
    raw = {
        "items": [
            {
                "space": "sp",
                "externalId": "r1",
                "createdTime": 1,
                "lastUpdatedTime": 2,
                "status": "created",
            }
        ],
        "nextCursor": "c",
        "hasNext": False,
    }
    sr = RecordsSyncResponse._load(raw)
    assert sr.next_cursor == "c"
    assert not sr.has_next
    assert isinstance(sr.items[0], SyncRecord)
