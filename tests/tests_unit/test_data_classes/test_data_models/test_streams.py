from __future__ import annotations

from cognite.client.data_classes.data_modeling.streams import (
    Stream,
    StreamList,
    StreamTemplate,
    StreamTemplateWriteSettings,
    StreamWrite,
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
    w = StreamWrite(
        "abc",
        StreamTemplateWriteSettings(StreamTemplate("ImmutableTestStream")),
    )
    dumped = w.dump()
    assert dumped["externalId"] == "abc"
    assert dumped["settings"]["template"]["name"] == "ImmutableTestStream"


def test_stream_write_dump_dict_escape_hatch() -> None:
    w = StreamWrite("abc", {"template": {"name": "ImmutableTestStream"}})
    dumped = w.dump()
    assert dumped["externalId"] == "abc"
    assert dumped["settings"]["template"]["name"] == "ImmutableTestStream"


def test_stream_write_load_roundtrip() -> None:
    raw = {
        "externalId": "x",
        "settings": {"template": {"name": "ImmutableTestStream", "version": "1"}},
    }
    w = StreamWrite._load(raw)
    assert isinstance(w.settings, StreamTemplateWriteSettings)
    assert w.settings.template.name == "ImmutableTestStream"
    assert w.settings.template.version == "1"
    assert w.dump(camel_case=True)["settings"]["template"]["version"] == "1"


def test_stream_write_load_arbitrary_settings_dict() -> None:
    raw = {
        "externalId": "x",
        "settings": {"template": {"name": "T"}, "extra": 1},
    }
    w = StreamWrite._load(raw)
    assert w.settings == {"template": {"name": "T"}, "extra": 1}
