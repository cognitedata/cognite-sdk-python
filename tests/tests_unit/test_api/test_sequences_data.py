from types import SimpleNamespace

from cognite.client.data_classes.sequences import SequenceRows, SequenceRowsList


class _DummyRows(SequenceRows):
    def __init__(self):
        pass


def test_retrieve_last_row_accepts_list_of_external_ids(monkeypatch, cognite_client):
    api = cognite_client.sequences.data
    calls = []

    def fake_do_request(method, path, json):
        calls.append(json)
        return SimpleNamespace(json=lambda: {})

    monkeypatch.setattr(api, "_do_request", fake_do_request, raising=True)
    monkeypatch.setattr(SequenceRows, "_load", classmethod(lambda cls, payload: _DummyRows()), raising=True)

    out = api.retrieve_last_row(external_id=["exA", "exB"])

    assert isinstance(out, SequenceRowsList)
    assert len(out) == 2
    assert len(calls) == 2


def test_retrieve_last_row_single_id_calls_once(monkeypatch, cognite_client):
    api = cognite_client.sequences.data
    calls = []

    def fake_do_request(method, path, json):
        calls.append(json)
        return SimpleNamespace(json=lambda: {})

    monkeypatch.setattr(api, "_do_request", fake_do_request, raising=True)
    monkeypatch.setattr(SequenceRows, "_load", classmethod(lambda cls, payload: _DummyRows()), raising=True)

    out = api.retrieve_last_row(id=42)

    assert isinstance(out, SequenceRows)
    assert len(calls) == 1
