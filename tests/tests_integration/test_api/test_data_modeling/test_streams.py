import os
import string

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling.streams import Stream, StreamApply, StreamSettings, StreamTemplate
from cognite.client.utils._text import random_string

if os.environ["COGNITE_PROJECT"] != "erlend-test":
    pytest.skip(
        "Skipping all Records integration tests, only enabled in alpha for erlend-test project", allow_module_level=True
    )


class TestStreamsAPI:
    def test_list(self, cognite_client: CogniteClient, persisted_stream: Stream) -> None:
        streams = cognite_client.data_modeling.streams.list(limit=-1)
        assert any(s.external_id == persisted_stream.external_id for s in streams)

    def test_retrieve(self, cognite_client: CogniteClient, persisted_stream: Stream) -> None:
        retrieved = cognite_client.data_modeling.streams.retrieve(persisted_stream.external_id)
        assert retrieved is not None
        assert retrieved.external_id == persisted_stream.external_id

    def test_delete(self, cognite_client: CogniteClient, persisted_stream: Stream) -> None:
        external_id = f"python-sdk-test-stream-{random_string(10, string.ascii_lowercase)}"
        cognite_client.data_modeling.streams.apply(
            StreamApply(
                external_id=external_id,
                settings=StreamSettings(template=StreamTemplate(name="MutableTestStream")),
            )
        )
        cognite_client.data_modeling.streams.delete(external_id)
        assert cognite_client.data_modeling.streams.retrieve(external_id) is None
