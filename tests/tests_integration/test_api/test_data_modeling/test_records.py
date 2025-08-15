from __future__ import annotations

import os
import string
from collections.abc import Callable
from random import random
from time import sleep
from typing import TypeVar

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import Container
from cognite.client.data_classes.data_modeling.ids import ContainerId
from cognite.client.data_classes.data_modeling.instances import SourceData
from cognite.client.data_classes.data_modeling.records import (
    LastUpdatedRange,
    RecordId,
    RecordIngest,
    RecordListWithCursor,
)
from cognite.client.data_classes.data_modeling.streams import Stream
from cognite.client.utils._text import random_string

T = TypeVar("T")

if os.environ["COGNITE_PROJECT"] != "erlend-test":
    pytest.skip(
        "Skipping all Records integration tests, only enabled in alpha for erlend-test project", allow_module_level=True
    )


def retry_assertion_errors(func: Callable[[], T], num_retries: int = 10) -> T:
    for i in range(num_retries):
        try:
            return func()
        except AssertionError as e:
            if i < num_retries - 1:
                sleep(i * random())
            else:
                raise e
    raise RuntimeError("Unexpected state reached")


class TestRecords:
    def test_records_ingest_upsert_delete(
        self, cognite_client: CogniteClient, persisted_stream: Stream, primitive_nullable_container: Container
    ) -> None:
        # Use the primitive container from integration fixtures
        space = primitive_nullable_container.space
        container = ContainerId(space=space, external_id=primitive_nullable_container.external_id)

        record_id = RecordId(space, f"rec-{random_string(10, string.ascii_lowercase)}")
        ingest = RecordIngest(
            id=record_id,
            sources=[
                SourceData(
                    source=container,
                    properties={"text": "hello", "int32": 1},
                )
            ],
        )

        cognite_client.data_modeling.records.ingest(persisted_stream.external_id, [ingest])

        # Upsert with a modified property
        upsert = RecordIngest(
            id=record_id,
            sources=[
                SourceData(
                    source=container,
                    properties={"text": "hello2", "int32": 2},
                )
            ],
        )
        cognite_client.data_modeling.records.upsert(persisted_stream.external_id, [upsert])

        # Delete the record
        cognite_client.data_modeling.records.delete(persisted_stream.external_id, record_id)

    def test_sync(
        self, cognite_client: CogniteClient, persisted_stream: Stream, primitive_nullable_container: Container
    ) -> None:
        space = primitive_nullable_container.space
        container = ContainerId(space=space, external_id=primitive_nullable_container.external_id)

        rec_external_id = f"rec-{random_string(10, string.ascii_lowercase)}"
        ingest = RecordIngest(
            id=RecordId(space=space, external_id=rec_external_id),
            sources=[SourceData(source=container, properties={"text": "hello", "int32": 1})],
        )

        # Ingest
        cognite_client.data_modeling.records.ingest(persisted_stream.external_id, [ingest])

        def sync_and_assert_result_present() -> RecordListWithCursor:
            synced = cognite_client.data_modeling.records.sync(
                persisted_stream.external_id, initialize_cursor="1d-ago", limit=100
            )
            assert any(r.id.external_id == rec_external_id for r in synced)
            return synced

        synced = retry_assertion_errors(sync_and_assert_result_present)

        # Expect presence of cursor fields (may be None depending on backend), but ensure attributes exist
        assert hasattr(synced, "cursor")

        # Cleanup
        cognite_client.data_modeling.records.delete(
            persisted_stream.external_id, [RecordId(space=space, external_id=rec_external_id)]
        )

    def test_filter(
        self, cognite_client: CogniteClient, persisted_stream: Stream, primitive_nullable_container: Container
    ) -> None:
        space = primitive_nullable_container.space
        container = ContainerId(space=space, external_id=primitive_nullable_container.external_id)

        rec_external_id = f"rec-{random_string(10, string.ascii_lowercase)}"
        ingest = RecordIngest(
            id=RecordId(space=space, external_id=rec_external_id),
            sources=[SourceData(source=container, properties={"text": "hello", "int32": 1})],
        )

        # Ingest
        cognite_client.data_modeling.records.ingest(persisted_stream.external_id, [ingest])

        # Filter
        def filter_and_assert_result_present() -> None:
            filtered = cognite_client.data_modeling.records.filter(
                persisted_stream.external_id, last_updated_time=LastUpdatedRange(gt=0), limit=100
            )
            assert any(r.id.external_id == rec_external_id for r in filtered)

        retry_assertion_errors(filter_and_assert_result_present)

        # Cleanup
        cognite_client.data_modeling.records.delete(
            persisted_stream.external_id, [RecordId(space=space, external_id=rec_external_id)]
        )
