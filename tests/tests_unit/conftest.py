from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Any
from unittest.mock import MagicMock

import pytest

from cognite.client import ClientConfig, CogniteClient, global_config
from cognite.client.credentials import Token
from cognite.client.data_classes import (
    AggregateResultItem,
    Asset,
    Event,
    FileMetadata,
    GeoLocation,
    Label,
    Row,
    SequenceColumn,
    Table,
    ThreeDModel,
    TimeSeries,
)
from cognite.client.data_classes import Sequence as CogniteSequence
from cognite.client.data_classes.agents import Agent, AgentTool
from cognite.client.data_classes.data_modeling import NodeId

# Files to exclude test directories or modules
collect_ignore = ["test_api/function_test_resources"]


# TODO: This class-scoped client causes side-effects between tests...
@pytest.fixture(scope="class")
def cognite_client() -> Iterator[CogniteClient]:
    with pytest.MonkeyPatch.context() as mp:
        # When writing unit tests, typcally with mocked responses, we don't want to wait unnecessarily:
        mp.setattr(global_config, "max_retries", 0)
        mp.setattr(global_config, "max_retries_connect", 0)
        mp.setattr(global_config, "max_retry_backoff", 0)

        cnf = ClientConfig(
            client_name="any",
            project="dummy",
            credentials=Token("bla"),
            timeout=1,
        )
        yield CogniteClient(cnf)


@pytest.fixture(scope="session")
def cognite_mock_client_placeholder() -> CogniteClient:
    """
    This is used for test cases where we need to pass a CogniteClient instance, but we don't actually use it.

    It is a performance optimization to avoid creating a CogniteClientMock for every test case:

    * CogniteClientMock is slow to create, but is stateful so must be created for every test case.

    Quick demo of difference:

    * Unit test with CogniteClientMock: 27s 407ms
    * Unit test with only creating CogniteClientMock once: 19s 765s
    """
    # We allow the mock to pass isinstance checks
    client = MagicMock()
    client.__class__ = CogniteClient  # type: ignore[assignment]
    return client


class DefaultResourceGenerator:
    @staticmethod
    def file_metadata(
        id: int = 1,
        uploaded: bool = True,
        created_time: int = 123,
        last_updated_time: int = 123,
        uploaded_time: int | None = None,
        external_id: str | None = None,
        instance_id: NodeId | None = None,
        name: str | None = None,
        source: str | None = None,
        mime_type: str | None = None,
        metadata: dict[str, str] | None = None,
        directory: str | None = None,
        asset_ids: Sequence[int] | None = None,
        data_set_id: int | None = None,
        labels: Sequence[Label] | None = None,
        geo_location: GeoLocation | None = None,
        source_created_time: int | None = None,
        source_modified_time: int | None = None,
        security_categories: Sequence[int] | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> FileMetadata:
        return FileMetadata(
            id=id,
            uploaded=uploaded,
            created_time=created_time,
            last_updated_time=last_updated_time,
            uploaded_time=uploaded_time,
            external_id=external_id,
            instance_id=instance_id,
            name=name,
            source=source,
            mime_type=mime_type,
            metadata=metadata,
            directory=directory,
            asset_ids=asset_ids,
            data_set_id=data_set_id,
            labels=labels,
            geo_location=geo_location,
            source_created_time=source_created_time,
            source_modified_time=source_modified_time,
            security_categories=security_categories,
            cognite_client=cognite_client,
        )

    @staticmethod
    def event(
        id: int = 1,
        external_id: str | None = None,
        data_set_id: int | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        description: str | None = None,
        type: str | None = None,
        subtype: str | None = None,
        source: str | None = None,
        asset_ids: Sequence[int] | None = None,
        created_time: int = 123,
        last_updated_time: int = 123,
        metadata: dict[str, str] | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> Event:
        return Event(
            id=id,
            external_id=external_id,
            data_set_id=data_set_id,
            start_time=start_time,
            end_time=end_time,
            description=description,
            type=type,
            subtype=subtype,
            source=source,
            asset_ids=asset_ids,
            created_time=created_time,
            last_updated_time=last_updated_time,
            metadata=metadata,
            cognite_client=cognite_client,
        )

    @staticmethod
    def asset(
        id: int = 1,
        external_id: str | None = None,
        parent_id: int | None = None,
        parent_external_id: str | None = None,
        geo_location: GeoLocation | None = None,
        root_id: int | None = None,
        name: str | None = None,
        description: str | None = None,
        metadata: dict[str, str] | None = None,
        source: str | None = None,
        data_set_id: int | None = None,
        created_time: int = 123,
        last_updated_time: int = 123,
        labels: list[Label] | None = None,
        aggregates: AggregateResultItem | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> Asset:
        return Asset(
            id=id,
            external_id=external_id,
            parent_id=parent_id,
            root_id=root_id,
            name=name,
            description=description,
            metadata=metadata,
            source=source,
            data_set_id=data_set_id,
            created_time=created_time,
            last_updated_time=last_updated_time,
            labels=labels or [],
            parent_external_id=parent_external_id,
            geo_location=geo_location,
            aggregates=aggregates,
            cognite_client=cognite_client,
        )

    @staticmethod
    def time_series(
        id: int = 1,
        external_id: str | None = None,
        instance_id: NodeId | None = None,
        is_step: bool = False,
        is_string: bool = False,
        description: str | None = None,
        security_categories: Sequence[int] | None = None,
        metadata: dict[str, str] | None = None,
        unit: str | None = None,
        unit_external_id: str | None = None,
        asset_id: int | None = None,
        data_set_id: int | None = None,
        name: str | None = None,
        legacy_name: str | None = None,
        created_time: int = 123,
        last_updated_time: int = 123,
        cognite_client: CogniteClient | None = None,
    ) -> TimeSeries:
        return TimeSeries(
            id=id,
            external_id=external_id,
            instance_id=instance_id,
            is_step=is_step,
            is_string=is_string,
            description=description,
            security_categories=security_categories,
            metadata=metadata,
            unit=unit,
            unit_external_id=unit_external_id,
            asset_id=asset_id,
            data_set_id=data_set_id,
            name=name,
            legacy_name=legacy_name,
            created_time=created_time,
            last_updated_time=last_updated_time,
            cognite_client=cognite_client,
        )

    @staticmethod
    def sequence(
        id: int = 1,
        external_id: str | None = None,
        name: str | None = None,
        description: str | None = None,
        data_set_id: int | None = None,
        created_time: int = 123,
        last_updated_time: int = 123,
        asset_id: int | None = None,
        metadata: dict[str, str] | None = None,
        columns: Sequence[SequenceColumn] | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> CogniteSequence:
        return CogniteSequence(
            id=id,
            external_id=external_id,
            name=name,
            description=description,
            data_set_id=data_set_id,
            created_time=created_time,
            last_updated_time=last_updated_time,
            cognite_client=cognite_client,
            asset_id=asset_id,
            metadata=metadata,
            columns=columns or [],
        )

    @staticmethod
    def raw_row(
        key: str = "default_key",
        columns: dict[str, Any] | None = None,
        last_updated_time: int = 123,
        cognite_client: CogniteClient | None = None,
    ) -> Row:
        return Row(
            key=key,
            columns=columns or {},
            last_updated_time=last_updated_time,
            cognite_client=cognite_client,
        )

    @staticmethod
    def raw_table(
        name: str = "default_table",
        created_time: int = 123,
        cognite_client: CogniteClient | None = None,
    ) -> Table:
        return Table(
            name=name,
            created_time=created_time,
            cognite_client=cognite_client,
        )

    @staticmethod
    def threed_model(
        id: int = 1,
        name: str = "default_3dmodel",
        data_set_id: int | None = None,
        created_time: int = 123,
        metadata: dict[str, str] | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> ThreeDModel:
        return ThreeDModel(
            id=id,
            name=name,
            data_set_id=data_set_id,
            created_time=created_time,
            metadata=metadata,
            cognite_client=cognite_client,
        )

    @staticmethod
    def agent(
        external_id: str = "test_agent",
        name: str = "Test Agent",
        description: str | None = None,
        instructions: str | None = None,
        model: str | None = None,
        tools: Sequence[AgentTool] | None = None,
        created_time: int = 123,
        last_updated_time: int = 123,
        owner_id: str | None = None,
    ) -> Agent:
        return Agent(
            external_id=external_id,
            name=name,
            description=description,
            instructions=instructions,
            model=model,
            tools=tools,
            created_time=created_time,
            last_updated_time=last_updated_time,
            owner_id=owner_id,
        )
