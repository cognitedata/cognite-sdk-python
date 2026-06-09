from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, NoReturn
from unittest.mock import AsyncMock

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import CogniteClient
from cognite.client._api.data_modeling.files import COGNITE_FILE_VIEW_ID
from cognite.client._cognite_client import AsyncCogniteClient
from cognite.client.data_classes.data_modeling import Node, NodeId, NodeList, ViewId
from cognite.client.data_classes.data_modeling.instances import NodeApply, Properties
from cognite.client.exceptions import CogniteFileUploadError, CogniteNotFoundError
from tests.tests_unit.conftest import DefaultResourceGenerator

CUSTOM_VIEW_ID = ViewId("my-space", "MyView", "v1")


def single_node(
    space: str = "sp",
    external_id: str = "acex",
    extra_sources: dict[ViewId, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    props = Properties(
        {COGNITE_FILE_VIEW_ID: {"name": "a-file"}, **(extra_sources or {})},
    )
    return Node(
        space=space,
        external_id=external_id,
        version=1,
        last_updated_time=0,
        created_time=0,
        deleted_time=None,
        properties=props,
        type=None,
    ).dump(camel_case=True)


# avoid gzip compression in tests to simplify inspection of request bodies:
@pytest.mark.usefixtures("disable_gzip")
class TestDMFilesRetrieve:
    def test_default_source_sends_single_source_in_request(
        self, cognite_client: CogniteClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(r".*/models/instances/byids$"),
            json={"items": [single_node()]},
        )
        cognite_client.data_modeling.files.retrieve(NodeId("s", "x"))

        body = json.loads(httpx_mock.get_requests()[0].content)
        sources = body.get("sources", [])
        assert len(sources) == 1
        assert sources[0]["source"]["externalId"] == COGNITE_FILE_VIEW_ID.external_id

    def test_custom_source_sends_two_sources_and_strips_cognite_file_props(
        self, cognite_client: CogniteClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(r".*/models/instances/byids$"),
            json={"items": [single_node(extra_sources={CUSTOM_VIEW_ID: {"custom_prop": 42}})]},
        )
        result = cognite_client.data_modeling.files.retrieve(NodeId("s", "x"), source=CUSTOM_VIEW_ID)

        body = json.loads(httpx_mock.get_requests()[0].content)
        sources = body.get("sources", [])
        assert len(sources) == 2

        assert isinstance(result, Node)
        assert COGNITE_FILE_VIEW_ID not in result.properties
        assert CUSTOM_VIEW_ID in result.properties
        assert result["custom_prop"] == 42

    def test_single_id_not_found_returns_none(self, cognite_client: CogniteClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(r".*/models/instances/byids$"),
            json={"items": []},
        )
        result = cognite_client.data_modeling.files.retrieve(NodeId("s", "missing"))
        assert result is None

    def test_single_id_found_returns_node_not_list(self, cognite_client: CogniteClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(r".*/models/instances/byids$"),
            json={"items": [single_node()]},
        )
        result = cognite_client.data_modeling.files.retrieve(NodeId("s", "x"))
        assert isinstance(result, Node)

    def test_list_of_ids_returns_node_list(self, cognite_client: CogniteClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(r".*/models/instances/byids$"),
            json={"items": [single_node("s", "xx"), single_node("s", "yy")]},
        )
        result = cognite_client.data_modeling.files.retrieve([NodeId("s", "xx"), NodeId("s", "yy")])
        assert isinstance(result, NodeList)
        assert len(result) == 2


NODE_ID = NodeId("sp", "x")
NOT_FOUND = CogniteNotFoundError("not found", code=404, missing=[])


def make_file_node() -> Node:
    return Node(
        space="sp",
        external_id="x",
        version=1,
        last_updated_time=0,
        created_time=0,
        deleted_time=None,
        type=None,
        properties=Properties({COGNITE_FILE_VIEW_ID: {"name": "a-file"}}),
    )


def make_non_file_node() -> Node:
    return Node(
        space="sp",
        external_id="x",
        version=1,
        last_updated_time=0,
        created_time=0,
        deleted_time=None,
        type=None,
        properties=None,
    )


class TestUploadWithRetry:
    async def test_zero_max_retries_raises_upload_error(
        self, async_client: AsyncCogniteClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from cognite.client.config import global_config

        async def failing_upload() -> NoReturn:
            raise NOT_FOUND

        monkeypatch.setattr(global_config, "max_retries", 0)
        monkeypatch.setattr(global_config, "max_retry_backoff", 1)
        with pytest.raises(CogniteFileUploadError, match="after 1 attempt"):
            await async_client.data_modeling.files._upload_with_retry(NODE_ID, failing_upload, NOT_FOUND)


class TestUploadToNewlyCreatedFileNode:
    """
    These tests are just unit tests for the following reason: all API calls goes out to either Data Modeling Instances API or
    the Files API, which are already integration-tested separately. The main point of these tests is to verify that the
    retry logic is correctly wired up to the right exceptions and conditions, and that the happy path returns the expected value.
    """

    async def test_happy_path_returns_none(self, async_client: AsyncCogniteClient) -> None:
        upload_fn = AsyncMock(return_value=DefaultResourceGenerator.file_metadata())
        result = await async_client.data_modeling.files._upload_to_newly_created_file_node(NODE_ID, upload_fn)
        assert result is None
        upload_fn.assert_awaited_once()

    async def test_not_a_file_node_raises(
        self, async_client: AsyncCogniteClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        upload_fn = AsyncMock(side_effect=NOT_FOUND)
        dm_files = async_client.data_modeling.files
        monkeypatch.setattr(dm_files, "retrieve", AsyncMock(return_value=None))

        with pytest.raises(CogniteFileUploadError, match="not a file node"):
            await dm_files._upload_to_newly_created_file_node(NODE_ID, upload_fn)

    async def test_file_node_with_propagation_delay_retries(
        self, async_client: AsyncCogniteClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from cognite.client.config import global_config

        monkeypatch.setattr(global_config, "max_retries", 1)
        monkeypatch.setattr(global_config, "max_retry_backoff", 0)

        upload_fn = AsyncMock(side_effect=[NOT_FOUND, DefaultResourceGenerator.file_metadata()])
        dm_files = async_client.data_modeling.files
        monkeypatch.setattr(dm_files, "retrieve", AsyncMock(return_value=make_file_node()))

        await dm_files._upload_to_newly_created_file_node(NODE_ID, upload_fn)
        assert upload_fn.await_count == 2


class TestUploadToExistingNode:
    async def test_happy_path_returns_none(self, async_client: AsyncCogniteClient) -> None:
        upload_fn = AsyncMock(return_value=DefaultResourceGenerator.file_metadata())
        result = await async_client.data_modeling.files._upload_to_existing_node(NODE_ID, upload_fn)
        assert result is None

    async def test_node_does_not_exist_raises(
        self, async_client: AsyncCogniteClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        upload_fn = AsyncMock(side_effect=NOT_FOUND)
        dm_files = async_client.data_modeling.files
        monkeypatch.setattr(dm_files, "retrieve", AsyncMock(return_value=None))
        monkeypatch.setattr(dm_files._instances_api, "retrieve_nodes", AsyncMock(return_value=None))

        with pytest.raises(CogniteFileUploadError, match="does not exist"):
            await dm_files._upload_to_existing_node(NODE_ID, upload_fn)

    async def test_node_exists_but_not_a_file_raises(
        self, async_client: AsyncCogniteClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        upload_fn = AsyncMock(side_effect=NOT_FOUND)
        dm_files = async_client.data_modeling.files
        monkeypatch.setattr(dm_files, "retrieve", AsyncMock(return_value=None))
        monkeypatch.setattr(dm_files._instances_api, "retrieve_nodes", AsyncMock(return_value=make_non_file_node()))

        with pytest.raises(CogniteFileUploadError, match="exists but is not a file node"):
            await dm_files._upload_to_existing_node(NODE_ID, upload_fn)

    async def test_file_node_with_propagation_delay_retries(
        self, async_client: AsyncCogniteClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from cognite.client.config import global_config

        monkeypatch.setattr(global_config, "max_retries", 2)
        monkeypatch.setattr(global_config, "max_retry_backoff", 0)

        upload_fn = AsyncMock(side_effect=[NOT_FOUND, NOT_FOUND, DefaultResourceGenerator.file_metadata()])
        dm_files = async_client.data_modeling.files
        monkeypatch.setattr(dm_files, "retrieve", AsyncMock(return_value=make_file_node()))

        await dm_files._upload_to_existing_node(NODE_ID, upload_fn)
        assert upload_fn.await_count == 3


class TestUploadPublicMethods:
    async def test_upload_applies_node_then_uploads(
        self, async_client: AsyncCogniteClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        dm_files = async_client.data_modeling.files
        apply_mock = AsyncMock()
        upload_mock = AsyncMock()
        monkeypatch.setattr(dm_files._instances_api, "apply", apply_mock)
        monkeypatch.setattr(dm_files, "_upload_to_newly_created_file_node", upload_mock)

        node = NodeApply(space="sp", external_id="x")
        await dm_files.upload(path=Path("dummy.txt"), node=node)

        apply_mock.assert_awaited_once_with(nodes=node)
        upload_mock.assert_awaited_once()

    async def test_upload_content_delegates_to_existing_node_helper(
        self, async_client: AsyncCogniteClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        dm_files = async_client.data_modeling.files
        existing_mock = AsyncMock()
        monkeypatch.setattr(dm_files, "_upload_to_existing_node", existing_mock)

        await dm_files.upload_content(path=Path("dummy.txt"), node_id=("sp", "x"))

        existing_mock.assert_awaited_once()
        assert existing_mock.call_args[0][0] == NODE_ID


@pytest.mark.usefixtures("disable_gzip")
class TestDMFilesList:
    def test_default_source_sends_single_source_in_request(
        self, cognite_client: CogniteClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(r".*/models/instances/list$"),
            json={"items": []},
        )
        cognite_client.data_modeling.files.list(limit=1)

        body = json.loads(httpx_mock.get_requests()[0].content)
        sources = body.get("sources", [])
        assert len(sources) == 1
        assert sources[0]["source"]["externalId"] == COGNITE_FILE_VIEW_ID.external_id

    def test_custom_source_sends_two_sources_and_strips_cognite_file_props(
        self, cognite_client: CogniteClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(r".*/models/instances/list$"),
            json={
                "items": [
                    single_node("s", "xx", extra_sources={CUSTOM_VIEW_ID: {"custom_prop": 42}}),
                    single_node("s", "yy", extra_sources={CUSTOM_VIEW_ID: {"custom_prop": 43}}),
                ]
            },
        )
        results = cognite_client.data_modeling.files.list(source=CUSTOM_VIEW_ID, limit=5)

        body = json.loads(httpx_mock.get_requests()[0].content)
        sources = body.get("sources", [])
        assert len(sources) == 2

        assert isinstance(results, NodeList)
        for node in results:
            assert COGNITE_FILE_VIEW_ID not in node.properties
            assert CUSTOM_VIEW_ID in node.properties
