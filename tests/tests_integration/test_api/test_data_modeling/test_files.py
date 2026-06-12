from __future__ import annotations

import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import Node, NodeApply, NodeId, NodeList, SpaceApply
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteDescribableNode, CogniteFileApply
from cognite.client.data_classes.data_modeling.instances import NodeOrEdgeData
from cognite.client.exceptions import CogniteFileUploadError
from cognite.client.utils._text import random_string

SPACE = "sp_python_sdk_dm_files_tests"
EXT_ID_PREFIX = "pysdk-dmfiles-int-test"


def unique_id(description: str = "file") -> str:
    return f"{EXT_ID_PREFIX}-{description}-{random_string(10)}"


@pytest.fixture(scope="session")
def dm_files_space(cognite_client: CogniteClient) -> str:
    return cognite_client.data_modeling.spaces.apply(
        SpaceApply(
            SPACE, name="SDK DM Files Test Space", description="Space for testing file-related data modeling features"
        )
    ).space


@pytest.fixture(scope="session")
def file_node(cognite_client: CogniteClient, dm_files_space: str) -> Iterator[NodeId]:
    node = CogniteFileApply(
        space=dm_files_space,
        external_id=unique_id(),
        name="integration-test-file.txt",
        mime_type="text/plain",
    )
    cognite_client.data_modeling.instances.apply(node)
    node_id = node.as_id()
    cognite_client.files.upload_content_bytes(b"hello integration test", instance_id=node_id)
    await_file_uploaded(cognite_client, node_id)
    yield node_id
    cognite_client.data_modeling.instances.delete(nodes=node_id)


@pytest.fixture(scope="session")
def non_file_node(cognite_client: CogniteClient, dm_files_space: str) -> Iterator[NodeId]:
    ext_id = unique_id("non-file")
    node = NodeApply(space=dm_files_space, external_id=ext_id)
    cognite_client.data_modeling.instances.apply(nodes=node)
    node_id = NodeId(dm_files_space, ext_id)
    yield node_id
    cognite_client.data_modeling.instances.delete(nodes=node_id)


def await_file_uploaded(client: CogniteClient, node_id: NodeId) -> None:
    for i in range(3):
        files = client.files.retrieve(instance_id=node_id)
        if files is not None and files.uploaded:
            return
        time.sleep(10)
    raise RuntimeError(f"File for {node_id=} never changed status to 'uploaded=True'")


class TestRetrieve:
    def test_single_file_node(self, cognite_client: CogniteClient, file_node: NodeId) -> None:
        result = cognite_client.data_modeling.files.retrieve(file_node)
        assert isinstance(result, Node)
        assert result.as_id() == file_node

    def test_single_non_file_node_returns_none(self, cognite_client: CogniteClient, non_file_node: NodeId) -> None:
        result = cognite_client.data_modeling.files.retrieve(non_file_node)
        assert result is None

    def test_multiple_mixed(self, cognite_client: CogniteClient, file_node: NodeId, non_file_node: NodeId) -> None:
        nonexistent = NodeId(SPACE, "i-do-not-exist-at-all")
        result = cognite_client.data_modeling.files.retrieve([file_node, non_file_node, nonexistent])
        assert isinstance(result, NodeList)
        assert len(result) == 1
        assert result[0].as_id() == file_node

    def test_with_custom_source(self, cognite_client: CogniteClient, file_node: NodeId) -> None:
        describable_view = CogniteDescribableNode.get_source()
        result = cognite_client.data_modeling.files.retrieve(file_node, source=describable_view)
        assert isinstance(result, Node)
        assert result.as_id() == file_node
        assert describable_view in result.properties
        assert CogniteFileApply.get_source() not in result.properties
        # Noddes from a single source (which we indirectly test here), allow short-hand lookup of properties:
        assert result["name"] == "integration-test-file.txt"


class TestList:
    @pytest.mark.usefixtures("file_node")
    def test_list_with_limit(self, cognite_client: CogniteClient) -> None:
        result = cognite_client.data_modeling.files.list(limit=3)
        assert isinstance(result, NodeList)
        assert 1 <= len(result) <= 3


class TestRetrieveDownloadUrls:
    def test_happy_path_passing_tuple(self, cognite_client: CogniteClient, file_node: NodeId) -> None:
        # Pass instance ID as tuple:
        urls = cognite_client.data_modeling.files.retrieve_download_urls(file_node.as_tuple())
        assert isinstance(urls, dict)
        # Ensure that urls are returned purely as NodeIds as per the type return annotation:
        assert file_node in urls
        assert urls[file_node].startswith("http")


# Note, we do not test download methods as they all wrap Files API methods directly with no additional logic.
# The upload methods however...:
class TestUpload:
    def test_upload_path(self, cognite_client: CogniteClient, dm_files_space: str, tmp_path: Path) -> None:
        content_file = tmp_path / "upload-test.txt"
        content_file.write_text("upload path test")
        ext_id = unique_id("upload-path")
        node = CogniteFileApply(
            space=dm_files_space, external_id=ext_id, name="upload-test.txt", mime_type="text/plain"
        )
        node_id = node.as_id()
        try:
            cognite_client.data_modeling.files.upload(content_file, node)
            await_file_uploaded(cognite_client, node_id)
            downloaded = cognite_client.data_modeling.files.download_bytes(node_id)
            assert downloaded == b"upload path test"
        finally:
            cognite_client.data_modeling.instances.delete(nodes=node_id)

    def test_upload_bytes_with_plain_node_apply(self, cognite_client: CogniteClient, dm_files_space: str) -> None:
        ext_id = unique_id("upload-bytes")
        node = NodeApply(
            space=dm_files_space,
            external_id=ext_id,
            sources=[
                NodeOrEdgeData(
                    source=CogniteFileApply.get_source(),
                    properties={"name": "upload-bytes.txt", "mimeType": "text/plain"},
                )
            ],
        )
        node_id = node.as_id()
        try:
            cognite_client.data_modeling.files.upload_bytes(b"bytes content", node)
            await_file_uploaded(cognite_client, node_id)
            downloaded = cognite_client.data_modeling.files.download_bytes(node_id)
            assert downloaded == b"bytes content"
        finally:
            cognite_client.data_modeling.instances.delete(nodes=node_id)

    def test_upload_content_path(self, cognite_client: CogniteClient, dm_files_space: str, tmp_path: Path) -> None:
        content_file = tmp_path / "upload-content.txt"
        content_file.write_text("content path test")

        ext_id = unique_id("upload-content")
        node = CogniteFileApply(
            space=dm_files_space, external_id=ext_id, name="upload-content.txt", mime_type="text/plain"
        )
        node_id = node.as_id()
        try:
            cognite_client.data_modeling.instances.apply(node)
            cognite_client.data_modeling.files.upload_content(content_file, node_id)
            await_file_uploaded(cognite_client, node_id)
            downloaded = cognite_client.data_modeling.files.download_bytes(node_id)
            assert downloaded == b"content path test"
        finally:
            cognite_client.data_modeling.instances.delete(nodes=node_id)

    def test_upload_content_bytes(self, cognite_client: CogniteClient, dm_files_space: str) -> None:
        ext_id = unique_id("upload-content-bytes")
        node = CogniteFileApply(
            space=dm_files_space, external_id=ext_id, name="upload-content-bytes.txt", mime_type="text/plain"
        )
        node_id = node.as_id()
        try:
            cognite_client.data_modeling.instances.apply(node)
            cognite_client.data_modeling.files.upload_content_bytes(b"content bytes test", node_id)
            await_file_uploaded(cognite_client, node_id)
            downloaded = cognite_client.data_modeling.files.download_bytes(node_id)
            assert downloaded == b"content bytes test"
        finally:
            cognite_client.data_modeling.instances.delete(nodes=node_id)


class TestUploadNotFileNode:
    def test_upload_to_non_file_node_raises(
        self, cognite_client: CogniteClient, non_file_node: NodeId, tmp_path: Path
    ) -> None:
        content_file = tmp_path / "dummy.txt"
        content_file.write_text("should fail")
        node = NodeApply(
            space=non_file_node.space,
            external_id=non_file_node.external_id,
        )
        with pytest.raises(CogniteFileUploadError, match="not a file node"):
            cognite_client.data_modeling.files.upload(content_file, node)

    def test_upload_bytes_to_non_file_node_raises(self, cognite_client: CogniteClient, non_file_node: NodeId) -> None:
        node = NodeApply(
            space=non_file_node.space,
            external_id=non_file_node.external_id,
        )
        with pytest.raises(CogniteFileUploadError, match="not a file node"):
            cognite_client.data_modeling.files.upload_bytes(b"should fail", node)

    def test_upload_content_to_non_file_node_raises(
        self, cognite_client: CogniteClient, non_file_node: NodeId, tmp_path: Path
    ) -> None:
        content_file = tmp_path / "dummy.txt"
        content_file.write_text("should fail")
        with pytest.raises(CogniteFileUploadError, match="not a file node"):
            cognite_client.data_modeling.files.upload_content(content_file, non_file_node)

    def test_upload_content_bytes_to_non_file_node_raises(
        self, cognite_client: CogniteClient, non_file_node: NodeId
    ) -> None:
        with pytest.raises(CogniteFileUploadError, match="not a file node"):
            cognite_client.data_modeling.files.upload_content_bytes(b"should fail", non_file_node)

    def test_upload_content_to_nonexistent_node_raises(self, cognite_client: CogniteClient) -> None:
        nonexistent = NodeId(SPACE, "this-node-does-not-exist")
        with pytest.raises(CogniteFileUploadError, match="does not exist"):
            cognite_client.data_modeling.files.upload_content_bytes(b"should fail", nonexistent)
