from __future__ import annotations

import re
from typing import TYPE_CHECKING

import pytest

from cognite.client.data_classes import (
    TransformationTaskParameters,
    WorkflowDefinition,
    WorkflowDefinitionUpsert,
    WorkflowTask,
    WorkflowVersion,
    WorkflowVersionId,
    WorkflowVersionUpsert,
)
from tests.utils import get_url

if TYPE_CHECKING:
    from pytest_httpx2 import HTTPXMock

    from cognite.client import AsyncCogniteClient, CogniteClient


def make_workflow_version(warnings: list[str] | None = None) -> WorkflowVersion:
    task = WorkflowTask(external_id="task1", parameters=TransformationTaskParameters(external_id="my_transformation"))
    return WorkflowVersion(
        workflow_external_id="my_workflow",
        version="1",
        workflow_definition=WorkflowDefinition(hash_="abc123", tasks=[task]),
        created_time=0,
        last_updated_time=0,
        warnings=warnings,
    )


class TestWorkflowVersionAPIWarnings:
    """The backend only returns 'warnings' in the create/upsert response, never for list/retrieve.
    These tests guard against that field accidentally leaking through the other two endpoints,
    e.g. if a future change routes them through some shared response-processing code path."""

    def test_upsert_response_with_warnings_is_kept(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx2_mock: HTTPXMock
    ) -> None:
        response_body = {"items": [make_workflow_version(warnings=["Function 'my_fn' is not deployed"]).dump()]}
        url_pattern = re.compile(re.escape(get_url(async_client.workflows.versions)) + r"/workflows/versions$")
        httpx2_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body)

        new_version = WorkflowVersionUpsert(
            workflow_external_id="my_workflow",
            version="1",
            workflow_definition=WorkflowDefinitionUpsert(
                tasks=[
                    WorkflowTask(
                        external_id="task1", parameters=TransformationTaskParameters(external_id="my_transformation")
                    )
                ]
            ),
        )
        res = cognite_client.workflows.versions.upsert(new_version)

        assert res.warnings == ["Function 'my_fn' is not deployed"]

    def test_list_response_without_warnings_key_leaves_warnings_none(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx2_mock: HTTPXMock
    ) -> None:
        response_body = {"items": [make_workflow_version().dump()]}
        assert "warnings" not in response_body["items"][0]
        url_pattern = re.compile(re.escape(get_url(async_client.workflows.versions)) + r"/workflows/versions/list$")
        httpx2_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body)

        res = cognite_client.workflows.versions.list()

        assert len(res) == 1
        assert res[0].warnings is None

    def test_retrieve_response_without_warnings_key_leaves_warnings_none(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, httpx2_mock: HTTPXMock
    ) -> None:
        response_body = make_workflow_version().dump()
        assert "warnings" not in response_body
        url_pattern = re.compile(
            re.escape(get_url(async_client.workflows.versions)) + r"/workflows/my_workflow/versions/1$"
        )
        httpx2_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body)

        res = cognite_client.workflows.versions.retrieve(WorkflowVersionId("my_workflow", "1"))

        assert res is not None
        assert res.warnings is None

    @pytest.mark.parametrize("endpoint_has_warnings", [True, False])
    def test_load_only_sets_warnings_when_present_in_response(self, endpoint_has_warnings: bool) -> None:
        resource = make_workflow_version(warnings=["some warning"] if endpoint_has_warnings else None).dump()

        loaded = WorkflowVersion._load(resource)

        if endpoint_has_warnings:
            assert loaded.warnings == ["some warning"]
        else:
            assert loaded.warnings is None
