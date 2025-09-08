from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, Literal, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    Workflow,
    WorkflowExecution,
    WorkflowExecutionList,
    WorkflowList,
    WorkflowUpsert,
    WorkflowVersion,
    WorkflowVersionList,
    WorkflowTrigger,
    WorkflowTriggerList,
)
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr


class AsyncWorkflowAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/workflows"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.executions = AsyncWorkflowExecutionAPI(self._config, self._api_version, self._cognite_client)
        self.versions = AsyncWorkflowVersionAPI(self._config, self._api_version, self._cognite_client)
        self.tasks = AsyncWorkflowTaskAPI(self._config, self._api_version, self._cognite_client)
        self.triggers = AsyncWorkflowTriggerAPI(self._config, self._api_version, self._cognite_client)

    async def list(self, all_versions: bool = False, limit: int | None = DEFAULT_LIMIT_READ) -> WorkflowList:
        """List workflows."""
        params = {}
        if all_versions:
            params["allVersions"] = all_versions
        return await self._list(
            list_cls=WorkflowList,
            resource_cls=Workflow,
            method="GET",
            limit=limit,
            other_params=params,
        )

    async def retrieve(self, workflow_external_id: str, version: str | None = None) -> Workflow | None:
        """Retrieve workflow."""
        try:
            path = f"{self._RESOURCE_PATH}/{workflow_external_id}"
            if version:
                path += f"/versions/{version}"
            res = await self._get(url_path=path)
            return Workflow._load(res.json(), cognite_client=self._cognite_client)
        except Exception:
            return None

    async def upsert(self, workflow: WorkflowUpsert | Sequence[WorkflowUpsert]) -> Workflow | WorkflowList:
        """Upsert workflows."""
        return await self._create_multiple(
            list_cls=WorkflowList,
            resource_cls=Workflow,
            items=workflow,
        )

    async def delete(self, workflow_external_id: str | Sequence[str]) -> None:
        """Delete workflows."""
        external_ids = [workflow_external_id] if isinstance(workflow_external_id, str) else workflow_external_id
        for ext_id in external_ids:
            await self._delete(url_path=f"{self._RESOURCE_PATH}/{ext_id}")


class AsyncWorkflowExecutionAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/workflows/executions"

    async def list(self, workflow_external_id: str | None = None, limit: int | None = DEFAULT_LIMIT_READ) -> WorkflowExecutionList:
        """List workflow executions."""
        filter = {}
        if workflow_external_id:
            filter["workflowExternalId"] = workflow_external_id
        return await self._list(
            list_cls=WorkflowExecutionList,
            resource_cls=WorkflowExecution,
            method="POST",
            limit=limit,
            filter=filter,
        )


class AsyncWorkflowVersionAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/workflows/versions"

    async def list(self, workflow_external_id: str, limit: int | None = DEFAULT_LIMIT_READ) -> WorkflowVersionList:
        """List workflow versions."""
        return await self._list(
            list_cls=WorkflowVersionList,
            resource_cls=WorkflowVersion,
            method="GET",
            limit=limit,
            resource_path=f"/workflows/{workflow_external_id}/versions",
        )


class AsyncWorkflowTaskAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/workflows/tasks"

    async def list(self, workflow_external_id: str, version: str, limit: int | None = DEFAULT_LIMIT_READ) -> dict:
        """List workflow tasks."""
        res = await self._get(url_path=f"/workflows/{workflow_external_id}/versions/{version}/workflowtasks")
        return res.json()


class AsyncWorkflowTriggerAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/workflows/triggers"

    async def list(self, workflow_external_id: str | None = None, limit: int | None = DEFAULT_LIMIT_READ) -> WorkflowTriggerList:
        """List workflow triggers."""
        filter = {}
        if workflow_external_id:
            filter["workflowExternalId"] = workflow_external_id
        return await self._list(
            list_cls=WorkflowTriggerList,
            resource_cls=WorkflowTrigger,
            method="POST",
            limit=limit,
            filter=filter,
        )
