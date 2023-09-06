from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Sequence

from cognite.client._api_client import APIClient
from cognite.client.data_classes.workflows import (
    Workflow,
    WorkflowCreate,
    WorkflowDefinition,
    WorkflowDefinitionCreate,
    WorkflowExecution,
    WorkflowExecutionList,
    WorkflowId,
    WorkflowList,
)
from cognite.client.utils._identifier import IdentifierSequence

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class WorkflowTaskAPI(APIClient):
    _RESOURCE_PATH = "/workflows/tasks"

    def __init__(
        self,
        config: ClientConfig,
        api_version: str | None,
        cognite_client: CogniteClient,
    ) -> None:
        super().__init__(config, api_version, cognite_client)
        self._api_subversion = "beta"

    def update(self, task_id: str, status: Literal["completed", "failed"], output: dict) -> None:
        ...


class WorkflowExecutionAPI(APIClient):
    _RESOURCE_PATH = "/workflows/executions"

    def __init__(
        self,
        config: ClientConfig,
        api_version: str | None,
        cognite_client: CogniteClient,
    ) -> None:
        super().__init__(config, api_version, cognite_client)
        self._api_subversion = "beta"

    def retrieve(self, external_id: str) -> WorkflowExecution:
        ...

    def trigger(
        self,
        workflow_id: WorkflowId | tuple[str, str],
        input: dict,
        authentication: dict,
    ) -> dict:
        ...

    def list(
        self,
        ids: WorkflowId | Sequence[WorkflowId] | None = None,
        created_time_start: int | None = None,
        created_time_end: int | None = None,
    ) -> WorkflowExecutionList:
        ...


class WorkflowDefinitionAPI(APIClient):
    _RESOURCE_PATH = "/workflows"

    def __init__(
        self,
        config: ClientConfig,
        api_version: str | None,
        cognite_client: CogniteClient,
    ) -> None:
        super().__init__(config, api_version, cognite_client)
        self._api_subversion = "beta"

    def apply(self, definition: WorkflowDefinitionCreate) -> WorkflowDefinition:
        response = self._post(
            url_path=f"{self._RESOURCE_PATH}/{definition.workflow_external_id}/versions",
            json=definition.dump(camel_case=True),
        )

        return WorkflowDefinition._load(response.json())

    # def delete(
    #     self,
    #     workflow_external_id: str,
    #     versions: str | Sequence[str],
    #     ignore_unknown_ids: bool = False,
    # ) -> None:
    #     self._post(
    #         url_path=f"{self._RESOURCE_PATH}/{workflow_external_id}/delete",
    #         params={"ignoreUnknownIds": ignore_unknown_ids},
    #         json=[versions] if isinstance(versions, str) else versions,
    #     )

    def list(self, workflow_external_id: str, version: str | None = None) -> WorkflowList:
        ...


class WorkflowAPI(APIClient):
    _RESOURCE_PATH = "/workflows"

    def __init__(
        self,
        config: ClientConfig,
        api_version: str | None,
        cognite_client: CogniteClient,
    ) -> None:
        super().__init__(config, api_version, cognite_client)
        self._api_subversion = "beta"
        self.definitions = WorkflowDefinitionAPI(config, api_version, cognite_client)
        self.executions = WorkflowExecutionAPI(config, api_version, cognite_client)
        self.tasks = WorkflowTaskAPI(config, api_version, cognite_client)

    def create(self, workflow: WorkflowCreate) -> Workflow:
        response = self._post(
            url_path=self._RESOURCE_PATH,
            json={"items": [workflow.dump(camel_case=True)]},
        )
        return Workflow._load(response.json()["items"][0])

    def retrieve(self, external_id: str) -> Workflow:
        response = self._get(url_path=self._RESOURCE_PATH + f"/{external_id}")
        return Workflow._load(response.json()["items"][0])

    def delete(self, external_id: str | Sequence[str], ignore_unknown_ids: bool = False) -> None:
        self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            params={"ignoreUnknownIds": ignore_unknown_ids},
            delete_limit=100,
            wrap_ids=True,
        )

    def list(self) -> WorkflowList:
        response = self._get(url_path=self._RESOURCE_PATH)
        return WorkflowList._load(response.json()["items"])
