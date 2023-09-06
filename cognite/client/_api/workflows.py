from __future__ import annotations

from typing import TYPE_CHECKING, Literal, MutableSequence, Sequence

from cognite.client._api_client import APIClient
from cognite.client.data_classes.workflows import (
    Workflow,
    WorkflowCreate,
    WorkflowExecution,
    WorkflowExecutionList,
    WorkflowIds,
    WorkflowList,
    WorkflowVersion,
    WorkflowVersionCreate,
    WorkflowVersionId,
    WorkflowVersionList,
)
from cognite.client.utils._identifier import (
    IdentifierSequence,
    WorkflowVersionIdentifierSequence,
)

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class BetaAPIClient(APIClient):
    def __init__(
        self,
        config: ClientConfig,
        api_version: str | None,
        cognite_client: CogniteClient,
    ) -> None:
        super().__init__(config, api_version, cognite_client)
        self._api_subversion = "beta"


class WorkflowTaskAPI(BetaAPIClient):
    _RESOURCE_PATH = "/workflows/tasks"

    def update(self, task_id: str, status: Literal["completed", "failed"], output: dict) -> None:
        ...


class WorkflowExecutionAPI(BetaAPIClient):
    _RESOURCE_PATH = "/workflows/executions"

    def retrieve(self, external_id: str) -> WorkflowExecution:
        ...

    def trigger(
        self,
        workflow_id: WorkflowVersionId | tuple[str, str],
        input: dict,
        authentication: dict,
    ) -> dict:
        ...

    def list(
        self,
        ids: WorkflowVersionId | Sequence[WorkflowVersionId] | None = None,
        created_time_start: int | None = None,
        created_time_end: int | None = None,
    ) -> WorkflowExecutionList:
        ...


class WorkflowVersionAPI(BetaAPIClient):
    _RESOURCE_PATH = "/workflows/versions"

    def create(self, version: WorkflowVersionCreate) -> WorkflowVersion:
        response = self._post(
            url_path=self._RESOURCE_PATH,
            json={"items": [version.dump(camel_case=True)]},
        )

        return WorkflowVersion._load(response.json()["items"][0])

    def delete(
        self,
        workflow_id: WorkflowVersionId
        | tuple[str, str]
        | MutableSequence[WorkflowVersionId]
        | MutableSequence[tuple[str, str]],
        ignore_unknown_ids: bool = False,
    ) -> None:
        identifiers = WorkflowIds._load(workflow_id).dump(camel_case=True)
        self._delete_multiple(
            identifiers=WorkflowVersionIdentifierSequence.load(identifiers),
            params={"ignoreUnknownIds": ignore_unknown_ids},
            delete_limit=100,
            wrap_ids=True,
        )

    def retrieve(self, workflow_external_id: str, version: str) -> WorkflowVersion:
        response = self._get(
            url_path=f"/workflows/{workflow_external_id}/versions/{version}",
        )

        return WorkflowVersion._load(response.json())

    def list(
        self,
        workflow_id: WorkflowVersionId
        | str
        | tuple[str, str]
        | MutableSequence[WorkflowVersionId]
        | MutableSequence[tuple[str, str]]
        | MutableSequence[str]
        | None = None,
    ) -> WorkflowVersionList:
        body: dict | None
        if workflow_id is None:
            body = None
        else:
            body = {
                "filter": {"workflowFilters": WorkflowIds._load(workflow_id).dump(camel_case=True, as_external_id=True)}
            }

        response = self._post(url_path=self._RESOURCE_PATH + "/list", json=body)

        return WorkflowVersionList._load(response.json()["items"])


class WorkflowAPI(BetaAPIClient):
    _RESOURCE_PATH = "/workflows"

    def __init__(
        self,
        config: ClientConfig,
        api_version: str | None,
        cognite_client: CogniteClient,
    ) -> None:
        super().__init__(config, api_version, cognite_client)
        self.versions = WorkflowVersionAPI(config, api_version, cognite_client)
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
        return Workflow._load(response.json())

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
