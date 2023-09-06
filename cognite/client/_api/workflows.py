from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, MutableSequence, Sequence

from cognite.client._api.functions import _create_session_and_return_nonce
from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.workflows import (
    TaskExecution,
    Workflow,
    WorkflowCreate,
    WorkflowExecution,
    WorkflowExecutionDetailed,
    WorkflowExecutionList,
    WorkflowIds,
    WorkflowList,
    WorkflowVersion,
    WorkflowVersionCreate,
    WorkflowVersionId,
    WorkflowVersionList,
)
from cognite.client.exceptions import CogniteAPIError
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

    def update(self, task_id: str, status: Literal["completed", "failed"], output: dict | None = None) -> TaskExecution:
        body: dict[str, Any] = {"status": status.upper()}
        if output is not None:
            body["output"] = output
        response = self._post(
            url_path=f"{self._RESOURCE_PATH}/{task_id}/update",
            json=body,
        )
        return TaskExecution._load(response.json())


class WorkflowExecutionAPI(BetaAPIClient):
    _RESOURCE_PATH = "/workflows/executions"

    def retrieve_detailed(self, external_id: str) -> WorkflowExecutionDetailed | None:
        try:
            response = self._get(url_path=f"{self._RESOURCE_PATH}/{external_id}")
        except CogniteAPIError as e:
            if e.code == 400:
                return None
            raise e
        return WorkflowExecutionDetailed._load(response.json())

    def trigger(
        self,
        workflow_external_id: str,
        version: str,
        input: dict,
    ) -> WorkflowExecution:
        nonce = _create_session_and_return_nonce(self._cognite_client)
        response = self._post(
            url_path=f"/workflows/{workflow_external_id}/versions/{version}/run",
            json={"input": input, "authentication": {"nonce": nonce}},
        )
        return WorkflowExecution._load(response.json())

    def list(
        self,
        workflow_ids: WorkflowVersionId
        | tuple[str, str]
        | MutableSequence[WorkflowVersionId]
        | MutableSequence[tuple[str, str]]
        | None = None,
        created_time_start: int | None = None,
        created_time_end: int | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> WorkflowExecutionList:
        filter_: dict[str, Any] = {}
        if workflow_ids is not None:
            filter_["workflowFilters"] = WorkflowIds._load(workflow_ids).dump(camel_case=True, as_external_id=True)
        if created_time_start is not None:
            filter_["createdTimeStart"] = created_time_start
        if created_time_end is not None:
            filter_["createdTimeEnd"] = created_time_end
        if filter_:
            body = {"filter": filter_}
        else:
            body = None

        response = self._post(url_path=self._RESOURCE_PATH + "/list", json=body, params={"limit": limit})

        return WorkflowExecutionList._load(response.json()["items"])


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

    def retrieve(self, workflow_external_id: str, version: str) -> WorkflowVersion | None:
        try:
            response = self._get(
                url_path=f"/workflows/{workflow_external_id}/versions/{version}",
            )
        except CogniteAPIError as e:
            if e.code == 404:
                return None
            raise e

        return WorkflowVersion._load(response.json())

    def list(
        self,
        workflow_ids: WorkflowVersionId
        | str
        | tuple[str, str]
        | MutableSequence[WorkflowVersionId]
        | MutableSequence[tuple[str, str]]
        | MutableSequence[str]
        | None = None,
    ) -> WorkflowVersionList:
        body: dict | None
        if workflow_ids is None:
            body = None
        else:
            body = {
                "filter": {
                    "workflowFilters": WorkflowIds._load(workflow_ids).dump(camel_case=True, as_external_id=True)
                }
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
