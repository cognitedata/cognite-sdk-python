from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING, Literal, overload

from cognite.client._api.workflows.executions import WorkflowExecutionAPI
from cognite.client._api.workflows.tasks import WorkflowTaskAPI
from cognite.client._api.workflows.triggers import WorkflowTriggerAPI
from cognite.client._api.workflows.versions import WorkflowVersionAPI
from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.workflows import (
    Workflow,
    WorkflowList,
    WorkflowUpsert,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._auxiliary import split_into_chunks
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._url import interpolate_and_url_encode
from cognite.client.utils._validation import assert_type
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import ClientConfig, AsyncCogniteClient


class WorkflowAPI(APIClient):
    _RESOURCE_PATH = "/workflows"

    def __init__(
        self,
        config: ClientConfig,
        api_version: str | None,
        cognite_client: AsyncCogniteClient,
    ) -> None:
        super().__init__(config, api_version, cognite_client)
        self.versions = WorkflowVersionAPI(config, api_version, cognite_client)
        self.executions = WorkflowExecutionAPI(config, api_version, cognite_client)
        self.tasks = WorkflowTaskAPI(config, api_version, cognite_client)
        self.triggers = WorkflowTriggerAPI(config, api_version, cognite_client)
        self._RETRIEVE_LIMIT = 1
        self._CREATE_LIMIT = 1
        self._DELETE_LIMIT = 100

    @overload
    def __call__(self, chunk_size: None = None, limit: int | None = None) -> AsyncIterator[Workflow]: ...

    @overload
    def __call__(self, chunk_size: int, limit: int | None = None) -> AsyncIterator[WorkflowList]: ...

    async def __call__(
        self, chunk_size: int | None = None, limit: int | None = None
    ) -> AsyncIterator[Workflow | WorkflowList]:
        """Iterate over workflows

        Args:
            chunk_size (int | None): The number of workflows to return in each chunk. Defaults to yielding one workflow at a time.
            limit (int | None): Maximum number of workflows to return. Defaults to returning all items.

        Yields:
            Workflow | WorkflowList: Yields Workflow one by one if chunk_size is None, otherwise yields WorkflowList objects.

        """
        async for item in self._list_generator(
            method="GET", resource_cls=Workflow, list_cls=WorkflowList, limit=limit, chunk_size=chunk_size
        ):
            yield item

    @overload
    async def upsert(self, workflow: WorkflowUpsert, mode: Literal["replace"] = "replace") -> Workflow: ...

    @overload
    async def upsert(
        self, workflow: Sequence[WorkflowUpsert], mode: Literal["replace"] = "replace"
    ) -> WorkflowList: ...

    async def upsert(
        self, workflow: WorkflowUpsert | Sequence[WorkflowUpsert], mode: Literal["replace"] = "replace"
    ) -> Workflow | WorkflowList:
        """`Create one or more workflow(s). <https://api-docs.cognite.com/20230101/tag/Workflow-versions/operation/CreateOrUpdateWorkflow>`_

        Note this is an upsert endpoint, so workflows that already exist will be updated, and new ones will be created.

        Args:
            workflow (WorkflowUpsert | Sequence[WorkflowUpsert]): The workflow(s) to upsert.
            mode (Literal['replace']): This is not an option for the API, but is included here to document that the upserts are always done in replace mode.

        Returns:
            Workflow | WorkflowList: The created workflow(s).

        Examples:

            Create one workflow with external id "my_workflow":

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import WorkflowUpsert
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> wf = WorkflowUpsert(external_id="my_workflow", description="my workflow description")
                >>> res = client.workflows.upsert(wf)

            Create multiple workflows:

                >>> wf2 = WorkflowUpsert(external_id="other", data_set_id=123)
                >>> res = client.workflows.upsert([wf, wf2])
        """
        if mode != "replace":
            raise ValueError("Only replace mode is supported for upserting workflows.")

        assert_type(workflow, "workflow", [WorkflowUpsert, Sequence])

        return await self._create_multiple(
            list_cls=WorkflowList,
            resource_cls=Workflow,
            items=workflow,
            input_resource_cls=WorkflowUpsert,
        )

    @overload
    async def retrieve(self, external_id: str, ignore_unknown_ids: bool = False) -> Workflow | None: ...

    @overload
    async def retrieve(self, external_id: SequenceNotStr[str], ignore_unknown_ids: bool = False) -> WorkflowList: ...

    async def retrieve(
        self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> Workflow | WorkflowList | None:
        """`Retrieve one or more workflows. <https://api-docs.cognite.com/20230101/tag/Workflows/operation/fetchWorkflowDetails>`_

        Args:
            external_id (str | SequenceNotStr[str]): Identifier (or sequence of identifiers) for a Workflow. Must be unique.
            ignore_unknown_ids (bool): When requesting multiple workflows, whether to ignore external IDs that are not found rather than throwing an exception.

        Returns:
            Workflow | WorkflowList | None: If a single external ID is specified: the requested workflow, or None if it does not exist. If several external IDs are specified: the requested workflows.

        Examples:

            Retrieve workflow with external ID "my_workflow":

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> workflow = client.workflows.retrieve("my_workflow")

            Retrieve multiple workflows:

                >>> workflow_list = client.workflows.retrieve(["foo", "bar"])
        """

        # We can not use _retrieve_multiple as the backend doesn't support 'ignore_unknown_ids':
        async def get_single(xid: str, ignore_missing: bool = ignore_unknown_ids) -> Workflow | None:
            try:
                response = await self._get(url_path=interpolate_and_url_encode("/workflows/{}", xid))
                return Workflow._load(response.json())
            except CogniteAPIError as e:
                if ignore_missing and e.code == 404:
                    return None
                raise

        if isinstance(external_id, str):
            return await get_single(external_id, ignore_missing=True)

        # Not really a point in splitting into chunks when chunk_size is 1, but...
        tasks = [AsyncSDKTask(get_single, xid) for xid in split_into_chunks(external_id, self._RETRIEVE_LIMIT)]
        tasks_summary = await execute_async_tasks(tasks, fail_fast=True)
        tasks_summary.raise_compound_exception_if_failed_tasks()
        return WorkflowList(list(filter(None, tasks_summary.results)), cognite_client=self._cognite_client)

    async def delete(self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> None:
        """`Delete one or more workflows with versions. <https://api-docs.cognite.com/20230101/tag/Workflows/operation/DeleteWorkflows>`_

        Args:
            external_id (str | SequenceNotStr[str]): External id or list of external ids to delete.
            ignore_unknown_ids (bool): Ignore external ids that are not found rather than throw an exception.

        Examples:

            Delete workflow with external_id "my_workflow":

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.workflows.delete("my_workflow")
        """
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            params={"ignoreUnknownIds": ignore_unknown_ids},
            wrap_ids=True,
        )

    async def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> WorkflowList:
        """`List workflows in the project. <https://api-docs.cognite.com/20230101/tag/Workflows/operation/FetchAllWorkflows>`_

        Args:
            limit (int | None): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None

        Returns:
            WorkflowList: Workflows in the CDF project.

        Examples:

            List all workflows:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.workflows.list(limit=None)
        """
        return await self._list(
            method="GET",
            resource_cls=Workflow,
            list_cls=WorkflowList,
            limit=limit,
        )
