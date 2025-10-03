from __future__ import annotations

import warnings
from collections.abc import Iterator, MutableSequence, Sequence
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.workflows import (
    WorkflowIds,
    WorkflowVersion,
    WorkflowVersionId,
    WorkflowVersionList,
    WorkflowVersionUpsert,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._auxiliary import interpolate_and_url_encode, split_into_chunks
from cognite.client.utils._concurrency import execute_tasks
from cognite.client.utils._identifier import WorkflowVersionIdentifierSequence
from cognite.client.utils._validation import assert_type

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient

WorkflowIdentifier: TypeAlias = WorkflowVersionId | tuple[str, str] | str
WorkflowVersionIdentifier: TypeAlias = WorkflowVersionId | tuple[str, str]


def wrap_workflow_ids(
    workflow_version_ids: WorkflowIdentifier | MutableSequence[WorkflowIdentifier] | None,
) -> list[dict[str, Any]]:
    if workflow_version_ids is None:
        return []
    return WorkflowIds.load(workflow_version_ids).dump(camel_case=True, as_external_id=True)


class WorkflowVersionAPI(APIClient):
    _RESOURCE_PATH = "/workflows/versions"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._CREATE_LIMIT = 1
        self._RETRIEVE_LIMIT = 1
        self._DELETE_LIMIT = 100

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        workflow_version_ids: WorkflowIdentifier | MutableSequence[WorkflowIdentifier] | None = None,
        limit: int | None = None,
    ) -> Iterator[WorkflowVersion]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        workflow_version_ids: WorkflowIdentifier | MutableSequence[WorkflowIdentifier] | None = None,
        limit: int | None = None,
    ) -> Iterator[WorkflowVersionList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        workflow_version_ids: WorkflowIdentifier | MutableSequence[WorkflowIdentifier] | None = None,
        limit: int | None = None,
    ) -> Iterator[WorkflowVersion] | Iterator[WorkflowVersionList]:
        """Iterate over workflow versions

        Args:
            chunk_size (int | None): The number of workflow versions to return in each chunk. Defaults to yielding one workflow version at a time.
            workflow_version_ids (WorkflowIdentifier | MutableSequence[WorkflowIdentifier] | None): Workflow version id or list of workflow version ids to filter on.
            limit (int | None): Maximum number of workflow versions to return. Defaults to returning all.

        Returns:
            Iterator[WorkflowVersion] | Iterator[WorkflowVersionList]: Yields WorkflowVersion one by one if chunk_size is None, otherwise yields WorkflowVersionList objects.
        """
        return self._list_generator(
            method="GET",
            resource_cls=WorkflowVersion,
            list_cls=WorkflowVersionList,
            filter={"workflowFilters": wrap_workflow_ids(workflow_version_ids)},
            limit=limit,
            chunk_size=chunk_size,
        )

    def __iter__(self) -> Iterator[WorkflowVersion]:
        """Iterate all over workflow versions"""
        return self()

    @overload
    def upsert(self, version: WorkflowVersionUpsert) -> WorkflowVersion: ...

    @overload
    def upsert(self, version: Sequence[WorkflowVersionUpsert]) -> WorkflowVersionList: ...

    def upsert(
        self, version: WorkflowVersionUpsert | Sequence[WorkflowVersionUpsert], mode: Literal["replace"] = "replace"
    ) -> WorkflowVersion | WorkflowVersionList:
        """`Create one or more workflow version(s). <https://api-docs.cognite.com/20230101/tag/Workflow-versions/operation/CreateOrUpdateWorkflowVersion>`_

        Note this is an upsert endpoint, so workflow versions that already exist will be updated, and new ones will be created.

        Args:
            version (WorkflowVersionUpsert | Sequence[WorkflowVersionUpsert]): The workflow version(s) to upsert.
            mode (Literal['replace']): This is not an option for the API, but is included here to document that the upserts are always done in replace mode.

        Returns:
            WorkflowVersion | WorkflowVersionList: The created workflow version(s).

        Examples:

            Create one workflow version with a single Function task:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import (
                ...     WorkflowVersionUpsert, WorkflowDefinitionUpsert,
                ...     WorkflowTask, FunctionTaskParameters,
                ... )
                >>> client = CogniteClient()
                >>> function_task = WorkflowTask(
                ...     external_id="my_workflow-task1",
                ...     parameters=FunctionTaskParameters(
                ...         external_id="my_fn_xid",
                ...         data={"a": 1, "b": 2},
                ...     ),
                ... )
                >>> new_version = WorkflowVersionUpsert(
                ...    workflow_external_id="my_workflow",
                ...    version="1",
                ...    workflow_definition=WorkflowDefinitionUpsert(
                ...        tasks=[function_task],
                ...        description="This workflow has one step",
                ...    ),
                ... )
                >>> res = client.workflows.versions.upsert(new_version)
        """
        if mode != "replace":
            raise ValueError("Only replace mode is supported for upserting workflow versions.")

        assert_type(version, "workflow version", [WorkflowVersionUpsert, Sequence])

        return self._create_multiple(
            list_cls=WorkflowVersionList,
            resource_cls=WorkflowVersion,
            items=version,
            input_resource_cls=WorkflowVersionUpsert,
        )

    def delete(
        self,
        workflow_version_id: WorkflowVersionIdentifier | MutableSequence[WorkflowVersionIdentifier],
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete a workflow version(s). <https://api-docs.cognite.com/20230101/tag/Workflow-versions/operation/DeleteSpecificVersionsOfWorkflow>`_

        Args:
            workflow_version_id (WorkflowVersionIdentifier | MutableSequence[WorkflowVersionIdentifier]): Workflow version id or list of workflow version ids to delete.
            ignore_unknown_ids (bool): Ignore external ids that are not found rather than throw an exception.

        Examples:

            Delete workflow version "1" of workflow "my workflow" specified by using a tuple:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.workflows.versions.delete(("my workflow", "1"))

            Delete workflow version "1" of workflow "my workflow" and workflow version "2" of workflow "my workflow 2" using the WorkflowVersionId class:

                >>> from cognite.client.data_classes import WorkflowVersionId
                >>> client.workflows.versions.delete([WorkflowVersionId("my workflow", "1"), WorkflowVersionId("my workflow 2", "2")])

        """
        identifiers = WorkflowIds.load(workflow_version_id).dump(camel_case=True)
        self._delete_multiple(
            identifiers=WorkflowVersionIdentifierSequence.load(identifiers),
            params={"ignoreUnknownIds": ignore_unknown_ids},
            wrap_ids=True,
        )

    @overload
    def retrieve(
        self,
        workflow_external_id: WorkflowVersionIdentifier | str,
        version: str | None = None,
        *,
        ignore_unknown_ids: bool = False,
    ) -> WorkflowVersion | None: ...

    @overload
    def retrieve(
        self,
        workflow_external_id: Sequence[WorkflowVersionIdentifier] | WorkflowIds,
        version: None = None,
        *,
        ignore_unknown_ids: bool = False,
    ) -> WorkflowVersionList: ...

    def retrieve(
        self,
        workflow_external_id: WorkflowVersionIdentifier | Sequence[WorkflowVersionIdentifier] | WorkflowIds | str,
        version: str | None = None,
        *,
        ignore_unknown_ids: bool = False,
    ) -> WorkflowVersion | WorkflowVersionList | None:
        """`Retrieve a workflow version. <https://api-docs.cognite.com/20230101/tag/Workflow-versions/operation/GetSpecificVersion>`_

        Args:
            workflow_external_id (WorkflowVersionIdentifier | Sequence[WorkflowVersionIdentifier] | WorkflowIds | str): External id of the workflow.
            version (str | None): Version of the workflow.
            ignore_unknown_ids (bool): When requesting multiple, whether to ignore external IDs that are not found rather than throwing an exception.

        Returns:
            WorkflowVersion | WorkflowVersionList | None: If a single identifier is specified: the requested workflow version, or None if it does not exist. If several ids are specified: the requested workflow versions.

        Examples:

            Retrieve workflow version 'v1' of workflow "my_workflow":

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import WorkflowVersionId
                >>> client = CogniteClient()
                >>> res = client.workflows.versions.retrieve(WorkflowVersionId("my_workflow", "v1"))

            Retrieve multiple workflow versions and ignore unknown:

                >>> res = client.workflows.versions.retrieve(
                ...     [WorkflowVersionId("my_workflow", "v1"), WorkflowVersionId("other", "v3.2")],
                ...     ignore_unknown_ids=True,
                ... )
                >>> # A sequence of tuples is also accepted:
                >>> res = client.workflows.versions.retrieve([("my_workflow", "v1"), ("other", "v3.2")])

            DEPRECATED: You can also pass workflow_external_id and version as separate arguments:

                >>> res = client.workflows.versions.retrieve("my_workflow", "v1")

        """
        match workflow_external_id, version:
            case str(), str():
                warnings.warn(
                    "This usage is deprecated, please pass one or more `WorkflowVersionId` instead.'",
                    DeprecationWarning,
                )
                workflow_external_id = WorkflowVersionId(workflow_external_id, version)
            case str(), None:
                raise TypeError(
                    "You must specify which 'version' of the workflow to retrieve. Deprecation Warning: This usage is deprecated, please pass "
                    "one or more `WorkflowVersionId` instead."
                )
            case WorkflowVersionId() | Sequence(), str():
                warnings.warn("Argument 'version' is ignored when passing one or more 'WorkflowVersionId'", UserWarning)

        # We can not use _retrieve_multiple as the backend doesn't support 'ignore_unknown_ids':
        def get_single(wf_xid: WorkflowVersionId, ignore_missing: bool = ignore_unknown_ids) -> WorkflowVersion | None:
            try:
                response = self._get(
                    url_path=interpolate_and_url_encode("/workflows/{}/versions/{}", *wf_xid.as_tuple())
                )
                return WorkflowVersion._load(response.json())
            except CogniteAPIError as e:
                if ignore_missing and e.code == 404:
                    return None
                raise

        # WorkflowVersionId doesn't require 'version' to be given, so we raise in case it is missing:
        given_wf_ids = WorkflowIds.load(workflow_external_id)
        if any(wf_id.version is None for wf_id in given_wf_ids):
            raise ValueError("Version must be specified for all workflow version IDs.")

        is_single = isinstance(workflow_external_id, WorkflowVersionId) or (
            isinstance(workflow_external_id, tuple) and len(given_wf_ids) == 1
        )
        if is_single:
            return get_single(given_wf_ids[0], ignore_missing=True)

        # Not really a point in splitting into chunks when chunk_size is 1, but...
        tasks = list(map(tuple, split_into_chunks(given_wf_ids, self._RETRIEVE_LIMIT)))
        tasks_summary = execute_tasks(get_single, tasks=tasks, max_workers=self._config.max_workers, fail_fast=True)
        tasks_summary.raise_compound_exception_if_failed_tasks()
        return WorkflowVersionList(list(filter(None, tasks_summary.results)), cognite_client=self._cognite_client)

    def list(
        self,
        workflow_version_ids: WorkflowIdentifier | MutableSequence[WorkflowIdentifier] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> WorkflowVersionList:
        """`List workflow versions in the project <https://api-docs.cognite.com/20230101/tag/Workflow-versions/operation/ListWorkflowVersions>`_

        Args:
            workflow_version_ids (WorkflowIdentifier | MutableSequence[WorkflowIdentifier] | None): Workflow version id or list of workflow version ids to filter on.
            limit (int | None): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None

        Returns:
            WorkflowVersionList: The requested workflow versions.

        Examples:

            Get all workflow version for workflows 'my_workflow' and 'my_workflow_2':

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.versions.list(["my_workflow", "my_workflow_2"])

            Get all workflow versions for workflows 'my_workflow' and 'my_workflow_2' using the WorkflowVersionId class:

                >>> from cognite.client.data_classes import WorkflowVersionId
                >>> res = client.workflows.versions.list(
                ...     [WorkflowVersionId("my_workflow"), WorkflowVersionId("my_workflow_2")])

            Get all workflow versions for workflows 'my_workflow' version '1' and 'my_workflow_2' version '2' using tuples:

                >>> res = client.workflows.versions.list(
                ...     [("my_workflow", "1"), ("my_workflow_2", "2")])

        """
        return self._list(
            method="POST",
            resource_cls=WorkflowVersion,
            list_cls=WorkflowVersionList,
            filter={"workflowFilters": wrap_workflow_ids(workflow_version_ids)},
            limit=limit,
        )
