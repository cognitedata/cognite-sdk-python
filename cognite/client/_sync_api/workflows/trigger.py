"""
===============================================================================
f416c36a9b35f9fa884c22ad837aa3f4
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any, TypeVar

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.workflows import (
    WorkflowTrigger,
    WorkflowTriggerCreate,
    WorkflowTriggerList,
    WorkflowTriggerRunList,
    WorkflowTriggerUpsert,
)
from cognite.client.utils._concurrency import ConcurrencySettings
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
from cognite.client.data_classes import ClientCredentials

_T = TypeVar("_T")


class SyncWorkflowTriggerAPI:
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient):
        self.__async_client = async_client

    def _run_sync(self, coro: Coroutine[Any, Any, _T]) -> _T:
        executor = ConcurrencySettings._get_event_loop_executor()
        return executor.run_coro(coro)

    def upsert(
        self, workflow_trigger: WorkflowTriggerUpsert, client_credentials: ClientCredentials | dict | None = None
    ) -> WorkflowTrigger:
        """
        `Create or update a trigger for a workflow. <https://api-docs.cognite.com/20230101/tag/Workflow-triggers/operation/CreateOrUpdateTriggers>`_

        Args:
            workflow_trigger (WorkflowTriggerUpsert): The workflow trigger specification.
            client_credentials (ClientCredentials | dict | None): Specific credentials that should be used to trigger the workflow execution. When passed will take precedence over the current credentials.

        Returns:
            WorkflowTrigger: The created or updated workflow trigger specification.

        Examples:

            Create or update a scheduled trigger for a workflow:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.workflows import WorkflowTriggerUpsert, WorkflowScheduledTriggerRule
                >>> from zoneinfo import ZoneInfo
                >>> client = CogniteClient()
                >>> client.workflows.triggers.upsert(
                ...     WorkflowTriggerUpsert(
                ...         external_id="my_trigger",
                ...         trigger_rule=WorkflowScheduledTriggerRule(cron_expression="0 0 * * *", timezone=ZoneInfo("UTC")),
                ...         workflow_external_id="my_workflow",
                ...         workflow_version="1",
                ...         input={"a": 1, "b": 2},
                ...         metadata={"key": "value"},
                ...     )
                ... )

            Create or update a data modeling trigger for a workflow:

                >>> from cognite.client.data_classes.workflows import WorkflowDataModelingTriggerRule, WorkflowTriggerDataModelingQuery
                >>> from cognite.client.data_classes.data_modeling.query import NodeResultSetExpression, Select, SourceSelector
                >>> from cognite.client.data_classes.data_modeling import ViewId
                >>> from cognite.client.data_classes.filters import Equals
                >>> view_id = ViewId("my_space_id", "view_external_id", "v1")
                >>> client.workflows.triggers.upsert(
                ...     WorkflowTriggerUpsert(
                ...         external_id="my_trigger",
                ...         trigger_rule=WorkflowDataModelingTriggerRule(
                ...             data_modeling_query=WorkflowTriggerDataModelingQuery(
                ...                 with_={"timeseries": NodeResultSetExpression(filter=Equals(view_id.as_property_ref("name"), value="my_name"))},
                ...                 select={"timeseries": Select([SourceSelector(view_id, ["name"])])},
                ...             ),
                ...             batch_size=500,
                ...             batch_timeout=300,
                ...         ),
                ...         workflow_external_id="my_workflow",
                ...         workflow_version="1",
                ...     )
                ... )
        """
        return self._run_sync(
            self.__async_client.workflows.triggers.upsert(
                workflow_trigger=workflow_trigger, client_credentials=client_credentials
            )
        )

    def create(
        self, workflow_trigger: WorkflowTriggerCreate, client_credentials: ClientCredentials | dict | None = None
    ) -> WorkflowTrigger:
        """
        Create or update a trigger for a workflow.

        .. admonition:: Deprecation Warning

            This method is deprecated, use '.upsert' instead. It will be removed in the next major version.
        """
        return self._run_sync(
            self.__async_client.workflows.triggers.create(
                workflow_trigger=workflow_trigger, client_credentials=client_credentials
            )
        )

    def delete(self, external_id: str | SequenceNotStr[str]) -> None:
        """
        `Delete one or more triggers for a workflow. <https://api-docs.cognite.com/20230101/tag/Workflow-triggers/operation/deleteTriggers>`_

        Args:
            external_id (str | SequenceNotStr[str]): The external id(s) of the trigger(s) to delete.

        Examples:

            Delete a trigger with external id 'my_trigger':

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.workflows.triggers.delete("my_trigger")

            Delete a list of triggers:

                >>> client.workflows.triggers.delete(["my_trigger", "another_trigger"])
        """
        return self._run_sync(self.__async_client.workflows.triggers.delete(external_id=external_id))

    def get_triggers(self, limit: int | None = DEFAULT_LIMIT_READ) -> WorkflowTriggerList:
        """
        List the workflow triggers.

        .. admonition:: Deprecation Warning

            This method is deprecated, use '.list' instead. It will be removed in the next major version.
        """
        return self._run_sync(self.__async_client.workflows.triggers.get_triggers(limit=limit))

    def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> WorkflowTriggerList:
        """
        `List the workflow triggers. <https://api-docs.cognite.com/20230101/tag/Workflow-triggers/operation/getTriggers>`_

        Args:
            limit (int | None): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            WorkflowTriggerList: The list of triggers.

        Examples:

            List all triggers:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.workflows.triggers.list(limit=None)
        """
        return self._run_sync(self.__async_client.workflows.triggers.list(limit=limit))

    def get_trigger_run_history(
        self, external_id: str, limit: int | None = DEFAULT_LIMIT_READ
    ) -> WorkflowTriggerRunList:
        """
        List the history of runs for a trigger.

        .. admonition:: Deprecation Warning

            This method is deprecated, use '.list_runs' instead. It will be removed in the next major version.
        """
        return self._run_sync(
            self.__async_client.workflows.triggers.get_trigger_run_history(external_id=external_id, limit=limit)
        )

    def list_runs(self, external_id: str, limit: int | None = DEFAULT_LIMIT_READ) -> WorkflowTriggerRunList:
        """
        `List the history of runs for a trigger. <https://api-docs.cognite.com/20230101/tag/Workflow-triggers/operation/getTriggerHistory>`_

        Args:
            external_id (str): The external id of the trigger to list runs for.
            limit (int | None): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            WorkflowTriggerRunList: The requested trigger runs.

        Examples:

            Get all runs for a trigger with external id 'my_trigger':

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.workflows.triggers.list_runs("my_trigger", limit=None)
        """
        return self._run_sync(self.__async_client.workflows.triggers.list_runs(external_id=external_id, limit=limit))
