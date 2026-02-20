"""
===============================================================================
726fd1c5f651769c35b0572f58c41192
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.workflows import (
    WorkflowTrigger,
    WorkflowTriggerList,
    WorkflowTriggerRunList,
    WorkflowTriggerUpsert,
)
from cognite.client.utils._async_helpers import run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
from cognite.client.data_classes import ClientCredentials


class SyncWorkflowTriggerAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def upsert(
        self, workflow_trigger: WorkflowTriggerUpsert, client_credentials: ClientCredentials | dict | None = None
    ) -> WorkflowTrigger:
        """
        `Create or update a trigger for a workflow. <https://api-docs.cognite.com/20230101/tag/Workflow-triggers/operation/CreateOrUpdateTriggers>`_

        Args:
            workflow_trigger: The workflow trigger specification.
            client_credentials: Specific credentials that should be used to trigger the workflow execution. When passed will take precedence over the current credentials.

        Returns:
            The created or updated workflow trigger specification.

        Examples:

            Create or update a scheduled trigger for a workflow:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.workflows import WorkflowTriggerUpsert, WorkflowScheduledTriggerRule
                >>> from zoneinfo import ZoneInfo
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
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
        return run_sync(
            self.__async_client.workflows.triggers.upsert(
                workflow_trigger=workflow_trigger, client_credentials=client_credentials
            )
        )

    def delete(self, external_id: str | SequenceNotStr[str]) -> None:
        """
        `Delete one or more triggers for a workflow. <https://api-docs.cognite.com/20230101/tag/Workflow-triggers/operation/deleteTriggers>`_

        Args:
            external_id: The external id(s) of the trigger(s) to delete.

        Examples:

            Delete a trigger with external id 'my_trigger':

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.workflows.triggers.delete("my_trigger")

            Delete a list of triggers:

                >>> client.workflows.triggers.delete(["my_trigger", "another_trigger"])
        """
        return run_sync(self.__async_client.workflows.triggers.delete(external_id=external_id))

    def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> WorkflowTriggerList:
        """
        `List the workflow triggers. <https://api-docs.cognite.com/20230101/tag/Workflow-triggers/operation/getTriggers>`_

        Args:
            limit: Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            The list of triggers.

        Examples:

            List all triggers:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.workflows.triggers.list(limit=None)
        """
        return run_sync(self.__async_client.workflows.triggers.list(limit=limit))

    def list_runs(self, external_id: str, limit: int | None = DEFAULT_LIMIT_READ) -> WorkflowTriggerRunList:
        """
        `List the history of runs for a trigger. <https://api-docs.cognite.com/20230101/tag/Workflow-triggers/operation/getTriggerHistory>`_

        Args:
            external_id: The external id of the trigger to list runs for.
            limit: Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            The requested trigger runs.

        Examples:

            Get all runs for a trigger with external id 'my_trigger':

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.workflows.triggers.list_runs("my_trigger", limit=None)
        """
        return run_sync(self.__async_client.workflows.triggers.list_runs(external_id=external_id, limit=limit))

    def pause(self, external_id: str) -> None:
        """
        `Pause a workflow trigger. <https://api-docs.cognite.com/20230101/tag/Workflow-triggers/operation/pauseTrigger>`_

        When a trigger is paused, it will not trigger new workflow executions.
        This operation is idempotent - pausing an already paused trigger has no effect.

        Args:
            external_id: The external id of the trigger to pause.

        Examples:

            Pause a trigger:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.workflows.triggers.pause("my_trigger")
        """
        return run_sync(self.__async_client.workflows.triggers.pause(external_id=external_id))

    def resume(self, external_id: str) -> None:
        """
        `Resume a paused workflow trigger. <https://api-docs.cognite.com/20230101/tag/Workflow-triggers/operation/resumeTrigger>`_

        When a trigger is resumed, it will start triggering workflow executions again according to its trigger rule.
        This operation is idempotent - resuming an already active trigger has no effect.

        Args:
            external_id: The external id of the trigger to resume.

        Examples:

            Resume a trigger:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.workflows.triggers.resume("my_trigger")
        """
        return run_sync(self.__async_client.workflows.triggers.resume(external_id=external_id))
