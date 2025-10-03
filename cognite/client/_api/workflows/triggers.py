from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.workflows import (
    WorkflowTrigger,
    WorkflowTriggerCreate,
    WorkflowTriggerList,
    WorkflowTriggerRun,
    WorkflowTriggerRunList,
    WorkflowTriggerUpsert,
)
from cognite.client.utils._auxiliary import interpolate_and_url_encode
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._session import create_session_and_return_nonce
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient
    from cognite.client.data_classes import ClientCredentials


class WorkflowTriggerAPI(APIClient):
    _RESOURCE_PATH = "/workflows/triggers"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._DELETE_LIMIT = 1

    def upsert(
        self,
        workflow_trigger: WorkflowTriggerUpsert,
        client_credentials: ClientCredentials | dict | None = None,
    ) -> WorkflowTrigger:
        """`Create or update a trigger for a workflow. <https://api-docs.cognite.com/20230101/tag/Workflow-triggers/operation/CreateOrUpdateTriggers>`_

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
        nonce = create_session_and_return_nonce(
            self._cognite_client, api_name="Workflow API", client_credentials=client_credentials
        )
        dumped = workflow_trigger.dump(camel_case=True)
        dumped["authentication"] = {"nonce": nonce}
        response = self._post(
            url_path=self._RESOURCE_PATH,
            json={"items": [dumped]},
        )
        return WorkflowTrigger._load(response.json().get("items")[0])

    # TODO: remove method and associated data classes in next major release
    def create(
        self,
        workflow_trigger: WorkflowTriggerCreate,
        client_credentials: ClientCredentials | dict | None = None,
    ) -> WorkflowTrigger:
        """Create or update a trigger for a workflow.

        .. admonition:: Deprecation Warning

            This method is deprecated, use '.upsert' instead. It will be removed in the next major version.
        """
        warnings.warn(
            "This method is deprecated, use '.upsert' instead. It will be removed in the next major release.",
            UserWarning,
        )
        return self.upsert(workflow_trigger, client_credentials)

    def delete(self, external_id: str | SequenceNotStr[str]) -> None:
        """`Delete one or more triggers for a workflow. <https://api-docs.cognite.com/20230101/tag/Workflow-triggers/operation/deleteTriggers>`_

        Args:
            external_id (str | SequenceNotStr[str]): The external id(s) of the trigger(s) to delete.

        Examples:

            Delete a trigger with external id 'my_trigger':

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.workflows.triggers.delete("my_trigger")

            Delete a list of triggers:

                >>> client.workflows.triggers.delete(["my_trigger", "another_trigger"])
        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            wrap_ids=True,
        )

    def get_triggers(self, limit: int | None = DEFAULT_LIMIT_READ) -> WorkflowTriggerList:
        """List the workflow triggers.

        .. admonition:: Deprecation Warning

            This method is deprecated, use '.list' instead. It will be removed in the next major version.
        """
        warnings.warn(
            "The 'get_triggers' method is deprecated, use 'list' instead. It will be removed in the next major release.",
            UserWarning,
        )
        return self.list(limit)

    def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> WorkflowTriggerList:
        """`List the workflow triggers. <https://api-docs.cognite.com/20230101/tag/Workflow-triggers/operation/getTriggers>`_

        Args:
            limit (int | None): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            WorkflowTriggerList: The list of triggers.

        Examples:

            List all triggers:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.triggers.list(limit=None)
        """
        return self._list(
            method="GET",
            url_path=self._RESOURCE_PATH,
            resource_cls=WorkflowTrigger,
            list_cls=WorkflowTriggerList,
            limit=limit,
        )

    def get_trigger_run_history(
        self, external_id: str, limit: int | None = DEFAULT_LIMIT_READ
    ) -> WorkflowTriggerRunList:
        """List the history of runs for a trigger.

        .. admonition:: Deprecation Warning

            This method is deprecated, use '.list_runs' instead. It will be removed in the next major version.
        """
        warnings.warn(
            "The 'get_trigger_run_history' method is deprecated, use 'list_runs' instead. It will be removed in the next major release.",
            UserWarning,
        )
        return self.list_runs(external_id, limit)

    def list_runs(self, external_id: str, limit: int | None = DEFAULT_LIMIT_READ) -> WorkflowTriggerRunList:
        """`List the history of runs for a trigger. <https://api-docs.cognite.com/20230101/tag/Workflow-triggers/operation/getTriggerHistory>`_

        Args:
            external_id (str): The external id of the trigger to list runs for.
            limit (int | None): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            WorkflowTriggerRunList: The requested trigger runs.

        Examples:

            Get all runs for a trigger with external id 'my_trigger':

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.triggers.list_runs("my_trigger", limit=None)
        """
        return self._list(
            method="GET",
            url_path=interpolate_and_url_encode(self._RESOURCE_PATH + "/{}/history", external_id),
            resource_cls=WorkflowTriggerRun,
            list_cls=WorkflowTriggerRunList,
            limit=limit,
        )
