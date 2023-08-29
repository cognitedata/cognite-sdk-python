from __future__ import annotations

from cognite.client import CogniteClient
from cognite.client.data_classes.workflows import Workflow, WorkflowCreate


class TestWorkflows:
    def test_create_delete(self, cognite_client: CogniteClient) -> None:
        workflow = WorkflowCreate(
            external_id="integration_test:test_create_delete",
            description="This is ephemeral workflow for testing purposes",
        )

        created_workflow: Workflow | None = None
        try:
            created_workflow = cognite_client.workflows.create(workflow)

            assert created_workflow.external_id == workflow.external_id
            assert created_workflow.description == workflow.description
        finally:
            if created_workflow is not None:
                cognite_client.workflows.delete(created_workflow.external_id)
