from __future__ import annotations

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.workflows import Workflow, WorkflowCreate, WorkflowList
from cognite.client.exceptions import CogniteAPIError


@pytest.fixture
def workflow_list(cognite_client: CogniteClient) -> WorkflowList:
    workflow1 = WorkflowCreate(
        external_id="integration_test:workflow1",
        description="This is  workflow for testing purposes",
    )
    workflow2 = WorkflowCreate(
        external_id="integration_test:workflow2",
        description="This is  workflow for testing purposes",
    )
    workflows = [workflow1, workflow2]
    listed = cognite_client.workflows.list()
    existing = {w.external_id for w in listed}
    call_list = False
    for workflow in workflows:
        if workflow.external_id not in existing:
            call_list = True
            cognite_client.workflows.create(workflow)
    if call_list:
        return cognite_client.workflows.list()
    return listed


class TestWorkflows:
    def test_create_delete(self, cognite_client: CogniteClient) -> None:
        workflow = WorkflowCreate(
            external_id="integration_test:test_create_delete",
            description="This is ephemeral workflow for testing purposes",
        )
        cognite_client.workflows.delete(workflow.external_id, ignore_unknown_ids=True)

        created_workflow: Workflow | None = None
        try:
            created_workflow = cognite_client.workflows.create(workflow)

            assert created_workflow.external_id == workflow.external_id
            assert created_workflow.description == workflow.description
            assert created_workflow.created_time is not None
        finally:
            if created_workflow is not None:
                cognite_client.workflows.delete(created_workflow.external_id)

    def test_delete_non_existing_raise(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(CogniteAPIError) as e:
            cognite_client.workflows.delete("integration_test:non_existing_workflow", ignore_unknown_ids=False)

        assert "workflows were not found" in str(e.value)

    def test_delete_non_existing(self, cognite_client: CogniteClient) -> None:
        cognite_client.workflows.delete("integration_test:non_existing_workflow", ignore_unknown_ids=True)

    def test_list_workflows(self, cognite_client: CogniteClient, workflow_list: WorkflowList) -> None:
        listed = cognite_client.workflows.list()

        assert len(listed) >= len(workflow_list)
        assert listed.get(external_id=workflow_list[0].external_id) == workflow_list[0]
        assert listed.get(external_id=workflow_list[1].external_id) == workflow_list[1]
