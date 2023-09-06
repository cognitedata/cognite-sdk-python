from __future__ import annotations

import time

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import Function
from cognite.client.data_classes.workflows import (
    CDFRequestParameters,
    FunctionParameters,
    Task,
    TransformationParameters,
    Workflow,
    WorkflowCreate,
    WorkflowExecutionList,
    WorkflowList,
    WorkflowVersion,
    WorkflowVersionCreate,
    WorkflowVersionList,
)
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


@pytest.fixture
def workflow_version_list(cognite_client: CogniteClient) -> WorkflowVersionList:
    workflow_id = "integration_test:workflow_with_versions"
    version1 = WorkflowVersionCreate(
        workflow_external_id=workflow_id,
        version="1",
        tasks=[
            Task(
                external_id=f"{workflow_id}:1:task1",
                parameters=TransformationParameters(
                    external_id="None-existing-transformation",
                ),
            )
        ],
    )
    version2 = WorkflowVersionCreate(
        workflow_external_id=workflow_id,
        version="2",
        tasks=[
            Task(
                external_id=f"{workflow_id}:2:task1",
                parameters=CDFRequestParameters(
                    resource_path="/dummy/no/real/resource/path",
                    method="GET",
                    body={"limit": 1},
                ),
            )
        ],
    )
    listed = cognite_client.workflows.versions.list(workflow_ids=workflow_id)
    existing = {w.version for w in listed}
    call_list = False
    for version in [version1, version2]:
        if version.version not in existing:
            call_list = True
            cognite_client.workflows.versions.create(version)
    if call_list:
        return cognite_client.workflows.versions.list(workflow_external_id=workflow_id)
    return listed


@pytest.fixture
def cdf_function_add(cognite_client: CogniteClient) -> Function:
    external_id = "integration_test:workflow:cdf_function_add"
    add_function = cognite_client.functions.retrieve(external_id=external_id)
    if add_function is not None:
        return add_function

    def handle(client, data: dict):
        output = data.copy()
        data["sum"] = data["a"] + data["b"]
        return output

    deployed = cognite_client.functions.create(name="Add", external_id=external_id, function_handle=handle)
    return deployed


@pytest.fixture
def cdf_function_multiply(cognite_client: CogniteClient) -> Function:
    external_id = "integration_test:workflow:cdf_function_multiply"
    multiply_function = cognite_client.functions.retrieve(external_id=external_id)
    if multiply_function is not None:
        return multiply_function

    def handle(client, data: dict):
        output = data.copy()
        data["product"] = data["a"] * data["b"]
        return output

    deployed = cognite_client.functions.create(name="Multiply", external_id=external_id, function_handle=handle)
    return deployed


@pytest.fixture
def add_multiply_workflow(
    cognite_client: CogniteClient, cdf_function_add: Function, cdf_function_multiply
) -> WorkflowVersion:
    workflow_id = "integration_test:workflow:add_multiply"
    version = WorkflowVersionCreate(
        workflow_external_id=workflow_id,
        version="1",
        tasks=[
            Task(
                external_id=f"{workflow_id}:1:add",
                parameters=FunctionParameters(
                    external_id=cdf_function_add.external_id,
                    data={"a": 1, "b": 2},
                ),
            ),
            Task(
                external_id=f"{workflow_id}:1:multiply",
                parameters=FunctionParameters(
                    external_id=cdf_function_multiply.external_id,
                    data={"a": 3, "b": 4},
                    is_async_complete=True,
                ),
                timeout=120,
                retries=2,
            ),
        ],
    )
    retrieved = cognite_client.workflows.versions.retrieve(workflow_id, version.version)
    if retrieved is not None:
        return retrieved
    return cognite_client.workflows.versions.create(version)


@pytest.fixture
def workflow_execution_list(
    cognite_client: CogniteClient, add_multiply_workflow: WorkflowVersion
) -> WorkflowExecutionList:
    executions = cognite_client.workflows.executions.list(workflow_external_id=add_multiply_workflow.as_id(), limit=5)
    if executions:
        return executions
    # Creating at least one execution
    result = cognite_client.workflows.executions.trigger(
        add_multiply_workflow.workflow_external_id, add_multiply_workflow.version, {"a": 5, "b": 6}
    )
    t0 = time.time()
    while result.status != "completed":
        result = cognite_client.workflows.executions.retrieve(result.external_id)
        time.sleep(5)
        if time.time() - t0 > 60:
            raise TimeoutError("Workflow execution did not complete in time")
    return cognite_client.workflows.executions.list(workflow_external_id=add_multiply_workflow.as_id(), limit=5)


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

    def test_retrieve_workflow(self, cognite_client: CogniteClient, workflow_list: WorkflowList) -> None:
        retrieved = cognite_client.workflows.retrieve(workflow_list[0].external_id)

        assert retrieved == workflow_list[0]

    def test_retrieve_non_existing_workflow(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(CogniteAPIError) as e:
            cognite_client.workflows.retrieve("integration_test:non_existing_workflow")

        assert "Workflow not found" in str(e.value)


class TestWorkflowVersions:
    def test_create_delete(self, cognite_client: CogniteClient) -> None:
        version = WorkflowVersionCreate(
            workflow_external_id="integration_test:workflow_versions:test_create_delete",
            version="1",
            tasks=[
                Task(
                    external_id="integration_test:workflow_definitions:test_create_delete:task1",
                    parameters=FunctionParameters(
                        external_id="integration_test:workflow_definitions:test_create_delete:task1:function",
                        data={"a": 1, "b": 2},
                    ),
                )
            ],
            description="This is ephemeral workflow definition for testing purposes",
        )
        cognite_client.workflows.versions.delete(version.as_id(), ignore_unknown_ids=True)

        created_version: WorkflowVersion | None = None
        try:
            created_version = cognite_client.workflows.versions.create(version)

            assert created_version.workflow_external_id == version.workflow_external_id
            assert created_version.description == version.description
            assert created_version.hash is not None
        finally:
            if created_version is not None:
                cognite_client.workflows.versions.delete(
                    created_version.as_id(),
                )
                cognite_client.workflows.delete(created_version.workflow_external_id)

    def test_list_workflow_versions(
        self, cognite_client: CogniteClient, workflow_version_list: WorkflowVersionList
    ) -> None:
        listed = cognite_client.workflows.versions.list(workflow_version_list.as_ids())

        assert len(listed) == len(workflow_version_list)
        assert listed == workflow_version_list

    def test_delete_non_existing_raise(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(CogniteAPIError) as e:
            cognite_client.workflows.versions.delete(
                ("integration_test:non_existing_workflow_version", "1"), ignore_unknown_ids=False
            )

        assert "not found" in str(e.value)

    def test_delete_non_existing(self, cognite_client: CogniteClient) -> None:
        cognite_client.workflows.versions.delete(
            ("integration_test:non_existing_workflow_version", "1"), ignore_unknown_ids=True
        )

    def test_retrieve_workflow(self, cognite_client: CogniteClient, workflow_version_list: WorkflowVersionList) -> None:
        retrieve_id = workflow_version_list[0].as_id()

        retrieved = cognite_client.workflows.versions.retrieve(*retrieve_id.as_primitive())

        assert retrieved == workflow_version_list[0]

    def test_retrieve_non_existing_workflow(self, cognite_client: CogniteClient) -> None:
        non_existing = cognite_client.workflows.versions.retrieve("integration_test:non_existing_workflow", "1")

        assert non_existing is None


class TestWorkflowExecutions:
    def test_list_workflow_executions(
        self, cognite_client: CogniteClient, workflow_execution_list: WorkflowExecutionList
    ) -> None:
        workflow_ids = set(w.as_workflow_id() for w in workflow_execution_list)

        listed = cognite_client.workflows.executions.list(
            workflow_external_id=list(workflow_ids), limit=len(workflow_ids)
        )

        assert len(listed) == len(workflow_execution_list)
        assert all(w.as_workflow_id() in workflow_ids for w in listed)

    def test_retrieve_workflow_execution(
        self, cognite_client: CogniteClient, workflow_execution_list: WorkflowExecutionList
    ) -> None:
        retrieved = cognite_client.workflows.executions.retrieve(workflow_execution_list[0].external_id)

        assert retrieved == workflow_execution_list[0]

    def test_retrieve_non_existing_workflow_execution(self, cognite_client: CogniteClient) -> None:
        non_existing = cognite_client.workflows.executions.retrieve_detailed(
            "integration_test:non_existing_workflow_execution"
        )

        assert non_existing is None

    def test_trigger_workflow_execution_update_task(
        self, cognite_client: CogniteClient, add_multiply_workflow: WorkflowVersion
    ) -> None:
        task_execution = cognite_client.workflows.executions.trigger(
            add_multiply_workflow.workflow_external_id, add_multiply_workflow.version, {"a": 41, "b": 1}
        )

        assert task_execution.status

        async_task = add_multiply_workflow.tasks[1]
        assert isinstance(async_task.parameters, FunctionParameters)
        assert async_task.parameters.is_async_complete
        task_execution = cognite_client.workflows.tasks.update(async_task.external_id, "completed")
        time.sleep(1)
        assert task_execution.status == "completed"
