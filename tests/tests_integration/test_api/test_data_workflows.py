from __future__ import annotations

import time

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import Function
from cognite.client.data_classes.workflows import (
    CDFTaskParameters,
    FunctionTaskParameters,
    SubworkflowTaskParameters,
    TransformationTaskParameters,
    Workflow,
    WorkflowDefinitionUpsert,
    WorkflowExecutionList,
    WorkflowList,
    WorkflowTask,
    WorkflowUpsert,
    WorkflowVersion,
    WorkflowVersionList,
    WorkflowVersionUpsert,
)
from cognite.client.exceptions import CogniteAPIError


@pytest.fixture
def workflow_list(cognite_client: CogniteClient) -> WorkflowList:
    workflow1 = WorkflowUpsert(
        external_id="integration_test-workflow1",
        description="This is  workflow for testing purposes",
    )
    workflow2 = WorkflowUpsert(
        external_id="integration_test-workflow2",
        description="This is  workflow for testing purposes",
    )
    workflows = [workflow1, workflow2]
    listed = cognite_client.workflows.list()
    existing = listed._external_id_to_item
    call_list = False
    for workflow in workflows:
        if workflow.external_id not in existing:
            call_list = True
            cognite_client.workflows.upsert(workflow)
    if call_list:
        return cognite_client.workflows.list()
    return listed


@pytest.fixture
def workflow_version_list(cognite_client: CogniteClient) -> WorkflowVersionList:
    workflow_id = "integration_test-workflow_with_versions"
    version1 = WorkflowVersionUpsert(
        workflow_external_id=workflow_id,
        version="1",
        workflow_definition=WorkflowDefinitionUpsert(
            tasks=[
                WorkflowTask(
                    external_id=f"{workflow_id}-1-task1",
                    parameters=TransformationTaskParameters(
                        external_id="None-existing-transformation",
                    ),
                )
            ],
            description=None,
        ),
    )
    version2 = WorkflowVersionUpsert(
        workflow_external_id=workflow_id,
        version="2",
        workflow_definition=WorkflowDefinitionUpsert(
            tasks=[
                WorkflowTask(
                    external_id="subworkflow1",
                    parameters=SubworkflowTaskParameters(
                        tasks=[
                            WorkflowTask(
                                external_id="s1-task1",
                                parameters=CDFTaskParameters(
                                    resource_path="/dummy/no/real/resource/path",
                                    method="GET",
                                    body={"limit": 1},
                                ),
                            ),
                            WorkflowTask(
                                external_id="s1-task2",
                                parameters=CDFTaskParameters(
                                    resource_path="/dummy/no/real/resource/path",
                                    method="GET",
                                    body={"limit": 1},
                                ),
                            ),
                        ]
                    ),
                ),
                WorkflowTask(
                    external_id="task1",
                    parameters=CDFTaskParameters(
                        resource_path="/dummy/no/real/resource/path",
                        method="GET",
                        body={"limit": 1},
                    ),
                    depends_on=["subworkflow1"],
                ),
            ],
            description=None,
        ),
    )
    listed = cognite_client.workflows.versions.list(workflow_version_ids=workflow_id)
    existing = {w.version for w in listed}
    call_list = False
    for version in [version1, version2]:
        if version.version not in existing:
            call_list = True
            cognite_client.workflows.versions.upsert(version)
    if call_list:
        return cognite_client.workflows.versions.list(workflow_version_ids=workflow_id)
    return listed


@pytest.fixture(scope="session")
def cdf_function_add(cognite_client: CogniteClient) -> Function:
    external_id = "integration_test-workflow-cdf_function_add"
    add_function = cognite_client.functions.retrieve(external_id=external_id)
    if add_function is not None:
        return add_function

    def handle(client, data: dict):
        output = data.copy()
        output["sum"] = output["a"] + output["b"]
        return output

    cognite_client.functions.create(name="Add", external_id=external_id, function_handle=handle)
    pytest.skip("Function need to be redeployed, skipping tests that need it", allow_module_level=True)


@pytest.fixture(scope="session")
def cdf_function_multiply(cognite_client: CogniteClient) -> Function:
    external_id = "integration_test-workflow-cdf_function_multiply"
    multiply_function = cognite_client.functions.retrieve(external_id=external_id)
    if multiply_function is not None:
        return multiply_function

    def handle(client, data: dict):
        output = data.copy()
        output["product"] = output["a"] * output["b"]
        return output

    cognite_client.functions.create(name="Multiply", external_id=external_id, function_handle=handle)
    pytest.skip(
        "Function need to be redeployed, skipping tests that need it",
        allow_module_level=True,
    )


@pytest.fixture(scope="session")
def add_multiply_workflow(
    cognite_client: CogniteClient, cdf_function_add: Function, cdf_function_multiply: Function
) -> WorkflowVersion:
    workflow_id = "integration_test-workflow-add_multiply"
    version = WorkflowVersionUpsert(
        workflow_external_id=workflow_id,
        version="1",
        workflow_definition=WorkflowDefinitionUpsert(
            description=None,
            tasks=[
                WorkflowTask(
                    external_id=f"{workflow_id}-1-add",
                    parameters=FunctionTaskParameters(
                        external_id=cdf_function_add.external_id,
                        data={"a": 1, "b": 2},
                    ),
                ),
                WorkflowTask(
                    external_id=f"{workflow_id}-1-multiply",
                    parameters=FunctionTaskParameters(
                        external_id=cdf_function_multiply.external_id,
                        data={"a": 3, "b": 4},
                        is_async_complete=True,
                    ),
                    timeout=120,
                    retries=2,
                ),
            ],
        ),
    )

    retrieved = cognite_client.workflows.versions.retrieve(version.workflow_external_id, version.version)
    if retrieved is not None:
        return retrieved
    else:
        return cognite_client.workflows.versions.upsert(version)


@pytest.fixture(scope="session")
def workflow_execution_list(
    cognite_client: CogniteClient, add_multiply_workflow: WorkflowVersion
) -> WorkflowExecutionList:
    executions = cognite_client.workflows.executions.list(workflow_version_ids=add_multiply_workflow.as_id(), limit=5)
    if executions:
        return executions
    # Creating at least one execution
    result = cognite_client.workflows.executions.trigger(
        add_multiply_workflow.workflow_external_id,
        add_multiply_workflow.version,
        {"a": 5, "b": 6},
    )
    t0 = time.time()
    while result.status != "completed":
        result = cognite_client.workflows.executions.retrieve_detailed(result.id)
        if result.status != "running":
            break
        try:
            cognite_client.workflows.tasks.update(result.executed_tasks[1].id, "completed")
        except CogniteAPIError as e:
            if e.message == f"Task with id {result.executed_tasks[1].id} is already in a terminal state":
                break
        time.sleep(0.5)
        if time.time() - t0 > 60:
            raise TimeoutError("Workflow execution did not complete in time")
    return cognite_client.workflows.executions.list(workflow_version_ids=add_multiply_workflow.as_id(), limit=5)


@pytest.fixture()
def clean_created_sessions(cognite_client: CogniteClient) -> None:
    existing_active_sessions = cognite_client.iam.sessions.list(status="active", limit=-1)
    yield None
    current_sessions = cognite_client.iam.sessions.list(status="active", limit=-1)
    existing_ids = {session.id for session in existing_active_sessions}
    to_revoked = [session.id for session in current_sessions if session.id not in existing_ids]
    cognite_client.iam.sessions.revoke(to_revoked)


class TestWorkflows:
    def test_upsert_delete(self, cognite_client: CogniteClient) -> None:
        workflow = WorkflowUpsert(
            external_id="integration_test-test_create_delete",
            description="This is ephemeral workflow for testing purposes",
        )
        cognite_client.workflows.delete(workflow.external_id, ignore_unknown_ids=True)

        created_workflow: Workflow | None = None
        try:
            created_workflow = cognite_client.workflows.upsert(workflow)

            assert created_workflow.external_id == workflow.external_id
            assert created_workflow.description == workflow.description
            assert created_workflow.created_time is not None
        finally:
            if created_workflow is not None:
                cognite_client.workflows.delete(created_workflow.external_id)

    def test_delete_non_existing_raise(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(CogniteAPIError) as e:
            cognite_client.workflows.delete("integration_test-non_existing_workflow", ignore_unknown_ids=False)

        assert "workflows were not found" in str(e.value)

    def test_delete_non_existing(self, cognite_client: CogniteClient) -> None:
        cognite_client.workflows.delete("integration_test-non_existing_workflow", ignore_unknown_ids=True)

    def test_retrieve_workflow(self, cognite_client: CogniteClient, workflow_list: WorkflowList) -> None:
        retrieved = cognite_client.workflows.retrieve(workflow_list[0].external_id)

        assert retrieved == workflow_list[0]

    def test_retrieve_non_existing_workflow(self, cognite_client: CogniteClient) -> None:
        non_existing = cognite_client.workflows.retrieve("integration_test-non_existing_workflow")

        assert non_existing is None


class TestWorkflowVersions:
    def test_upsert_delete(self, cognite_client: CogniteClient) -> None:
        version = WorkflowVersionUpsert(
            workflow_external_id="integration_test-workflow_versions-test_create_delete",
            version="1",
            workflow_definition=WorkflowDefinitionUpsert(
                tasks=[
                    WorkflowTask(
                        external_id="integration_test-workflow_definitions-test_create_delete-subworkflow1",
                        parameters=SubworkflowTaskParameters(
                            tasks=[
                                WorkflowTask(
                                    external_id="integration_test-workflow_definitions-test_create_delete-task1",
                                    parameters=FunctionTaskParameters(
                                        external_id="integration_test-workflow_definitions-test_create_delete-task1-function",
                                        data={"a": 1, "b": 2},
                                    ),
                                )
                            ]
                        ),
                    )
                ],
                description="This is ephemeral workflow definition for testing purposes",
            ),
        )
        cognite_client.workflows.versions.delete(version.as_id(), ignore_unknown_ids=True)

        created_version: WorkflowVersion | None = None
        try:
            created_version = cognite_client.workflows.versions.upsert(version)

            assert created_version.workflow_external_id == version.workflow_external_id
            assert created_version.workflow_definition.description == version.workflow_definition.description
            assert isinstance(created_version.workflow_definition.hash_, str)
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

    def test_list_workflow_version_limit(
        self, cognite_client: CogniteClient, workflow_version_list: WorkflowVersionList
    ) -> None:
        listed = cognite_client.workflows.versions.list(limit=1)

        assert len(listed) == 1

    def test_delete_non_existing_raise(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(CogniteAPIError) as e:
            cognite_client.workflows.versions.delete(
                ("integration_test-non_existing_workflow_version", "1"),
                ignore_unknown_ids=False,
            )

        assert "not found" in str(e.value)

    def test_delete_non_existing(self, cognite_client: CogniteClient) -> None:
        cognite_client.workflows.versions.delete(
            ("integration_test-non_existing_workflow_version", "1"),
            ignore_unknown_ids=True,
        )

    def test_retrieve_workflow(self, cognite_client: CogniteClient, workflow_version_list: WorkflowVersionList) -> None:
        retrieve_id = workflow_version_list[0].as_id()

        retrieved = cognite_client.workflows.versions.retrieve(*retrieve_id.as_primitive())

        assert retrieved == workflow_version_list[0]

    def test_retrieve_non_existing_workflow(self, cognite_client: CogniteClient) -> None:
        non_existing = cognite_client.workflows.versions.retrieve("integration_test-non_existing_workflow", "1")

        assert non_existing is None


class TestWorkflowExecutions:
    def test_list_workflow_executions(
        self,
        cognite_client: CogniteClient,
        workflow_execution_list: WorkflowExecutionList,
    ) -> None:
        workflow_ids = set(w.as_workflow_id() for w in workflow_execution_list)

        assert workflow_ids, "There should be at least one workflow execution to test list with"
        listed = cognite_client.workflows.executions.list(
            workflow_version_ids=list(workflow_ids), limit=len(workflow_execution_list)
        )

        assert len(listed) == len(workflow_execution_list)
        assert all(w.as_workflow_id() in workflow_ids for w in listed)

    def test_retrieve_workflow_execution_detailed(
        self,
        cognite_client: CogniteClient,
        workflow_execution_list: WorkflowExecutionList,
    ) -> None:
        assert workflow_execution_list, "There should be at least one workflow execution to test retrieve detailed with"
        retrieved = cognite_client.workflows.executions.retrieve_detailed(workflow_execution_list[0].id)

        assert retrieved.as_execution().dump() == workflow_execution_list[0].dump()
        assert retrieved.executed_tasks

    def test_retrieve_non_existing_workflow_execution(self, cognite_client: CogniteClient) -> None:
        non_existing = cognite_client.workflows.executions.retrieve_detailed(
            "integration_test-non_existing_workflow_execution"
        )

        assert non_existing is None

    # Each trigger creates a new execution, so we need to clean up after each test to avoid
    # running out of quota
    @pytest.mark.usefixtures("clean_created_sessions")
    def test_trigger_retrieve_detailed_update_update_task(
        self,
        cognite_client: CogniteClient,
        add_multiply_workflow: WorkflowVersion,
    ) -> None:
        workflow_execution = cognite_client.workflows.executions.trigger(
            add_multiply_workflow.workflow_external_id,
            add_multiply_workflow.version,
        )

        async_task = add_multiply_workflow.workflow_definition.tasks[1]
        assert isinstance(async_task.parameters, FunctionTaskParameters)
        assert async_task.parameters.is_async_complete

        workflow_execution_detailed = cognite_client.workflows.executions.retrieve_detailed(workflow_execution.id)
        async_task = workflow_execution_detailed.executed_tasks[1]

        async_task = cognite_client.workflows.tasks.update(async_task.id, "completed")
        assert async_task.status == "completed"

    @pytest.mark.usefixtures("clean_created_sessions")
    def test_trigger_cancel_retry_workflow(
        self, cognite_client: CogniteClient, add_multiply_workflow: WorkflowVersion
    ) -> None:
        workflow_execution = cognite_client.workflows.executions.trigger(
            add_multiply_workflow.workflow_external_id,
            add_multiply_workflow.version,
        )

        cancelled_workflow_execution = cognite_client.workflows.executions.cancel(
            id=workflow_execution.id, reason="test"
        )
        assert cancelled_workflow_execution.status == "terminated"
        assert cancelled_workflow_execution.reason_for_incompletion == "test"

        retried_workflow_execution = cognite_client.workflows.executions.retry(workflow_execution.id)
        assert retried_workflow_execution.status == "running"
