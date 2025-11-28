from __future__ import annotations

import time
import unittest
from collections.abc import Iterator
from zoneinfo import ZoneInfo

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import DataSet
from cognite.client.data_classes.data_modeling import ViewId
from cognite.client.data_classes.data_modeling.query import NodeResultSetExpression, Select, SourceSelector
from cognite.client.data_classes.simulators.runs import SimulationInputOverride, SimulationValueUnitName
from cognite.client.data_classes.workflows import (
    CDFTaskParameters,
    FunctionTaskParameters,
    SimulationTaskOutput,
    SimulationTaskParameters,
    SubworkflowReferenceParameters,
    SubworkflowTaskParameters,
    TransformationTaskParameters,
    Workflow,
    WorkflowDataModelingTriggerRule,
    WorkflowDefinitionUpsert,
    WorkflowExecution,
    WorkflowExecutionDetailed,
    WorkflowExecutionList,
    WorkflowList,
    WorkflowScheduledTriggerRule,
    WorkflowTask,
    WorkflowTrigger,
    WorkflowTriggerDataModelingQuery,
    WorkflowTriggerUpsert,
    WorkflowUpsert,
    WorkflowVersion,
    WorkflowVersionId,
    WorkflowVersionList,
    WorkflowVersionUpsert,
)
from cognite.client.exceptions import CogniteAPIError, CogniteAuthError, CogniteNotFoundError

COGNITE_CLIENT = pytest.fixture(scope="session")(lambda: CogniteClient())


def pytest_generate_tests(metafunc):
    if metafunc.cls is not None and hasattr(metafunc.cls, "parameterize_exceptions"):
        for fixture in metafunc.fixturenames:
            if fixture in metafunc.cls.parameterize_exceptions:
                metafunc.parametrize(fixture, metafunc.cls.parameterize_exceptions[fixture], indirect=True)


@pytest.fixture(scope="session")
def cognite_client() -> CogniteClient:
    return COGNITE_CLIENT()


@pytest.fixture(scope="session")
def permanent_workflow(cognite_client) -> Workflow:
    workflows = cognite_client.workflows.list(limit=1)
    assert len(workflows) > 0
    return workflows[0]


@pytest.fixture(scope="session")
def permanent_workflow_version(cognite_client, permanent_workflow) -> WorkflowVersion:
    versions = cognite_client.workflows.versions.list(permanent_workflow.external_id, limit=1)
    assert len(versions) > 0
    return versions[0]


@pytest.fixture(scope="session")
def permanent_workflow_execution(cognite_client) -> WorkflowExecutionDetailed:
    executions = cognite_client.workflows.executions.list(limit=1)
    assert len(executions) > 0
    # Need to use retrieve_detailed to get the execution with tasks
    return cognite_client.workflows.executions.retrieve_detailed(executions[0].id)


@pytest.fixture(scope="session")
def permanent_scheduled_trigger(cognite_client) -> WorkflowTrigger:
    triggers = cognite_client.workflows.triggers.list(
        filter_={"trigger_type": "scheduled"},
        limit=1,
    )
    assert len(triggers) > 0
    return triggers[0]


@pytest.fixture(scope="session")
def dt_workflow_trigger(cognite_client) -> WorkflowTrigger:
    triggers = cognite_client.workflows.triggers.list(
        filter_={"trigger_type": "data_modeling"},
        limit=1,
    )
    if not triggers:
        pytest.skip("No data modeling trigger found")
    return triggers[0]


class TestWorkflows:
    def test_list_workflows(self, cognite_client: CogniteClient) -> None:
        workflows = cognite_client.workflows.list()
        assert len(workflows) >= 0

    def test_list_workflows_with_filter(self, cognite_client: CogniteClient) -> None:
        workflows = cognite_client.workflows.list(filter_={"states": ["deleted"]})
        assert len(workflows) >= 0

    @pytest.mark.timeout(20)
    def test_create_workflow_object_and_run(self, cognite_client: CogniteClient) -> None:
        external_id = f"integration_test-workflow-{int(time.time())}"
        workflow_def = WorkflowDefinitionUpsert(
            description="Integration test workflow",
            tasks=[
                WorkflowTask(
                    external_id="myTask",
                    parameters=CDFTaskParameters(
                        resource_path="/assets",
                        method="GET",
                        path_parameters={"id": {"bind": ["data", "id"]}},
                        headers={"Content-Type": "application/json"},
                    ),
                )
            ],
        )
        workflow = WorkflowUpsert(external_id=external_id, definition=workflow_def)
        try:
            created_workflow = cognite_client.workflows.upsert(workflow)
            assert created_workflow.external_id == external_id

            execution = cognite_client.workflows.executions.run(external_id, input_={"data": {"id": "123"}})
            assert execution.workflow_external_id == external_id

        finally:
            try:
                cognite_client.workflows.delete(external_id)
            except Exception:
                pass  # ignore cleanup failure

    def test_retrieve_workflow(self, cognite_client: CogniteClient, permanent_workflow) -> None:
        workflow = cognite_client.workflows.retrieve(permanent_workflow.external_id)
        assert workflow.external_id == permanent_workflow.external_id

    def test_create_workflow_definition_with_similar_task_parameters(
        self, cognite_client: CogniteClient, data_set: DataSet
    ) -> None:
        external_id = f"integration_test-workflow-definition-{int(time.time())}"

        # Two function tasks that have identical parameters should both work
        workflow_def = WorkflowDefinitionUpsert(
            description="Integration test function parameter workflow",
            tasks=[
                WorkflowTask(
                    external_id="myFirstTask",
                                parameters=FunctionTaskParameters(
                        external_id="function-external-id",
                        data={"foo": "hello"},
                        data_set_id=data_set.id,
                    ),
                ),
                WorkflowTask(
                    external_id="mySecondTask",
                    parameters=FunctionTaskParameters(
                        external_id="function-external-id",
                        data={"foo": "world"},
                        data_set_id=data_set.id,
                    ),
                ),
            ],
        )
        workflow = WorkflowUpsert(external_id=external_id, definition=workflow_def)
        try:
            created_workflow = cognite_client.workflows.upsert(workflow)
            assert created_workflow.external_id == external_id

            definition = cognite_client.workflows.definitions.retrieve(external_id)
            assert definition is not None

        finally:
            try:
                cognite_client.workflows.delete(external_id)
            except Exception:
                pass  # ignore cleanup failure

    def test_retrieve_workflow_version(
        self, cognite_client: CogniteClient, permanent_workflow_version: WorkflowVersion
    ) -> None:
        version = cognite_client.workflows.versions.retrieve(
            permanent_workflow_version.workflow_external_id, permanent_workflow_version.version
        )
        assert version.version == permanent_workflow_version.version

    def test_list_workflow_versions(self, cognite_client: CogniteClient, permanent_workflow: Workflow) -> None:
        versions = cognite_client.workflows.versions.list(permanent_workflow.external_id)
        assert len(versions) >= 1

    def test_list_and_retrieve_execution(
        self, cognite_client: CogniteClient, permanent_workflow_execution: WorkflowExecutionDetailed
    ) -> None:
        executions = cognite_client.workflows.executions.list(
            filter_={"workflow_external_ids": [permanent_workflow_execution.workflow_external_id]},
            limit=1,
        )
        assert len(executions) >= 1
        execution = cognite_client.workflows.executions.retrieve(executions[0].id)
        assert execution.id == executions[0].id

    def test_retrieve_execution_detailed(
        self, cognite_client: CogniteClient, permanent_workflow_execution: WorkflowExecutionDetailed
    ) -> None:
        execution = cognite_client.workflows.executions.retrieve_detailed(permanent_workflow_execution.id)
        assert execution.id == permanent_workflow_execution.id
        assert len(execution.tasks) >= 1


class TestWorkflowTriggers:
    def test_list_triggers(self, cognite_client: CogniteClient) -> None:
        triggers = cognite_client.workflows.triggers.list()
        assert len(triggers) >= 0

    def test_list_triggers_with_filter(self, cognite_client: CogniteClient) -> None:
        triggers = cognite_client.workflows.triggers.list(filter_={"trigger_type": "scheduled"})
        assert len(triggers) >= 0

    def test_create_delete_trigger(self, cognite_client: CogniteClient, permanent_workflow: Workflow) -> None:
        trigger_external_id = f"integration_test-trigger-{int(time.time())}"
        trigger_upsert = WorkflowTriggerUpsert(
            external_id=trigger_external_id,
            workflow_external_id=permanent_workflow.external_id,
            workflow_version=permanent_workflow.version,
            trigger_rule=WorkflowScheduledTriggerRule(cron_expression="0 0 * * *"),
        )

        try:
            created_trigger = cognite_client.workflows.triggers.upsert(trigger_upsert)
            assert created_trigger.external_id == trigger_external_id

            retrieved_trigger = cognite_client.workflows.triggers.retrieve(trigger_external_id)
            assert retrieved_trigger.external_id == trigger_external_id

        finally:
            try:
                cognite_client.workflows.triggers.delete(trigger_external_id)
            except Exception:
                pass  # ignore cleanup failure

    def test_create_data_modeling_trigger(self, cognite_client: CogniteClient, permanent_workflow: Workflow) -> None:
        external_id = f"integration_test-dm-trigger-{int(time.time())}"
        trigger_upsert = WorkflowTriggerUpsert(
            external_id=external_id,
            workflow_external_id=permanent_workflow.external_id,
            workflow_version=permanent_workflow.version,
                trigger_rule=WorkflowDataModelingTriggerRule(
                query=WorkflowTriggerDataModelingQuery(
                    with_=[
                        Select(
                            sources=[
                                SourceSelector(
                                    source=ViewId(space="cdf_core", external_id="CogniteAsset", version="v1"),
                                )
                            ]
                        ).select()
                    ],
                    select=NodeResultSetExpression(),
                ),
                check_interval=60,
            ),
        )
        try:
            created_trigger = cognite_client.workflows.triggers.upsert(trigger_upsert)
            assert created_trigger.external_id == external_id

        finally:
            try:
                cognite_client.workflows.triggers.delete(external_id)
            except Exception:
                pass  # ignore cleanup failure

    def test_create_scheduled_trigger(self, cognite_client: CogniteClient, permanent_workflow: Workflow) -> None:
        external_id = f"integration_test-scheduled-trigger-{int(time.time())}"
        trigger_upsert = WorkflowTriggerUpsert(
            external_id=external_id,
            workflow_external_id=permanent_workflow.external_id,
            workflow_version=permanent_workflow.version,
            trigger_rule=WorkflowScheduledTriggerRule(
                cron_expression="0 8 * * *",
                timezone=ZoneInfo("America/New_York"),
            ),
        )
        try:
            created_trigger = cognite_client.workflows.triggers.upsert(trigger_upsert)
            assert created_trigger.external_id == external_id

        finally:
            try:
                cognite_client.workflows.triggers.delete(external_id)
            except Exception:
                pass  # ignore cleanup failure

    def test_list_trigger_runs(self, cognite_client: CogniteClient, permanent_scheduled_trigger) -> None:
        runs = cognite_client.workflows.triggers.list_runs(
            permanent_scheduled_trigger.external_id,
            limit=1,
        )
        assert len(runs) >= 0

    def test_retrieve_trigger(self, cognite_client: CogniteClient, permanent_scheduled_trigger) -> None:
        trigger = cognite_client.workflows.triggers.retrieve(permanent_scheduled_trigger.external_id)
        assert trigger.external_id == permanent_scheduled_trigger.external_id

    def test_retrieve_nonexistent_trigger_should_fail(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(CogniteNotFoundError, match="not found"):
            cognite_client.workflows.triggers.retrieve("nonexistent_trigger")

    def test_list_runs_with_nonexistent_trigger_should_fail(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(CogniteAPIError, match=r"Workflow trigger not found\."):
            cognite_client.workflows.triggers.list_runs("nonexistent_trigger")

    def test_get_trigger_run_history_with_nonexistent_trigger_should_fail(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(CogniteAPIError, match=r"Workflow trigger not found\."):
            cognite_client.workflows.triggers.get_trigger_run_history(
                external_id="integration_test-non_existing_trigger"
            )

    def test_pause_resume_trigger(
        self,
        cognite_client: CogniteClient,
        permanent_workflow: Workflow,
    ) -> None:
        trigger_external_id = f"integration_test-pause_resume_trigger-{int(time.time())}"
        trigger_upsert = WorkflowTriggerUpsert(
            external_id=trigger_external_id,
            workflow_external_id=permanent_workflow.external_id,
            workflow_version=permanent_workflow.version,
            trigger_rule=WorkflowScheduledTriggerRule(cron_expression="0 0 * * *"),
        )

        try:
            created_trigger = cognite_client.workflows.triggers.upsert(trigger_upsert)
            assert created_trigger.external_id == trigger_external_id

            cognite_client.workflows.triggers.pause(trigger_external_id)

            trigger_after_pause = cognite_client.workflows.triggers.retrieve(trigger_external_id)
            assert trigger_after_pause is not None
            assert trigger_after_pause.is_paused is True

            cognite_client.workflows.triggers.pause(trigger_external_id)

            trigger_still_paused = cognite_client.workflows.triggers.retrieve(trigger_external_id)
            assert trigger_still_paused.is_paused is True

            cognite_client.workflows.triggers.resume(trigger_external_id)

            trigger_after_resume = cognite_client.workflows.triggers.retrieve(trigger_external_id)
            assert trigger_after_resume is not None
            assert trigger_after_resume.is_paused is False

            cognite_client.workflows.triggers.resume(trigger_external_id)

            trigger_still_resumed = cognite_client.workflows.triggers.retrieve(trigger_external_id)
            assert trigger_still_resumed.is_paused is False

        finally:
            try:
                cognite_client.workflows.triggers.delete(trigger_external_id)
            except Exception:
                pass

    def test_pause_resume_nonexistent_trigger(self, cognite_client: CogniteClient) -> None:
        # Test pause on non-existent trigger
        with pytest.raises(CogniteAPIError, match=r"Trigger not found\."):
            cognite_client.workflows.triggers.pause("integration_test-non_existing_trigger")

        # Test resume on non-existent trigger
        with pytest.raises(CogniteAPIError, match=r"Trigger not found\."):
            cognite_client.workflows.triggers.resume("integration_test-non_existing_trigger")