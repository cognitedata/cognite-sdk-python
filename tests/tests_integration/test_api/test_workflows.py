from __future__ import annotations

import time
import unittest
from datetime import datetime

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import DataSet
from cognite.client.data_classes.data_modeling import ViewId
from cognite.client.data_classes.data_modeling.query import NodeResultSetExpression, Select, SourceSelector
from cognite.client.data_classes.simulators import SimulationInputOverride, SimulationValueUnitName
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
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils import timestamp_to_ms
from cognite.client.utils._text import random_string
from tests.tests_integration.test_api.test_simulators.seed_sim_data import (
    ensure_workflow_simint_routine,
    finish_simulation_runs,
)


@pytest.fixture
def workflow_simint_routine(cognite_client: CogniteClient) -> str:
    ensure_workflow_simint_routine(cognite_client)


@pytest.fixture(autouse=True, scope="module")
def wf_setup_module(cognite_client: CogniteClient) -> None:
    """setup any state specific to the execution of the given module."""
    resource_age = timestamp_to_ms("30m-ago")

    wf_triggers = cognite_client.workflows.triggers.list(limit=None)
    wf_triggers_to_delete = [wf.external_id for wf in wf_triggers if wf.created_time < resource_age]
    if wf_triggers_to_delete:
        cognite_client.workflows.triggers.delete(wf_triggers_to_delete)

    wf_versions = cognite_client.workflows.versions.list(limit=None)
    wf_versions_to_delete = [
        (wf.workflow_external_id, wf.version) for wf in wf_versions if wf.created_time < resource_age
    ]
    if wf_versions_to_delete:
        cognite_client.workflows.versions.delete(wf_versions_to_delete)

    wfs = cognite_client.workflows.list(limit=None)
    wfs_to_delete = [wf.external_id for wf in wfs if wf.created_time < resource_age]
    if wfs_to_delete:
        cognite_client.workflows.delete(wfs_to_delete)


@pytest.fixture(scope="session")
def data_set(cognite_client: CogniteClient) -> DataSet:
    return cognite_client.data_sets.list(limit=1)[0]


@pytest.fixture
def new_workflow(cognite_client: CogniteClient, data_set: DataSet):
    workflow = WorkflowUpsert(
        external_id=f"integration_test-workflow_{random_string(5)}",
        data_set_id=data_set.id,
    )
    yield cognite_client.workflows.upsert(workflow)
    cognite_client.workflows.delete(workflow.external_id, ignore_unknown_ids=True)
    assert cognite_client.workflows.retrieve(workflow.external_id) is None


@pytest.fixture(scope="class")
def workflow_list(cognite_client: CogniteClient, data_set: DataSet):
    workflow_1 = WorkflowUpsert(
        external_id=f"integration_test-workflow_1_{random_string(5)}",
        description="This workflow is for testing purposes",
        data_set_id=data_set.id,
    )
    workflow_2 = WorkflowUpsert(
        external_id=f"integration_test-workflow_2_{random_string(5)}",
        description="This workflow is for testing purposes",
    )
    for workflow in [workflow_1, workflow_2]:
        cognite_client.workflows.upsert(workflow)
    yield cognite_client.workflows.list()
    cognite_client.workflows.delete([workflow_1.external_id, workflow_2.external_id], ignore_unknown_ids=True)
    assert cognite_client.workflows.retrieve(workflow_1.external_id) is None
    assert cognite_client.workflows.retrieve(workflow_2.external_id) is None


@pytest.fixture
def new_workflow_version(cognite_client: CogniteClient, new_workflow: Workflow):
    version = WorkflowVersionUpsert(
        workflow_external_id=new_workflow.external_id,
        version="1",
        workflow_definition=WorkflowDefinitionUpsert(
            tasks=[
                WorkflowTask(
                    external_id=f"{new_workflow.external_id}-1-task1",
                    parameters=CDFTaskParameters(
                        resource_path="/timeseries",
                        method="GET",
                    ),
                ),
            ],
        ),
    )
    yield cognite_client.workflows.versions.upsert(version)
    cognite_client.workflows.versions.delete((new_workflow.external_id, version.version), ignore_unknown_ids=True)
    assert cognite_client.workflows.versions.retrieve(new_workflow.external_id, version.version) is None


@pytest.fixture
def async_workflow_version(cognite_client: CogniteClient, new_workflow: Workflow):
    version = WorkflowVersionUpsert(
        workflow_external_id=new_workflow.external_id,
        version="1",
        workflow_definition=WorkflowDefinitionUpsert(
            tasks=[
                WorkflowTask(
                    external_id=f"{new_workflow.external_id}-1-multiply",
                    parameters=FunctionTaskParameters(
                        external_id="non-existing-function-async-resolve",
                        data={"a": 3, "b": 4},
                        is_async_complete=True,
                    ),
                    timeout=120,
                    retries=2,
                ),
            ],
        ),
    )
    yield cognite_client.workflows.versions.upsert(version)
    cognite_client.workflows.versions.delete((new_workflow.external_id, version.version), ignore_unknown_ids=True)
    assert cognite_client.workflows.versions.retrieve(new_workflow.external_id, version.version) is None


@pytest.fixture
def workflow_version_list(cognite_client: CogniteClient, new_workflow: Workflow):
    version_1 = WorkflowVersionUpsert(
        workflow_external_id=new_workflow.external_id,
        version="1",
        workflow_definition=WorkflowDefinitionUpsert(
            tasks=[
                WorkflowTask(
                    external_id=f"{new_workflow.external_id}-1-task1",
                    parameters=CDFTaskParameters(
                        resource_path="/timeseries",
                        method="GET",
                    ),
                )
            ],
        ),
    )
    version_2 = WorkflowVersionUpsert(
        workflow_external_id=new_workflow.external_id,
        version="2",
        workflow_definition=WorkflowDefinitionUpsert(
            tasks=[
                WorkflowTask(
                    external_id="subworkflow1",
                    parameters=SubworkflowTaskParameters(
                        tasks=[
                            WorkflowTask(
                                external_id="s1-task1",
                                parameters=FunctionTaskParameters(
                                    external_id="non-existing-function",
                                    data={"a": 3, "b": 4},
                                    is_async_complete=True,
                                ),
                                timeout=120,
                                retries=2,
                            ),
                            WorkflowTask(
                                external_id="s1-task2",
                                parameters=TransformationTaskParameters(
                                    external_id="non-existing-transformation",
                                ),
                            ),
                        ]
                    ),
                ),
                WorkflowTask(
                    external_id="task1",
                    parameters=SubworkflowReferenceParameters(
                        workflow_external_id=new_workflow.external_id,
                        version="1",
                    ),
                    depends_on=["subworkflow1"],
                ),
            ],
        ),
    )
    for version in [version_1, version_2]:
        cognite_client.workflows.versions.upsert(version)
    yield cognite_client.workflows.versions.list(workflow_version_ids=new_workflow.external_id)
    cognite_client.workflows.versions.delete(
        [(new_workflow.external_id, version_1.version), (new_workflow.external_id, version_2.version)],
        ignore_unknown_ids=True,
    )
    assert cognite_client.workflows.versions.retrieve(new_workflow.external_id, version_1.version) is None
    assert cognite_client.workflows.versions.retrieve(new_workflow.external_id, version_2.version) is None


@pytest.fixture
def workflow_execution_list(cognite_client: CogniteClient, new_workflow_version: WorkflowVersion):
    run_1 = cognite_client.workflows.executions.run(
        new_workflow_version.workflow_external_id,
        new_workflow_version.version,
        input={"a": 5, "b": 6},
        metadata={"test": "integration_completed"},
    )
    total_sleep = 0.0
    while run_1.status == "running" and total_sleep < 30:
        time.sleep(0.5)
        total_sleep += 0.5
        run_1 = cognite_client.workflows.executions.retrieve_detailed(run_1.id).as_execution()

    run_2 = cognite_client.workflows.executions.run(
        new_workflow_version.workflow_external_id,
        new_workflow_version.version,
        input={"a": 5, "b": 6},
        metadata={"test": "integration_cancelled"},
    )
    run_2 = cognite_client.workflows.executions.cancel(id=run_2.id, reason="test cancel")
    return WorkflowExecutionList([run_1, run_2])


@pytest.fixture(scope="class")
def workflow_setup_pre_trigger(cognite_client: CogniteClient):
    workflow = WorkflowUpsert(
        external_id=f"integration_test-workflow_{random_string(5)}",
    )
    cognite_client.workflows.upsert(workflow)
    version = WorkflowVersionUpsert(
        workflow_external_id=workflow.external_id,
        version="1",
        workflow_definition=WorkflowDefinitionUpsert(
            tasks=[
                WorkflowTask(
                    external_id=f"{workflow.external_id}-1-task1",
                    parameters=CDFTaskParameters(
                        resource_path="/timeseries",
                        method="GET",
                    ),
                ),
            ],
        ),
    )
    cognite_client.workflows.versions.upsert(version)
    yield version
    cognite_client.workflows.versions.delete((workflow.external_id, version.version), ignore_unknown_ids=True)
    assert cognite_client.workflows.versions.retrieve(workflow.external_id, version.version) is None
    cognite_client.workflows.delete(workflow.external_id, ignore_unknown_ids=True)
    assert cognite_client.workflows.retrieve(workflow.external_id) is None


@pytest.fixture(scope="class")
def workflow_scheduled_trigger(cognite_client: CogniteClient, workflow_setup_pre_trigger: WorkflowVersion):
    version = workflow_setup_pre_trigger
    trigger = cognite_client.workflows.triggers.upsert(
        WorkflowTriggerUpsert(
            external_id=f"scheduled-trigger_{version.workflow_external_id}",
            trigger_rule=WorkflowScheduledTriggerRule(cron_expression="* * * * *"),
            workflow_external_id=version.workflow_external_id,
            workflow_version=version.version,
            input={"a": 1, "b": 2},
            metadata={"test": "integration_schedule"},
        )
    )
    # have to sleep until workflow is triggered because it's the only way to properly test get_trigger_run_history
    now = datetime.now()
    seconds_til_next_minute = 60 - now.second + 5
    time.sleep(seconds_til_next_minute)

    yield trigger
    cognite_client.workflows.triggers.delete(trigger.external_id)
    assert cognite_client.workflows.retrieve(trigger.external_id) is None


@pytest.fixture(scope="class")
def workflow_data_modeling_trigger(cognite_client: CogniteClient, workflow_setup_pre_trigger: WorkflowVersion):
    version = workflow_setup_pre_trigger
    trigger = cognite_client.workflows.triggers.create(
        WorkflowTriggerUpsert(
            external_id=f"data-modeling-trigger_{version.workflow_external_id}",
            trigger_rule=WorkflowDataModelingTriggerRule(
                data_modeling_query=WorkflowTriggerDataModelingQuery(
                    with_={"timeseries": NodeResultSetExpression()},
                    select={
                        "timeseries": Select(
                            sources=[SourceSelector(ViewId("cdf_cdm", "CogniteTimeSeries", "v1"), ["name"])]
                        )
                    },
                ),
                batch_size=500,
                batch_timeout=300,
            ),
            workflow_external_id=version.workflow_external_id,
            workflow_version=version.version,
        )
    )
    yield trigger
    cognite_client.workflows.triggers.delete(trigger.external_id)
    assert cognite_client.workflows.retrieve(trigger.external_id) is None


class TestWorkflows:
    def test_upsert_preexisting(self, cognite_client: CogniteClient, new_workflow: Workflow) -> None:
        new_workflow.description = "Updated description for testing purposes"
        updated_workflow = cognite_client.workflows.upsert(new_workflow.as_write())

        assert updated_workflow.external_id == new_workflow.external_id
        assert updated_workflow.description == new_workflow.description
        assert updated_workflow.data_set_id == new_workflow.data_set_id

    def test_delete_multiple_non_existing_raise(self, cognite_client: CogniteClient, new_workflow: Workflow) -> None:
        with pytest.raises(CogniteAPIError, match="workflows were not found"):
            cognite_client.workflows.delete(
                [new_workflow.external_id, "integration_test-non_existing_workflow"], ignore_unknown_ids=False
            )
        assert cognite_client.workflows.retrieve(new_workflow.external_id) is not None

    def test_delete_multiple_non_existing(self, cognite_client: CogniteClient, new_workflow: Workflow) -> None:
        cognite_client.workflows.delete(
            [new_workflow.external_id, "integration_test-non_existing_workflow"], ignore_unknown_ids=True
        )
        assert cognite_client.workflows.retrieve(new_workflow.external_id) is None

    def test_retrieve_workflow(self, cognite_client: CogniteClient, workflow_list: WorkflowList) -> None:
        retrieved = cognite_client.workflows.retrieve(workflow_list[0].external_id)
        assert retrieved == workflow_list[0]

    def test_retrieve_non_existing_workflow(self, cognite_client: CogniteClient) -> None:
        non_existing = cognite_client.workflows.retrieve("integration_test-non_existing_workflow")
        assert non_existing is None

    @pytest.mark.skip("flaky; fix underway")
    def test_list_workflows(self, cognite_client: CogniteClient, workflow_list: WorkflowList) -> None:
        listed = cognite_client.workflows.list(limit=-1)
        assert len(listed) >= len(workflow_list)
        assert workflow_list._external_id_to_item.keys() <= listed._external_id_to_item.keys()


class TestWorkflowVersions:
    def test_upsert_run_delete_with_simulation_task(
        self,
        cognite_client: CogniteClient,
        workflow_simint_routine: str,
    ):
        workflow_id = "integration_test-workflow_for_simulator_integration" + random_string(5)

        version = WorkflowVersionUpsert(
            workflow_external_id=workflow_id,
            version="1",
            workflow_definition=WorkflowDefinitionUpsert(
                tasks=[
                    WorkflowTask(
                        external_id=f"{workflow_id}-1-task1" + random_string(5),
                        parameters=SimulationTaskParameters(
                            routine_external_id=workflow_simint_routine,
                            inputs=[
                                SimulationInputOverride(
                                    reference_id="CWT", value=11, unit=SimulationValueUnitName(name="F")
                                )
                            ],
                        ),
                        timeout=100,
                    )
                ],
                description=None,
            ),
        )

        cognite_client.workflows.versions.delete(version.as_id(), ignore_unknown_ids=True)
        created_version: WorkflowVersion | None = None

        try:
            created_version = cognite_client.workflows.versions.upsert(version)
            assert created_version.workflow_external_id == workflow_id
            assert created_version.workflow_definition.tasks[0].type == "simulation"
            assert len(created_version.workflow_definition.tasks) > 0

            execution = cognite_client.workflows.executions.run(workflow_id, version.version)
            execution_detailed = None
            simulation_task = None

            for _ in range(20):
                execution_detailed = cognite_client.workflows.executions.retrieve_detailed(execution.id)
                simulation_task = execution_detailed.executed_tasks[0]

                if simulation_task.status == "in_progress":
                    finish_simulation_runs(cognite_client, workflow_simint_routine)

                if execution_detailed.status == "completed" or execution_detailed.status == "failed":
                    break

                time.sleep(1.5)

            assert isinstance(simulation_task.output, SimulationTaskOutput)
            assert simulation_task.status == "completed"
            assert simulation_task.output.run_id is not None
            assert simulation_task.output.log_id is not None

        finally:
            if created_version is not None:
                cognite_client.workflows.versions.delete(
                    created_version.as_id(),
                )
                cognite_client.workflows.delete(created_version.workflow_external_id)

    def test_upsert_preexisting(self, cognite_client: CogniteClient, new_workflow_version: WorkflowVersion) -> None:
        new_workflow_version.workflow_definition.description = "Updated description for testing purposes"
        updated_version = cognite_client.workflows.versions.upsert(new_workflow_version.as_write())

        assert updated_version.workflow_external_id == new_workflow_version.workflow_external_id
        assert updated_version.version == new_workflow_version.version
        assert updated_version.workflow_definition.description == new_workflow_version.workflow_definition.description

    def test_list_workflow_versions(
        self, cognite_client: CogniteClient, workflow_version_list: WorkflowVersionList
    ) -> None:
        wf_xid = workflow_version_list[0].workflow_external_id
        listed_by_wf_xid = cognite_client.workflows.versions.list(wf_xid)
        listed_by_wf_version_id = cognite_client.workflows.versions.list(WorkflowVersionId(wf_xid))
        listed_by_as_ids = cognite_client.workflows.versions.list(workflow_version_list.as_ids())

        ids_tuples = [wid.as_primitive() for wid in workflow_version_list.as_ids()]
        listed_by_tuples = cognite_client.workflows.versions.list(ids_tuples)

        unittest.TestCase().assertCountEqual(workflow_version_list, listed_by_wf_xid)
        unittest.TestCase().assertCountEqual(listed_by_wf_xid, listed_by_wf_version_id)
        unittest.TestCase().assertCountEqual(listed_by_wf_version_id, listed_by_as_ids)
        unittest.TestCase().assertCountEqual(listed_by_as_ids, listed_by_tuples)

        listed_limit = cognite_client.workflows.versions.list(limit=1)
        assert len(listed_limit) == 1

    def test_delete_non_existing_raise(
        self, cognite_client: CogniteClient, new_workflow_version: WorkflowVersion
    ) -> None:
        with pytest.raises(CogniteAPIError, match="not found"):
            cognite_client.workflows.versions.delete(
                [
                    (new_workflow_version.workflow_external_id, new_workflow_version.version),
                    (new_workflow_version.workflow_external_id, "non_existing_version"),
                ],
                ignore_unknown_ids=False,
            )
        assert cognite_client.workflows.versions.retrieve(*new_workflow_version.as_id().as_primitive()) is not None

    def test_delete_non_existing(self, cognite_client: CogniteClient, new_workflow_version: WorkflowVersion) -> None:
        cognite_client.workflows.versions.delete(
            [
                (new_workflow_version.workflow_external_id, new_workflow_version.version),
                (new_workflow_version.workflow_external_id, "non_existing_version"),
            ],
            ignore_unknown_ids=True,
        )
        assert cognite_client.workflows.versions.retrieve(new_workflow_version.as_id().as_tuple()) is None

    def test_retrieve_workflow(self, cognite_client: CogniteClient, new_workflow_version: WorkflowVersion) -> None:
        retrieved = cognite_client.workflows.versions.retrieve(new_workflow_version.as_id().as_tuple())
        assert retrieved == new_workflow_version

    def test_retrieve_non_existing_workflow(self, cognite_client: CogniteClient) -> None:
        non_existing = cognite_client.workflows.versions.retrieve("integration_test-non_existing_workflow", "1")
        assert non_existing is None


class TestWorkflowExecutions:
    def test_list_workflow_executions(
        self,
        cognite_client: CogniteClient,
        workflow_execution_list: WorkflowExecutionList,
    ) -> None:
        listed = cognite_client.workflows.executions.list(
            workflow_version_ids=workflow_execution_list[0].as_workflow_id()
        )

        unittest.TestCase().assertCountEqual(listed, workflow_execution_list)

    def test_list_workflow_executions_by_status(
        self,
        cognite_client: CogniteClient,
        workflow_execution_list: WorkflowExecutionList,
    ) -> None:
        listed_completed = cognite_client.workflows.executions.list(statuses=["completed", "terminated"])
        for execution in listed_completed:
            assert execution.status in ["completed", "terminated"]

    def test_retrieve_workflow_execution_detailed(
        self,
        cognite_client: CogniteClient,
        workflow_execution_list: WorkflowExecutionList,
    ) -> None:
        retrieved = cognite_client.workflows.executions.retrieve_detailed(workflow_execution_list[0].id)
        assert retrieved.as_execution().dump() == workflow_execution_list[0].dump()
        assert retrieved.executed_tasks
        assert retrieved.metadata == {"test": "integration_completed"}

    def test_retrieve_non_existing_workflow_execution(self, cognite_client: CogniteClient) -> None:
        non_existing = cognite_client.workflows.executions.retrieve_detailed(
            "integration_test-non_existing_workflow_execution"
        )

        assert non_existing is None

    def test_trigger_retrieve_detailed_update_async_task(
        self,
        cognite_client: CogniteClient,
        async_workflow_version: WorkflowVersion,
    ) -> None:
        workflow_execution = cognite_client.workflows.executions.run(
            async_workflow_version.workflow_external_id,
            async_workflow_version.version,
        )

        async_task = async_workflow_version.workflow_definition.tasks[0]
        assert isinstance(async_task.parameters, FunctionTaskParameters)
        assert async_task.parameters.is_async_complete

        workflow_execution_detailed = cognite_client.workflows.executions.retrieve_detailed(workflow_execution.id)
        async_task = workflow_execution_detailed.executed_tasks[0]

        async_task = cognite_client.workflows.tasks.update(async_task.id, "completed")
        assert async_task.status == "completed"
        time.sleep(5)
        assert cognite_client.workflows.executions.retrieve_detailed(workflow_execution.id).status == "completed"

    def test_trigger_cancel_retry_workflow(
        self, cognite_client: CogniteClient, new_workflow_version: WorkflowVersion
    ) -> None:
        workflow_execution = cognite_client.workflows.executions.run(
            new_workflow_version.workflow_external_id,
            new_workflow_version.version,
        )

        cancelled_workflow_execution = cognite_client.workflows.executions.cancel(
            id=workflow_execution.id, reason="test"
        )
        assert cancelled_workflow_execution.status == "terminated"
        assert cancelled_workflow_execution.reason_for_incompletion == "test"

        retried_workflow_execution = cognite_client.workflows.executions.retry(workflow_execution.id)
        assert retried_workflow_execution.status == "running"


class TestWorkflowTriggers:
    def test_create_update_preexisting_scheduled_trigger(
        self,
        cognite_client: CogniteClient,
        workflow_scheduled_trigger: WorkflowTrigger,
    ) -> None:
        assert workflow_scheduled_trigger is not None
        assert workflow_scheduled_trigger.external_id.startswith("scheduled-trigger_integration_test-workflow")
        assert workflow_scheduled_trigger.trigger_rule == WorkflowScheduledTriggerRule(cron_expression="* * * * *")
        assert workflow_scheduled_trigger.workflow_external_id.startswith("integration_test-workflow_")
        assert workflow_scheduled_trigger.workflow_version == "1"
        assert workflow_scheduled_trigger.input == {"a": 1, "b": 2}
        assert workflow_scheduled_trigger.metadata == {"test": "integration_schedule"}
        assert workflow_scheduled_trigger.created_time is not None
        assert workflow_scheduled_trigger.last_updated_time is not None

        updated_trigger = cognite_client.workflows.triggers.upsert(
            WorkflowTriggerUpsert(
                external_id=workflow_scheduled_trigger.external_id,
                trigger_rule=WorkflowScheduledTriggerRule(cron_expression="0 * * * *"),
                workflow_external_id=workflow_scheduled_trigger.workflow_external_id,
                workflow_version=workflow_scheduled_trigger.workflow_version,
                input=workflow_scheduled_trigger.input,
            )
        )
        assert updated_trigger is not None
        assert updated_trigger.external_id == workflow_scheduled_trigger.external_id
        assert updated_trigger.trigger_rule == WorkflowScheduledTriggerRule(cron_expression="0 * * * *")
        assert updated_trigger.workflow_external_id == workflow_scheduled_trigger.workflow_external_id
        assert updated_trigger.workflow_version == workflow_scheduled_trigger.workflow_version
        assert updated_trigger.input == workflow_scheduled_trigger.input
        assert updated_trigger.metadata == {}
        assert updated_trigger.created_time == workflow_scheduled_trigger.created_time
        assert updated_trigger.last_updated_time > workflow_scheduled_trigger.last_updated_time

    def test_create_update_delete_data_modeling_trigger(
        self,
        cognite_client: CogniteClient,
        workflow_data_modeling_trigger: WorkflowTrigger,
    ) -> None:
        assert workflow_data_modeling_trigger is not None
        assert workflow_data_modeling_trigger.external_id.startswith("data-modeling-trigger_integration_test-workflow")
        assert workflow_data_modeling_trigger.trigger_rule == WorkflowDataModelingTriggerRule(
            data_modeling_query=WorkflowTriggerDataModelingQuery(
                with_={"timeseries": NodeResultSetExpression()},
                select={
                    "timeseries": Select(
                        sources=[SourceSelector(ViewId("cdf_cdm", "CogniteTimeSeries", "v1"), ["name"])]
                    )
                },
            ),
            batch_size=500,
            batch_timeout=300,
        )
        assert workflow_data_modeling_trigger.workflow_external_id.startswith("integration_test-workflow_")
        assert workflow_data_modeling_trigger.workflow_version == "1"
        assert workflow_data_modeling_trigger.created_time is not None
        assert workflow_data_modeling_trigger.last_updated_time is not None
        updated_trigger = cognite_client.workflows.triggers.upsert(
            WorkflowTriggerUpsert(
                external_id=workflow_data_modeling_trigger.external_id,
                trigger_rule=WorkflowDataModelingTriggerRule(
                    data_modeling_query=WorkflowTriggerDataModelingQuery(
                        with_={"timeseries": NodeResultSetExpression()},
                        select={
                            "timeseries": Select(
                                sources=[SourceSelector(ViewId("cdf_cdm", "CogniteTimeSeries", "v1"), ["name"])]
                            )
                        },
                    ),
                    batch_size=100,
                    batch_timeout=100,
                ),
                workflow_external_id=workflow_data_modeling_trigger.workflow_external_id,
                workflow_version=workflow_data_modeling_trigger.workflow_version,
            )
        )
        assert updated_trigger is not None
        assert updated_trigger.external_id == workflow_data_modeling_trigger.external_id
        assert updated_trigger.trigger_rule == WorkflowDataModelingTriggerRule(
            data_modeling_query=WorkflowTriggerDataModelingQuery(
                with_={"timeseries": NodeResultSetExpression()},
                select={
                    "timeseries": Select(
                        sources=[SourceSelector(ViewId("cdf_cdm", "CogniteTimeSeries", "v1"), ["name"])]
                    )
                },
            ),
            batch_size=100,
            batch_timeout=100,
        )
        assert updated_trigger.workflow_external_id == workflow_data_modeling_trigger.workflow_external_id
        assert updated_trigger.workflow_version == workflow_data_modeling_trigger.workflow_version
        assert updated_trigger.created_time == workflow_data_modeling_trigger.created_time
        assert updated_trigger.last_updated_time > workflow_data_modeling_trigger.last_updated_time

    def test_trigger_list(
        self,
        cognite_client: CogniteClient,
        workflow_scheduled_trigger: WorkflowTrigger,
        workflow_data_modeling_trigger: WorkflowTrigger,
    ) -> None:
        listed = cognite_client.workflows.triggers.get_triggers(limit=-1)
        assert workflow_scheduled_trigger.external_id in listed._external_id_to_item
        assert workflow_data_modeling_trigger.external_id in listed._external_id_to_item

    def test_trigger_run_history(
        self,
        cognite_client: CogniteClient,
        workflow_scheduled_trigger: WorkflowTrigger,
    ) -> None:
        history = cognite_client.workflows.triggers.get_trigger_run_history(
            external_id=workflow_scheduled_trigger.external_id
        )
        assert len(history) > 0
        assert history[0].external_id == workflow_scheduled_trigger.external_id
        assert history[0].workflow_external_id == workflow_scheduled_trigger.workflow_external_id
        assert history[0].workflow_version == workflow_scheduled_trigger.workflow_version

        detailed = cognite_client.workflows.executions.retrieve_detailed(history[0].workflow_execution_id)
        assert detailed is not None
        assert detailed.metadata == workflow_scheduled_trigger.metadata
        # version gets appended to input when executed by a trigger
        workflow_scheduled_trigger.input["version"] = workflow_scheduled_trigger.workflow_version
        assert detailed.input == workflow_scheduled_trigger.input

    def test_trigger_run_history_non_existing(
        self,
        cognite_client: CogniteClient,
    ) -> None:
        with pytest.raises(CogniteAPIError, match="Workflow trigger not found."):
            cognite_client.workflows.triggers.get_trigger_run_history(
                external_id="integration_test-non_existing_trigger"
            )
