import json
from pathlib import Path
from typing import Any

import pytest

from cognite.client.data_classes.workflows import (
    CDFTaskOutput,
    DynamicTaskOutput,
    DynamicTaskParameters,
    FunctionTaskOutput,
    FunctionTaskParameters,
    TransformationTaskOutput,
    WorkflowExecutionDetailed,
    WorkflowIds,
    WorkflowTask,
    WorkflowTaskOutput,
    WorkflowVersionId,
)


class TestWorkflowTaskOutput:
    @pytest.mark.parametrize(
        "output",
        [
            FunctionTaskOutput(call_id=123, function_id=3456, response={"test": 1}),
            DynamicTaskOutput(),
            CDFTaskOutput(response={"test": 1}, status_code=200),
            TransformationTaskOutput(job_id=789),
        ],
    )
    def test_serialization(self, output: WorkflowTaskOutput):
        assert WorkflowTaskOutput.load_output(output.dump(camel_case=True)).dump() == output.dump()


class TestWorkflowId:
    @pytest.mark.parametrize(
        "workflow_id",
        [
            WorkflowVersionId(workflow_external_id="abc"),
            WorkflowVersionId(workflow_external_id="def", version="3000"),
        ],
    )
    def test_serialization(self, workflow_id: WorkflowVersionId):
        assert WorkflowVersionId._load(workflow_id.dump(camel_case=True)).dump() == workflow_id.dump()


class TestWorkflowIds:
    @pytest.mark.parametrize(
        "resource, expected",
        [
            [("abc",), WorkflowIds([WorkflowVersionId("abc")])],
            [("abc", "def"), WorkflowIds([WorkflowVersionId("abc", "def")])],
            [{"workflowExternalId": "abc"}, WorkflowIds([WorkflowVersionId("abc")])],
            [{"workflowExternalId": "abc", "version": "def"}, WorkflowIds([WorkflowVersionId("abc", "def")])],
            [WorkflowVersionId("abc"), WorkflowIds([WorkflowVersionId("abc")])],
            [["abc", "def"], WorkflowIds([WorkflowVersionId("abc"), WorkflowVersionId("def")])],
            [
                [WorkflowVersionId("abc"), WorkflowVersionId("def")],
                WorkflowIds([WorkflowVersionId("abc"), WorkflowVersionId("def")]),
            ],
            [
                WorkflowIds([WorkflowVersionId("abc"), WorkflowVersionId("def")]),
                WorkflowIds([WorkflowVersionId("abc"), WorkflowVersionId("def")]),
            ],
            [
                [("abc", "def"), ("ghi", "jkl")],
                WorkflowIds([WorkflowVersionId("abc", "def"), WorkflowVersionId("ghi", "jkl")]),
            ],
        ],
    )
    def test_load(self, resource: Any, expected: WorkflowIds):
        assert WorkflowIds._load(resource) == expected


class TestWorkflowExecutionDetailed:
    @pytest.fixture(scope="class")
    def execution_data(self) -> dict:
        test_data = Path(__file__).parent / "data/workflow_execution.json"
        with test_data.open() as f:
            return json.load(f)

    def test_dump(self, execution_data: dict):
        wf_execution = WorkflowExecutionDetailed._load(execution_data)
        dumped = wf_execution.dump(camel_case=False)
        dumped_camel = wf_execution.dump(camel_case=True)
        assert dumped["metadata"] == {"supervisor": "Jimmy", "best_number": 42}
        assert dumped["input"] == {
            "triggerEvent": {
                "externalId": "TEST_test_7ca14a56-c807-4bd6-b287-64936078ef26",
            },
            "version": "latest",
            "snake_case_lets_go": "yes"
        }
        assert dumped["metadata"] == dumped_camel["metadata"]
        assert dumped["input"] == dumped_camel["input"]

    def test_load_works(self, execution_data: dict):
        wf_execution = WorkflowExecutionDetailed._load(execution_data)
        assert wf_execution.id == "7b6bf517-4812-4874-b227-fa7db36830a3"
        assert wf_execution.workflow_external_id == "TestWorkflowTypeBidProcess"
        assert wf_execution.version == "latest"
        assert wf_execution.status == "completed"
        assert wf_execution.created_time == 1696240547972
        assert wf_execution.start_time == 1696240547886
        assert wf_execution.end_time == 1696240836564
        assert wf_execution.metadata == {"supervisor": "Jimmy", "best_number": 42}
        assert wf_execution.input == {
            "triggerEvent": {
                "externalId": "TEST_test_7ca14a56-c807-4bd6-b287-64936078ef26",
            },
            "version": "latest",
            "snake_case_lets_go": "yes"
        }

    def test_definition_parsed_correctly(self, execution_data: dict):
        wf_execution = WorkflowExecutionDetailed._load(execution_data)
        assert wf_execution.workflow_definition.hash_ == "8AE17296EE6BCCD0B7D9C184E100A5F98069553C"

        expected = [
            WorkflowTask(
                external_id="testTaskDispatcher",
                type="function",
                parameters=FunctionTaskParameters(
                    external_id="bid_process_task_dispatcher",
                    data={
                        "workflowType": "TestWorkflowType",
                        "applicationVersion": "123456",
                        "testProcessEventExternalId": "${workflow.input.triggerEvent.externalId}",
                    },
                ),
                retries=2,
                timeout=300,
            ),
            WorkflowTask(
                external_id="applicationExecution",
                type="dynamic",
                description="Run a collection of preprocessor and app runs concurrently",
                parameters=DynamicTaskParameters(tasks="${testTaskDispatcher.output.response.testTasks}"),
                retries=0,
                timeout=3600,
                depends_on=["testTaskDispatcher"],
            ),
        ]
        assert len(wf_execution.workflow_definition.tasks) == 4

        for expected_task, actual_task in zip(expected, wf_execution.workflow_definition.tasks):
            assert actual_task.external_id == expected_task.external_id
            assert actual_task.type == expected_task.type
            assert actual_task.parameters.dump() == expected_task.parameters.dump()
            assert actual_task.retries == expected_task.retries
            assert actual_task.timeout == expected_task.timeout
            assert actual_task.depends_on == expected_task.depends_on

    def test_executed_tasks_parsed_correctly(self, execution_data: dict):
        wf_execution = WorkflowExecutionDetailed._load(execution_data)
        wf_execution.executed_tasks
