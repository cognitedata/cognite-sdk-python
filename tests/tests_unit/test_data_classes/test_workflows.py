from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from cognite.client.data_classes.simulators.runs import SimulationInputOverride
from cognite.client.data_classes.workflows import (
    CDFTaskOutput,
    DynamicTaskOutput,
    DynamicTaskParameters,
    FunctionTaskOutput,
    FunctionTaskParameters,
    SimulationTaskOutput,
    SimulationTaskParameters,
    SubworkflowTaskOutput,
    TransformationTaskOutput,
    TransformationTaskParameters,
    UnknownWorkflowTaskOutput,
    UnknownWorkflowTaskParameters,
    WorkflowDefinition,
    WorkflowDefinitionUpsert,
    WorkflowExecutionDetailed,
    WorkflowIds,
    WorkflowTask,
    WorkflowTaskOutput,
    WorkflowTaskParameters,
    WorkflowVersionId,
)


class TestWorkFlowDefinitions:
    def test_upsert_variant_doesnt_accept_hash(self) -> None:
        task = WorkflowTask(external_id="foo", parameters=TransformationTaskParameters(external_id="something"))
        WorkflowDefinition(tasks=[task], description="desc", hash_="very-random")

        with pytest.raises(TypeError, match=r"unexpected keyword argument 'hash_'$"):
            WorkflowDefinitionUpsert(tasks=[task], description="desc", hash_="very-random")  # type: ignore[call-arg]


class TestWorkflowId:
    @pytest.mark.parametrize(
        "workflow_id",
        [
            WorkflowVersionId(workflow_external_id="abc"),
            WorkflowVersionId(workflow_external_id="def", version="3000"),
        ],
    )
    def test_serialization(self, workflow_id: WorkflowVersionId) -> None:
        assert WorkflowVersionId.load(workflow_id.dump(camel_case=True)).dump() == workflow_id.dump()


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
    def test_load(self, resource: Any, expected: WorkflowIds) -> None:
        assert WorkflowIds.load(resource) == expected


class TestWorkflowExecutionDetailed:
    @pytest.fixture(scope="class")
    def execution_data(self) -> dict[str, Any]:
        test_data = Path(__file__).parent / "data/workflow_execution.json"
        with test_data.open() as f:
            return json.load(f)

    def test_dump(self, execution_data: dict[str, Any]) -> None:
        wf_execution = WorkflowExecutionDetailed.load(execution_data)
        dumped = wf_execution.dump(camel_case=False)
        dumped_camel = wf_execution.dump(camel_case=True)
        assert dumped["metadata"] == {"supervisor": "Jimmy", "best_number": 42}
        assert dumped["input"] == {
            "triggerEvent": {
                "externalId": "TEST_test_7ca14a56-c807-4bd6-b287-64936078ef26",
            },
            "version": "latest",
            "snake_case_lets_go": "yes",
        }
        assert dumped["metadata"] == dumped_camel["metadata"]
        assert dumped["input"] == dumped_camel["input"]

    def test_load(self, execution_data: dict[str, Any]) -> None:
        wf_execution = WorkflowExecutionDetailed.load(execution_data)
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
            "snake_case_lets_go": "yes",
        }

    def test_definition_parsed_correctly(self, execution_data: dict[str, Any]) -> None:
        wf_execution = WorkflowExecutionDetailed.load(execution_data)
        assert wf_execution.workflow_definition.hash_ == "8AE17296EE6BCCD0B7D9C184E100A5F98069553C"

        expected = [
            WorkflowTask(
                external_id="testTaskDispatcher",
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
                on_failure="abortWorkflow",
            ),
            WorkflowTask(
                external_id="applicationExecution",
                description="Run a collection of preprocessor and app runs concurrently",
                parameters=DynamicTaskParameters(tasks="${testTaskDispatcher.output.response.testTasks}"),
                retries=0,
                timeout=3600,
                on_failure="skipTask",
                depends_on=["testTaskDispatcher"],
            ),
            WorkflowTask(
                external_id="testSimulation",
                parameters=SimulationTaskParameters(
                    inputs=[SimulationInputOverride(reference_id="some-ref", value=1)],
                    routine_external_id="routine-1",
                    run_time=123,
                ),
                retries=0,
                timeout=3600,
                on_failure="skipTask",
                depends_on=["testTaskDispatcher"],
            ),
        ]
        assert len(wf_execution.workflow_definition.tasks) == 5

        for expected_task, actual_task in zip(expected, wf_execution.workflow_definition.tasks):
            assert actual_task.external_id == expected_task.external_id
            assert actual_task.type == expected_task.type
            assert actual_task.parameters.dump() == expected_task.parameters.dump()
            assert actual_task.retries == expected_task.retries
            assert actual_task.timeout == expected_task.timeout
            assert actual_task.depends_on == expected_task.depends_on

    def test_executed_tasks_parsed_correctly(self, execution_data: dict[str, Any]) -> None:
        wf_execution = WorkflowExecutionDetailed.load(execution_data)
        tasks = wf_execution.executed_tasks

        assert len(tasks) == 17

        first = tasks[0]
        assert first.id == "38b3e696-adcb-4bf8-9217-747449f55289"
        assert isinstance(first.output, FunctionTaskOutput)
        assert first.output.call_id == 1453249902969082
        assert first.dump(camel_case=True) == execution_data["executedTasks"][0]

        dynamic_tasks = [t for t in tasks if isinstance(t.output, DynamicTaskOutput)]
        assert len(dynamic_tasks) == 2
        assert all(t.output.dump() == {} for t in dynamic_tasks)


class TestWorkflowTask:
    @pytest.mark.parametrize(
        ["raw"],
        [
            (
                {
                    "externalId": "task1",
                    "type": "function",
                    "parameters": {
                        "function": {"externalId": "myFunction"},
                    },
                },
            ),
            (
                {
                    "externalId": "task1",
                    "type": "transformation",
                    "parameters": {
                        "transformation": {
                            "externalId": "myTransformation",
                            "concurrencyPolicy": "fail",
                            "useTransformationCredentials": False,
                        }
                    },
                },
            ),
            (
                {
                    "externalId": "taskFuture",
                    "type": "futureWorkflowTaskType",
                    "parameters": {"futureWorkflowTaskType": {"alpha": 1}},
                },
            ),
        ],
    )
    def test_serialization(self, raw: dict[str, Any]) -> None:
        loaded = WorkflowTask._load(raw)
        assert loaded.dump() == raw


class TestWorkflowTaskOutput:
    """Note: WorkflowTaskOutput subclasses does not work with our automatic test setup because their _load need the
    full API payload (from parent object), where the payload typically contains a taskType + output wrapper.
    However, their dump() returns just the flat attributes, so the auto-test (dump -> load -> dump) cannot
    round-trip them correctly. We test them manually here instead."""

    @pytest.mark.parametrize(
        ["output", "expected"],
        [
            (
                FunctionTaskOutput(call_id=123, function_id=3456, response={"test": 1}),
                {"callId": 123, "functionId": 3456, "response": {"test": 1}},
            ),
            (DynamicTaskOutput(), {}),
            (CDFTaskOutput(response={"test": 1}, status_code=200), {"response": {"test": 1}, "statusCode": 200}),
            (TransformationTaskOutput(job_id=789), {"jobId": 789}),
        ],
    )
    def test_serialization(self, output: WorkflowTaskOutput, expected: dict[str, Any]) -> None:
        assert output.dump(camel_case=True) == expected

    @pytest.mark.parametrize(
        ["payload", "expected_type", "expected_dump"],
        [
            (
                {"taskType": "function", "output": {"callId": 123, "functionId": 456, "response": {"result": "ok"}}},
                FunctionTaskOutput,
                {"callId": 123, "functionId": 456, "response": {"result": "ok"}},
            ),
            (
                {"taskType": "transformation", "output": {"jobId": 789}},
                TransformationTaskOutput,
                {"jobId": 789},
            ),
            (
                {"taskType": "cdf", "output": {"response": {"key": "val"}, "statusCode": 200}},
                CDFTaskOutput,
                {"response": {"key": "val"}, "statusCode": 200},
            ),
            (
                {"taskType": "dynamic"},
                DynamicTaskOutput,
                {},
            ),
            (
                {"taskType": "subworkflow"},
                SubworkflowTaskOutput,
                {},
            ),
            (
                {"taskType": "simulation", "output": {"runId": 1, "logId": 2, "statusMessage": "done"}},
                SimulationTaskOutput,
                {"runId": 1, "logId": 2, "statusMessage": "done"},
            ),
            (
                {"taskType": "novelFutureType", "output": {"customKey": "customVal"}},
                UnknownWorkflowTaskOutput,
                {"customKey": "customVal"},
            ),
            (
                {"taskType": "novelFutureType"},
                UnknownWorkflowTaskOutput,
                {},
            ),
        ],
        ids=[
            "function",
            "transformation",
            "cdf",
            "dynamic",
            "subworkflow",
            "simulation",
            "unknown",
            "unknown-no-output",
        ],
    )
    def test_load_output_and_dump(
        self,
        payload: dict[str, Any],
        expected_type: type[WorkflowTaskOutput],
        expected_dump: dict[str, Any],
    ) -> None:
        loaded = WorkflowTaskOutput.load_output(payload)
        assert isinstance(loaded, expected_type)
        assert loaded.dump(camel_case=True) == expected_dump

    def test_load_output_raises_on_non_string_task_type(self) -> None:
        with pytest.raises(ValueError, match="Invalid taskType"):
            WorkflowTaskOutput.load_output({"taskType": 123})

    def test_unknown_output_dump_camel_case_false_warns_and_preserves_keys(self) -> None:
        payload = {"taskType": "novelType", "output": {"someUserKey": "val"}}
        loaded = WorkflowTaskOutput.load_output(payload)
        assert isinstance(loaded, UnknownWorkflowTaskOutput)
        with pytest.warns(UserWarning, match="snake case is not supported"):
            result = loaded.dump(camel_case=False)
        # The user key should be preserved:
        assert result == {"someUserKey": "val"}


class TestUnknownWorkflowTaskParameters:
    """UnknownWorkflowTaskParameters._load requires task_type from the parent WorkflowTask payload.
    Thus, it cannot be constructed from its own dump() output alone. Round-trip is tested via WorkflowTask."""

    def test_load_parameters_raises_on_non_string_task_type(self) -> None:
        with pytest.raises(ValueError, match=r"Invalid \(task\) type"):
            WorkflowTaskParameters.load_parameters({"type": 123, "parameters": {}})

    def test_load_and_dump_via_workflow_task(self) -> None:
        raw: dict[str, Any] = {
            "externalId": "myTask",
            "type": "novelFutureType",
            "parameters": {"novelFutureType": {"alpha": 1, "beta": "two"}},
        }
        task = WorkflowTask._load(raw)
        assert isinstance(task.parameters, UnknownWorkflowTaskParameters)
        assert task.parameters.task_type == "novelFutureType"
        assert task.dump() == raw

    def test_dump_camel_case_false_warns_and_preserves_keys(self) -> None:
        params = UnknownWorkflowTaskParameters("myType", {"someUserKey": "someValue"})
        with pytest.warns(UserWarning, match="snake case is not supported"):
            result = params.dump(camel_case=False)
        assert result == {"someUserKey": "someValue"}

    def test_dump_camel_case_true_returns_stored_data(self) -> None:
        data = {"someUserKey": 42, "nested": {"a": 1}}
        params = UnknownWorkflowTaskParameters("futureType", data)
        assert params.dump(camel_case=True) == data
