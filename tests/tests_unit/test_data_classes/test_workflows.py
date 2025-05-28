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
    SimulationInputOverride,
    SimulationTaskParameters,
    TransformationTaskOutput,
    TransformationTaskParameters,
    WorkflowDefinition,
    WorkflowDefinitionUpsert,
    WorkflowExecutionDetailed,
    WorkflowIds,
    WorkflowTask,
    WorkflowTaskOutput,
    WorkflowVersionId,
)


class TestWorkFlowDefinitions:
    def test_upsert_variant_doesnt_accept_hash(self):
        task = WorkflowTask(external_id="foo", parameters=TransformationTaskParameters(external_id="something"))
        WorkflowDefinition(tasks=[task], description="desc", hash_="very-random")

        with pytest.raises(TypeError, match="unexpected keyword argument 'hash_'$"):
            WorkflowDefinitionUpsert(tasks=[task], description="desc", hash_="very-random")


class TestWorkflowTaskOutput:
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
    def test_serialization(self, output: WorkflowTaskOutput, expected: dict):
        assert output.dump(camel_case=True) == expected


class TestWorkflowId:
    @pytest.mark.parametrize(
        "workflow_id",
        [
            WorkflowVersionId(workflow_external_id="abc"),
            WorkflowVersionId(workflow_external_id="def", version="3000"),
        ],
    )
    def test_serialization(self, workflow_id: WorkflowVersionId):
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
    def test_load(self, resource: Any, expected: WorkflowIds):
        assert WorkflowIds.load(resource) == expected


class TestWorkflowExecutionDetailed:
    @pytest.fixture(scope="class")
    def execution_data(self) -> dict:
        test_data = Path(__file__).parent / "data/workflow_execution.json"
        with test_data.open() as f:
            return json.load(f)

    def test_dump(self, execution_data: dict):
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

    def test_load(self, execution_data: dict):
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

    def test_definition_parsed_correctly(self, execution_data: dict):
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

    def test_executed_tasks_parsed_correctly(self, execution_data: dict):
        wf_execution = WorkflowExecutionDetailed.load(execution_data)

        expected = [
            ("38b3e696-adcb-4bf8-9217-747449f55289", "function", "completed", 1453249902969082),
            ("e9ca204f-2031-46f4-9567-162a54f5eb38", "dynamic", "completed", None),
            ("475d03b3-6e6c-44ad-b8bb-534e1e5560c4", "dynamic", "completed", None),
            ("3f41e58f-cf5d-4391-b276-4698384918fc", "function", "completed", 6958901858387174),
            ("f7a73a85-73df-4a84-bcd2-3a6a05b48673", "function", "completed", 7668632761093605),
            ("9d65f598-5cef-4476-b6be-cc04bb832163", "function", "completed", 1952977430849976),
            ("f69f7b1c-6aea-4213-b5d0-deb2fbabe5a0", "function", "completed", 3675281144107549),
            ("eb240dbc-0062-4314-b4d6-fb51442d02e7", "function", "completed", 2967264911514077),
            ("f7ced93f-e686-4a15-9676-5b32b1f4e52c", "function", "completed", 8805488770050088),
            ("e326b41e-302a-4f29-847b-f0eb76fcde59", "function", "completed", 5300981250266599),
            ("dc9a55a8-eeaf-4beb-9842-8767de7385ec", "function", "completed", 4673999868714216),
            ("3ee0ed1f-dd5c-4334-a239-9e076c68230e", "function", "completed", 1736067993713132),
            ("185a8630-0a60-4adc-b0bb-4fa27fd915df", "function", "completed", 3044388486346549),
            ("51bcfd5e-3ba7-41a2-bc27-34eb9d18e8d0", "function", "completed", 5421875616701543),
            ("11653aa1-391c-4efa-93be-13b0e05f9e9f", "function", "completed", 244237303353736),
            ("c17875c7-6f8f-4ada-84ef-43b4191428aa", "function", "completed", 8400742455854509),
            ("2c3f97df-12a7-4d9e-a488-b3e2400d2434", "function", "completed", 2097899365071295),
        ]

        for (exp_id, exp_type, exp_status, exp_call_id), actual_task in zip(expected, wf_execution.executed_tasks):
            assert actual_task.id == exp_id
            assert actual_task.task_type == exp_type
            assert actual_task.status == exp_status
            if actual_task.task_type == "function":
                assert actual_task.output.call_id == exp_call_id
            if actual_task.task_type == "dynamic":
                assert actual_task.output.dump() == {}

            if actual_task.id == "38b3e696-adcb-4bf8-9217-747449f55289":
                assert actual_task.dump(camel_case=True) == execution_data["executedTasks"][0]


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
        ],
    )
    def test_serialization(self, raw: dict):
        loaded = WorkflowTask._load(raw)
        assert loaded.dump() == raw
