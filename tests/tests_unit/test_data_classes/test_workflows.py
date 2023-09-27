from typing import Any

import pytest

from cognite.client.data_classes.workflows import (
    CDFTaskOutput,
    DynamicTaskOutput,
    FunctionTaskOutput,
    FunctionTaskParameters,
    TransformationTaskOutput,
    WorkflowTask,
    WorkflowTaskOutput,
    WorkflowVersionId,
    _WorkflowIds,
)


class TestWorkflowTaskOutput:
    @pytest.mark.parametrize(
        "output",
        [
            FunctionTaskOutput(call_id=123, function_id=3456, response={"test": 1}),
            DynamicTaskOutput(
                dynamic_tasks=[
                    WorkflowTask(external_id="abc", name="abc", parameters=FunctionTaskParameters(external_id="def"))
                ]
            ),
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
            [("abc",), _WorkflowIds([WorkflowVersionId("abc")])],
            [("abc", "def"), _WorkflowIds([WorkflowVersionId("abc", "def")])],
            [{"workflowExternalId": "abc"}, _WorkflowIds([WorkflowVersionId("abc")])],
            [{"workflowExternalId": "abc", "version": "def"}, _WorkflowIds([WorkflowVersionId("abc", "def")])],
            [WorkflowVersionId("abc"), _WorkflowIds([WorkflowVersionId("abc")])],
            [["abc", "def"], _WorkflowIds([WorkflowVersionId("abc"), WorkflowVersionId("def")])],
            [
                [WorkflowVersionId("abc"), WorkflowVersionId("def")],
                _WorkflowIds([WorkflowVersionId("abc"), WorkflowVersionId("def")]),
            ],
            [
                _WorkflowIds([WorkflowVersionId("abc"), WorkflowVersionId("def")]),
                _WorkflowIds([WorkflowVersionId("abc"), WorkflowVersionId("def")]),
            ],
            [
                [("abc", "def"), ("ghi", "jkl")],
                _WorkflowIds([WorkflowVersionId("abc", "def"), WorkflowVersionId("ghi", "jkl")]),
            ],
        ],
    )
    def test_load(self, resource: Any, expected: _WorkflowIds):
        assert _WorkflowIds._load(resource) == expected
