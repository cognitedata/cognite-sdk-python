import pytest

from cognite.client.data_classes.workflows import (
    CDFTaskOutput,
    DynamicTaskOutput,
    FunctionTaskOutput,
    FunctionTaskParameters,
    TransformationTaskOutput,
    WorkflowTask,
    WorkflowTaskOutput,
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
