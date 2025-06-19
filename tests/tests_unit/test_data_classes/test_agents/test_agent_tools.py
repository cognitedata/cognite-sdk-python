from __future__ import annotations

import pytest

from cognite.client.data_classes.agents.agent_tools import (
    AgentTool,
    AskDocumentAgentTool,
    QueryKnowledgeGraphAgentTool,
    QueryKnowledgeGraphAgentToolConfiguration,
    QueryTimeSeriesDatapointsAgentTool,
    SummarizeDocumentAgentTool,
    UnknownAgentTool,
)

qkgExample = {
    "name": "qkgExample",
    "type": "queryKnowledgeGraph",
    "description": "Query the knowledge graph",
    "configuration": {
        "dataModels": [
            {"space": "cdf_cdm", "externalId": "CogniteCore", "version": "v1", "viewExternalIds": ["CogniteAsset"]}
        ],
        "instanceSpaces": {"type": "manual", "spaces": ["my_space"]},
    },
}

askDocumentExample = {
    "name": "askDocumentExample",
    "type": "askDocument",
    "description": "Ask a question about the document",
}

summarizeDocumentExample = {
    "name": "summarizeDocumentExample",
    "type": "summarizeDocument",
    "description": "Summarize the document",
}

queryTimeSeriesDatapointsExample = {
    "name": "queryTimeSeriesDatapointsExample",
    "type": "queryTimeSeriesDatapoints",
    "description": "Query the time series datapoints",
}

unknownExample = {
    "name": "unknownExample",
    "type": "yolo",  # This is not a known tool type
    "description": "An unknown tool",
    "configuration": {"key": "value"},
}


class TestAgentToolLoad:
    @pytest.mark.parametrize(
        "tool_data,expected_type",
        [
            (qkgExample, QueryKnowledgeGraphAgentTool),
            (askDocumentExample, AskDocumentAgentTool),
            (summarizeDocumentExample, SummarizeDocumentAgentTool),
            (queryTimeSeriesDatapointsExample, QueryTimeSeriesDatapointsAgentTool),
            (unknownExample, UnknownAgentTool),
        ],
        ids=["queryKnowledgeGraph", "askDocument", "summarizeDocument", "queryTimeSeriesDatapoints", "somethingElse"],
    )
    def test_agent_tool_load_returns_correct_subtype(self, tool_data: dict, expected_type: type[AgentTool]) -> None:
        """Test that AgentTool._load() returns the correct subtype based on the tool type."""
        loaded_tool = AgentTool._load(tool_data)

        assert isinstance(loaded_tool, expected_type)
        assert loaded_tool.name == tool_data["name"]
        assert loaded_tool.description == tool_data["description"]

        if isinstance(loaded_tool, UnknownAgentTool):
            assert loaded_tool.type == tool_data["type"]
        else:
            assert loaded_tool._type == expected_type._type

        # Handle configuration comparison based on tool type
        if "configuration" in tool_data:
            if isinstance(loaded_tool, QueryKnowledgeGraphAgentTool):
                # For QueryKnowledgeGraph, we expect a structured configuration object
                assert isinstance(loaded_tool.configuration, QueryKnowledgeGraphAgentToolConfiguration)
                # Compare by serializing the structured object back to dict
                assert loaded_tool.configuration.dump(camel_case=True) == tool_data["configuration"]
            else:
                # For other tools (like UnknownAgentTool), configuration should be a dict
                assert loaded_tool.configuration == tool_data["configuration"]

    def test_unknown_agent_tool_preserves_custom_type(self) -> None:
        """Test that UnknownAgentTool preserves the original type string."""
        loaded_tool = AgentTool._load(unknownExample)

        assert isinstance(loaded_tool, UnknownAgentTool)
        assert loaded_tool.type == "yolo"
        assert loaded_tool.configuration == unknownExample["configuration"]
