from __future__ import annotations

import pytest

from cognite.client.data_classes.agents.agent_tools import (
    AgentTool,
    AskDocumentAgentTool,
    AskDocumentAgentToolUpsert,
    DataModelInfo,
    InstanceSpaces,
    QueryKnowledgeGraphAgentTool,
    QueryKnowledgeGraphAgentToolConfiguration,
    QueryKnowledgeGraphAgentToolUpsert,
    QueryTimeSeriesDatapointsAgentTool,
    QueryTimeSeriesDatapointsAgentToolUpsert,
    SummarizeDocumentAgentTool,
    SummarizeDocumentAgentToolUpsert,
    UnknownAgentTool,
)

qkg_example = {
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

ask_document_example = {
    "name": "askDocumentExample",
    "type": "askDocument",
    "description": "Ask a question about the document",
}

summarize_document_example = {
    "name": "summarizeDocumentExample",
    "type": "summarizeDocument",
    "description": "Summarize the document",
}

query_time_series_datapoints_example = {
    "name": "queryTimeSeriesDatapointsExample",
    "type": "queryTimeSeriesDatapoints",
    "description": "Query the time series datapoints",
}

unknown_example = {
    "name": "unknownExample",
    "type": "yolo",  # This is not a known tool type
    "description": "An unknown tool",
    "configuration": {"key": "value"},
}

class TestAgentToolLoad:
    @pytest.mark.parametrize(
        "tool_data,expected_type",
        [
            (qkg_example, QueryKnowledgeGraphAgentTool),
            (ask_document_example, AskDocumentAgentTool),
            (summarize_document_example, SummarizeDocumentAgentTool),
            (query_time_series_datapoints_example, QueryTimeSeriesDatapointsAgentTool),
            (unknown_example, UnknownAgentTool),
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
        loaded_tool = AgentTool._load(unknown_example)

        assert isinstance(loaded_tool, UnknownAgentTool)
        assert loaded_tool.type == unknown_example["type"]
        assert loaded_tool.configuration == unknown_example["configuration"]


class TestAgentToolDump:
    @pytest.mark.parametrize(
        "tool_data,expected_type",
        [
            (qkg_example, QueryKnowledgeGraphAgentTool),
            (ask_document_example, AskDocumentAgentTool),
            (summarize_document_example, SummarizeDocumentAgentTool),
            (query_time_series_datapoints_example, QueryTimeSeriesDatapointsAgentTool),
            (unknown_example, UnknownAgentTool),
        ],
    )
    def test_agent_tool_dump_returns_correct_type(self, tool_data: dict, expected_type: type[AgentTool]) -> None:
        """Test that AgentTool.dump() returns the correct type."""
        loaded_tool = AgentTool._load(tool_data)
        dumped_tool = loaded_tool.dump(camel_case=True)

        if isinstance(loaded_tool, UnknownAgentTool):
            assert dumped_tool["type"] == unknown_example["type"]
        else:
            assert dumped_tool["type"] == expected_type._type

        assert dumped_tool["name"] == tool_data["name"]
        assert dumped_tool["description"] == tool_data["description"]

    def test_agent_tool_dump_returns_correct_type_for_unknown_tool(self) -> None:
        """Test that AgentTool.dump() returns the correct type for unknown tools."""
        loaded_tool = AgentTool._load(unknown_example)
        dumped_tool = loaded_tool.dump(camel_case=True)

        assert dumped_tool["type"] == unknown_example["type"]
        assert dumped_tool["name"] == unknown_example["name"]
        assert dumped_tool["description"] == unknown_example["description"]
        assert dumped_tool["configuration"] == unknown_example["configuration"]

    def test_agent_tool_dump_returns_correct_type_for_query_knowledge_graph_tool(self) -> None:
        """Test that AgentTool.dump() returns the correct type for query knowledge graph tools."""
        loaded_tool = AgentTool._load(qkg_example)
        dumped_tool = loaded_tool.dump(camel_case=True)

        assert dumped_tool["type"] == "queryKnowledgeGraph"
        assert dumped_tool["name"] == qkg_example["name"]
        assert dumped_tool["description"] == qkg_example["description"]
        assert dumped_tool["configuration"] == qkg_example["configuration"]


class TestAgentToolUpsert:
    @pytest.mark.parametrize(
        "tool_data,expected_type",
        [
            (qkg_example, QueryKnowledgeGraphAgentTool),
            (ask_document_example, AskDocumentAgentTool),
            (summarize_document_example, SummarizeDocumentAgentTool),
            (query_time_series_datapoints_example, QueryTimeSeriesDatapointsAgentTool),
            (unknown_example, UnknownAgentTool),
        ],
    )
    def test_agent_tool_upsert_returns_correct_type(self, tool_data: dict, expected_type: type[AgentTool]) -> None:
        """Test that AgentToolUpsert.dump() returns the correct type."""
        loaded_tool = AgentTool._load(tool_data)
        dumped_tool = loaded_tool.as_write().dump(camel_case=True)

        if isinstance(loaded_tool, UnknownAgentTool):
            assert dumped_tool["type"] == unknown_example["type"]
        else:
            assert dumped_tool["type"] == expected_type._type

        assert dumped_tool["name"] == tool_data["name"]
        assert dumped_tool["description"] == tool_data["description"]
