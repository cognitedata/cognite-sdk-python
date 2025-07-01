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


def test_ask_document_tool():
    tool = AskDocumentAgentToolUpsert(name="ask_document", description="Ask questions about documents")
    dumped = tool.dump(camel_case=True)
    assert dumped["name"] == "ask_document"
    assert dumped["description"] == "Ask questions about documents"
    assert dumped["type"] == "askDocument"


def test_summarize_document_tool():
    tool = SummarizeDocumentAgentToolUpsert(name="summarize_document", description="Summarize document content")
    dumped = tool.dump(camel_case=True)
    assert dumped["name"] == "summarize_document"
    assert dumped["description"] == "Summarize document content"
    assert dumped["type"] == "summarizeDocument"


def test_query_knowledge_graph_tool():
    config = QueryKnowledgeGraphAgentToolConfiguration(
        data_models=[
            DataModelInfo(space="my_space", external_id="my_model", version="v1", view_external_ids=["my_view"])
        ],
        instance_spaces=InstanceSpaces(type="all"),
    )

    tool = QueryKnowledgeGraphAgentToolUpsert(
        name="query_graph", description="Query the knowledge graph", configuration=config
    )

    dumped = tool.dump(camel_case=True)
    assert dumped["name"] == "query_graph"
    assert dumped["description"] == "Query the knowledge graph"
    assert dumped["type"] == "queryKnowledgeGraph"
    assert dumped["configuration"]["dataModels"][0]["space"] == "my_space"
    assert dumped["configuration"]["dataModels"][0]["externalId"] == "my_model"
    assert dumped["configuration"]["dataModels"][0]["version"] == "v1"
    assert dumped["configuration"]["dataModels"][0]["viewExternalIds"] == ["my_view"]
    assert dumped["configuration"]["instanceSpaces"]["type"] == "all"


def test_query_timeseries_datapoints_tool():
    tool = QueryTimeSeriesDatapointsAgentToolUpsert(name="query_ts", description="Query time series data")
    dumped = tool.dump(camel_case=True)
    assert dumped["name"] == "query_ts"
    assert dumped["description"] == "Query time series data"
    assert dumped["type"] == "queryTimeSeriesDatapoints"


@pytest.mark.parametrize(
    "tool_class,expected_type",
    [
        (AskDocumentAgentToolUpsert, "askDocument"),
        (SummarizeDocumentAgentToolUpsert, "summarizeDocument"),
        (QueryTimeSeriesDatapointsAgentToolUpsert, "queryTimeSeriesDatapoints"),
    ],
)
def test_tool_types(tool_class, expected_type):
    """Test that each tool class has the correct type when dumped"""
    tool = tool_class(name="test_tool", description="Test description")
    dumped = tool.dump(camel_case=True)
    assert dumped["type"] == expected_type


def test_knowledge_graph_tool_with_manual_spaces():
    """Test knowledge graph tool with manually specified spaces"""
    config = QueryKnowledgeGraphAgentToolConfiguration(
        data_models=[
            DataModelInfo(space="my_space", external_id="my_model", version="v1", view_external_ids=["my_view"])
        ],
        instance_spaces=InstanceSpaces(type="manual", spaces=["space1", "space2"]),
    )

    tool = QueryKnowledgeGraphAgentToolUpsert(
        name="query_graph", description="Query the knowledge graph", configuration=config
    )

    dumped = tool.dump(camel_case=True)
    assert dumped["configuration"]["instanceSpaces"]["type"] == "manual"
    assert dumped["configuration"]["instanceSpaces"]["spaces"] == ["space1", "space2"]


def test_knowledge_graph_tool_multiple_models():
    """Test knowledge graph tool with multiple data models"""
    config = QueryKnowledgeGraphAgentToolConfiguration(
        data_models=[
            DataModelInfo(space="space1", external_id="model1", version="v1", view_external_ids=["view1"]),
            DataModelInfo(space="space2", external_id="model2", version="v2", view_external_ids=["view2", "view3"]),
        ],
        instance_spaces=InstanceSpaces(type="all"),
    )

    tool = QueryKnowledgeGraphAgentToolUpsert(
        name="query_graph", description="Query the knowledge graph", configuration=config
    )

    dumped = tool.dump(camel_case=True)
    assert len(dumped["configuration"]["dataModels"]) == 2
    assert dumped["configuration"]["dataModels"][0]["space"] == "space1"
    assert dumped["configuration"]["dataModels"][1]["space"] == "space2"
    assert dumped["configuration"]["dataModels"][1]["viewExternalIds"] == ["view2", "view3"]


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
