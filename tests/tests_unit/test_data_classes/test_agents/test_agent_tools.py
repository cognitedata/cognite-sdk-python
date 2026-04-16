from __future__ import annotations

import pytest

from cognite.client.data_classes.agents.agent_tools import (
    AgentTool,
    AskDocumentAgentTool,
    QueryAgentTool,
    QueryAgentToolConfiguration,
    QueryKnowledgeGraphAgentTool,
    QueryKnowledgeGraphAgentToolConfiguration,
    QueryTimeSeriesDatapointsAgentTool,
    SummarizeDocumentAgentTool,
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

query_example = {
    "name": "queryExample",
    "type": "query",
    "description": "Run flexible queries against your data model",
    "configuration": {
        "dataModels": {
            "type": "manual",
            "dataModels": [
                {
                    "space": "cdf_idm",
                    "externalId": "CogniteProcessIndustries",
                    "version": "v1",
                    "viewExternalIds": ["CogniteAsset"],
                }
            ],
        },
        "instanceSpaces": {"type": "manual", "spaces": ["my_space"]},
    },
}

query_no_config_example = {
    "name": "queryNoConfigExample",
    "type": "query",
    "description": "Run flexible queries against your data model",
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
            (query_example, QueryAgentTool),
            (query_no_config_example, QueryAgentTool),
            (unknown_example, UnknownAgentTool),
        ],
        ids=[
            "queryKnowledgeGraph",
            "askDocument",
            "summarizeDocument",
            "queryTimeSeriesDatapoints",
            "query",
            "queryNoConfig",
            "somethingElse",
        ],
    )
    def test_agent_tool_load_returns_correct_subtype(self, tool_data: dict, expected_type: type[AgentTool]) -> None:
        """Test that AgentTool._load() returns the correct subtype based on the tool type."""
        loaded_tool = AgentTool._load(tool_data)

        assert isinstance(loaded_tool, expected_type)
        assert loaded_tool.name == tool_data["name"]
        assert loaded_tool.description == tool_data["description"]

        if expected_type is UnknownAgentTool:
            assert isinstance(loaded_tool, UnknownAgentTool)
            assert loaded_tool.type == tool_data["type"]
        else:
            assert loaded_tool._type == expected_type._type

        if "configuration" in tool_data:
            if expected_type is QueryKnowledgeGraphAgentTool:
                assert isinstance(loaded_tool, QueryKnowledgeGraphAgentTool)
                assert isinstance(loaded_tool.configuration, QueryKnowledgeGraphAgentToolConfiguration)
                assert loaded_tool.configuration.dump(camel_case=True) == tool_data["configuration"]
            elif expected_type is QueryAgentTool:
                assert isinstance(loaded_tool, QueryAgentTool)
                assert isinstance(loaded_tool.configuration, QueryAgentToolConfiguration)
                assert loaded_tool.configuration.dump(camel_case=True) == tool_data["configuration"]
            elif expected_type is UnknownAgentTool:
                assert isinstance(loaded_tool, UnknownAgentTool)
                assert loaded_tool.configuration == tool_data["configuration"]
            else:
                raise TypeError(f"Unhandled tool type in test case: {expected_type}")

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
            (query_example, QueryAgentTool),
            (unknown_example, UnknownAgentTool),
        ],
    )
    def test_agent_tool_dump_returns_correct_type(self, tool_data: dict, expected_type: type[AgentTool]) -> None:
        """Test that AgentTool.dump() returns the correct type."""
        loaded_tool = AgentTool._load(tool_data)
        assert isinstance(loaded_tool, expected_type)
        dumped_tool = loaded_tool.dump(camel_case=True)

        if expected_type is UnknownAgentTool:
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
            (query_example, QueryAgentTool),
            (unknown_example, UnknownAgentTool),
        ],
    )
    def test_agent_tool_upsert_returns_correct_type(self, tool_data: dict, expected_type: type[AgentTool]) -> None:
        """Test that AgentToolUpsert.dump() returns the correct type."""
        loaded_tool = AgentTool._load(tool_data)
        assert isinstance(loaded_tool, expected_type)
        dumped_tool = loaded_tool.as_write().dump(camel_case=True)

        if expected_type is UnknownAgentTool:
            assert dumped_tool["type"] == unknown_example["type"]
        else:
            assert dumped_tool["type"] == expected_type._type

        assert dumped_tool["name"] == tool_data["name"]
        assert dumped_tool["description"] == tool_data["description"]


class TestQueryAgentTool:
    def test_load_with_configuration(self) -> None:
        loaded = AgentTool._load(query_example)
        assert isinstance(loaded, QueryAgentTool)
        assert loaded.configuration is not None
        assert isinstance(loaded.configuration, QueryAgentToolConfiguration)
        assert len(loaded.configuration.data_models) == 1
        assert loaded.configuration.data_models[0].space == "cdf_idm"
        assert loaded.configuration.data_models[0].external_id == "CogniteProcessIndustries"
        assert loaded.configuration.instance_spaces is not None
        assert loaded.configuration.instance_spaces.type == "manual"
        assert loaded.configuration.instance_spaces.spaces == ["my_space"]

    def test_load_without_configuration(self) -> None:
        loaded = AgentTool._load(query_no_config_example)
        assert isinstance(loaded, QueryAgentTool)
        assert loaded.configuration is None

    def test_round_trip(self) -> None:
        loaded = AgentTool._load(query_example)
        dumped = loaded.dump(camel_case=True)
        assert dumped == query_example

    def test_as_write_round_trip(self) -> None:
        loaded = AgentTool._load(query_example)
        write = loaded.as_write()
        dumped = write.dump(camel_case=True)
        assert dumped == query_example
