from __future__ import annotations

from cognite.client.data_classes.agents.agent_tools import (
    AgentTool,
    AgentToolList,
    AgentToolUpsert,
    AgentToolUpsertList,
)


class TestAgentToolUpsert:
    def test_load_dump(self) -> None:
        data = {
            "name": "test_tool",
            "type": "test_type",
            "description": "A test tool",
            "configuration": {"key": "value"},
        }

        tool = AgentToolUpsert._load(data)
        assert tool.name == "test_tool"
        assert tool.type == "test_type"
        assert tool.description == "A test tool"
        assert tool.configuration == {"key": "value"}

        dumped = tool.dump(camel_case=True)
        assert data == dumped

    def test_as_write(self) -> None:
        tool_upsert = AgentToolUpsert(
            name="test_tool",
            type="test_type",
            description="A test tool",
        )
        assert tool_upsert is tool_upsert.as_write()


class TestAgentTool:
    def test_load_dump(self) -> None:
        # This is a valid configuration for the queryKnowledgeGraph tool
        query_knowledge_graph_configuration = {
            "dataModels": [
                {
                    "space": "cdf_cdm",
                    "externalId": "CogniteCore",
                    "version": "v1",
                    "viewExternalIds": ["CogniteAsset"],
                }
            ],
            "instanceSpaces": {"type": "all"},
        }

        # This is a valid data for the queryKnowledgeGraph tool
        data = {
            "name": "test_tool",
            "type": "queryKnowledgeGraph",
            "description": "A test tool",
            "configuration": query_knowledge_graph_configuration,
        }

        tool = AgentTool._load(data)
        assert tool.name == "test_tool"
        assert tool.type == "queryKnowledgeGraph"
        assert tool.description == "A test tool"
        assert tool.configuration == query_knowledge_graph_configuration

        dumped = tool.dump(camel_case=True)
        assert data == dumped

    def test_load_dump_no_configuration(self) -> None:
        data = {
            "name": "test_tool",
            "type": "test_type",
            "description": "A test tool",
        }

        tool = AgentTool._load(data)
        assert tool.name == "test_tool"
        assert tool.type == "test_type"
        assert tool.description == "A test tool"
        assert tool.configuration is None

        dumped = tool.dump(camel_case=True)
        assert data == dumped

    def test_as_write(self) -> None:
        tool = AgentTool(
            name="test_tool",
            type="test_type",
            description="A test tool",
            configuration={"key": "value"},
        )

        write_tool = tool.as_write()
        assert isinstance(write_tool, AgentToolUpsert)
        assert write_tool.name == tool.name
        assert write_tool.type == tool.type
        assert write_tool.description == tool.description
        assert write_tool.configuration == tool.configuration


class TestAgentToolList:
    def test_as_write(self) -> None:
        tools = [
            AgentTool(name="tool1", type="type1", description="desc1"),
            AgentTool(name="tool2", type="type2", description="desc2"),
        ]
        tool_list = AgentToolList(tools)

        write_list = tool_list.as_write()
        assert isinstance(write_list, AgentToolUpsertList)
        assert len(write_list) == 2
        assert all(isinstance(tool, AgentToolUpsert) for tool in write_list)
        assert write_list[0].name == "tool1"
        assert write_list[1].name == "tool2"
