from __future__ import annotations

import pytest

from cognite.client.data_classes.agents.agent_tools import (
    AgentTool,
    AgentToolUpsert,
    AskDocumentAgentTool,
    SummarizeDocumentAgentTool,
)
from cognite.client.data_classes.agents.agents import Agent, AgentList, AgentUpsert, AgentUpsertList


@pytest.fixture
def agent_upsert_dump() -> dict:
    return {
        "externalId": "test_agent",
        "name": "Test Agent",
        "description": "A test agent",
        "instructions": "Test instructions",
        "model": "gpt-4",
        "tools": [
            {  # Valid queryKnowledgeGraph tool
                "name": "test_tool",
                "type": "queryKnowledgeGraph",
                "description": "A test tool",
                "configuration": {
                    "dataModels": [
                        {
                            "space": "cdf_cdm",
                            "externalId": "CogniteCore",
                            "version": "v1",
                            "viewExternalIds": ["CogniteAsset"],
                        }
                    ],
                    "instanceSpaces": {"type": "all"},
                },
            }
        ],
    }


@pytest.fixture
def agent_dump(agent_upsert_dump: dict) -> dict:
    return {
        **agent_upsert_dump,
        "createdTime": 667008000000,
        "lastUpdatedTime": 667008000001,
        "ownerId": "this!1sMy@ID",
    }


@pytest.fixture
def agent_minimal_dump() -> dict:
    return {
        "externalId": "test_agent",
        "name": "Test Agent",
    }


class TestAgentUpsert:
    def test_load_dump(self, agent_upsert_dump: dict) -> None:
        agent = AgentUpsert._load(agent_upsert_dump)
        assert agent.external_id == "test_agent"
        assert agent.name == "Test Agent"
        assert agent.description == "A test agent"
        assert agent.instructions == "Test instructions"
        assert agent.model == "gpt-4"
        assert len(agent.tools) == 1
        assert isinstance(agent.tools[0], AgentToolUpsert)
        assert agent.tools[0].name == "test_tool"

        dumped = agent.dump(camel_case=True)
        assert agent_upsert_dump == dumped

    def test_as_write(self) -> None:
        agent_upsert = AgentUpsert(
            external_id="test_agent",
            name="Test Agent",
        )
        assert agent_upsert is agent_upsert.as_write()


class TestAgent:
    def test_load_dump(self, agent_dump: dict) -> None:
        agent = Agent._load(agent_dump)
        assert agent.external_id == "test_agent"
        assert agent.name == "Test Agent"
        assert agent.description == "A test agent"
        assert agent.instructions == "Test instructions"
        assert agent.model == "gpt-4"
        assert len(agent.tools) == 1
        assert isinstance(agent.tools[0], AgentTool)
        assert agent.tools[0].name == "test_tool"
        assert agent.created_time == 667008000000
        assert agent.last_updated_time == 667008000001
        assert agent.owner_id == "this!1sMy@ID"

        dumped = agent.dump(camel_case=True)
        assert agent_dump == dumped

    def test_load_dump_minimal(self, agent_minimal_dump: dict) -> None:
        agent = Agent._load(agent_minimal_dump)
        assert agent.external_id == "test_agent"
        assert agent.name == "Test Agent"
        assert agent.description is None
        assert agent.instructions is None
        assert agent.model is None
        assert agent.tools is None

        dumped = agent.dump(camel_case=True)
        assert agent_minimal_dump == dumped

    def test_tools_handling(self) -> None:
        # Test with no tools
        agent = Agent(external_id="test_agent", name="Test Agent")
        assert agent.tools is None

        # Test with an empty list of tools
        agent = Agent(external_id="test_agent", name="Test Agent", tools=[])
        assert agent.tools == []

        # Test with list of tools
        tools_list = [
            SummarizeDocumentAgentTool(name="test_tool1", description="A test tool"),
            AskDocumentAgentTool(name="test_tool2", description="Another test tool"),
        ]
        agent = Agent(external_id="test_agent", name="Test Agent", tools=tools_list)
        assert len(agent.tools) == 2
        assert all(isinstance(tool, AgentTool) for tool in agent.tools)
        assert agent.tools[0].name == "test_tool1"
        assert agent.tools[1].name == "test_tool2"

        # Test with empty list of tools
        agent = Agent(external_id="test_agent", name="Test Agent", tools=[])
        assert agent.tools == []

    def test_agent_with_empty_tools_list(self) -> None:
        """Test agent creation with empty tools list."""
        agent = Agent(external_id="test", name="test", tools=[])
        assert agent.tools == []
        assert not agent.tools  # Should be falsy

    def test_agent_with_none_tools(self) -> None:
        """Test agent creation with None tools."""
        agent = Agent(external_id="test", name="test", tools=None)
        assert agent.tools is None

    def test_post_init_tools_validation(self) -> None:
        # Test with invalid tool type
        with pytest.raises(TypeError):
            Agent(
                external_id="test_agent",
                name="Test Agent",
                tools=[{"name": "test_tool", "type": "test_type", "description": "A test tool"}],
            )

    def test_as_write(self) -> None:
        agent = Agent(
            external_id="test_agent",
            name="Test Agent",
            description="A test agent",
            instructions="Test instructions",
            model="gpt-4",
            tools=[SummarizeDocumentAgentTool(name="test_tool", description="A test tool")],
        )

        write_agent = agent.as_write()
        assert isinstance(write_agent, AgentUpsert)
        assert write_agent.external_id == agent.external_id
        assert write_agent.name == agent.name
        assert write_agent.description == agent.description
        assert write_agent.instructions == agent.instructions
        assert write_agent.model == agent.model
        assert len(write_agent.tools) == 1
        assert isinstance(write_agent.tools[0], AgentToolUpsert)
        assert write_agent.tools[0].name == "test_tool"


class TestAgentList:
    def test_as_write(self) -> None:
        agents = [
            Agent(external_id="agent1", name="Agent 1"),
            Agent(external_id="agent2", name="Agent 2"),
        ]
        agent_list = AgentList(agents)

        write_list = agent_list.as_write()
        assert isinstance(write_list, AgentUpsertList)
        assert len(write_list) == 2
        assert all(isinstance(agent, AgentUpsert) for agent in write_list)
        assert write_list[0].external_id == "agent1"
        assert write_list[1].external_id == "agent2"


def test_load_with_missing_required_fields():
    """Test that loading fails gracefully with missing required fields."""
    with pytest.raises(KeyError):
        AgentTool._load({"type": "askDocument"})  # Missing name and description


def test_load_with_invalid_tool_type():
    """Test handling of completely invalid tool data."""
    with pytest.raises(KeyError):
        AgentTool._load({"name": "test", "description": "test"})  # Missing type
