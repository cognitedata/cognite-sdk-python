from __future__ import annotations

import pytest

from cognite.client.data_classes.agents.agent_tools import AgentTool, AgentToolApply
from cognite.client.data_classes.agents.agents import Agent, AgentApply, AgentApplyList, AgentList


class TestAgent:
    def test_load_dump(self) -> None:
        data = {
            "externalId": "test_agent",
            "name": "Test Agent",
            "description": "A test agent",
            "instructions": "Test instructions",
            "model": "gpt-4",
            "tools": [
                {
                    "name": "test_tool",
                    "type": "test_type",
                    "description": "A test tool",
                }
            ],
            "createdTime": 667008000000,
            "lastUpdatedTime": 667008000001,
            "ownerId": "this!1sMy@ID",
        }

        agent = Agent._load(data)
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
        assert data == dumped

    def test_load_dump_minimal(self) -> None:
        data = {
            "externalId": "test_agent",
            "name": "Test Agent",
        }

        agent = Agent._load(data)
        assert agent.external_id == "test_agent"
        assert agent.name == "Test Agent"
        assert agent.description is None
        assert agent.instructions is None
        assert agent.model is None
        assert agent.tools is None

        dumped = agent.dump(camel_case=True)
        assert data == dumped

    def test_tools_handling(self) -> None:
        # Test with no tools
        agent = Agent(external_id="test_agent", name="Test Agent")
        assert agent.tools is None

        # Test with an empty list of tools
        agent = Agent(external_id="test_agent", name="Test Agent", tools=[])
        assert agent.tools == []

        # Test with list of tools
        tools_list = [
            AgentTool(name="test_tool1", type="test_type", description="A test tool"),
            AgentTool(name="test_tool2", type="test_type", description="Another test tool"),
        ]
        agent = Agent(external_id="test_agent", name="Test Agent", tools=tools_list)
        assert len(agent.tools) == 2
        assert all(isinstance(tool, AgentTool) for tool in agent.tools)
        assert agent.tools[0].name == "test_tool1"
        assert agent.tools[1].name == "test_tool2"

        # Test with empty list of tools
        agent = Agent(external_id="test_agent", name="Test Agent", tools=[])
        assert agent.tools == []

    def test_post_init_tools_validation(self) -> None:
        # Test with invalid tool type
        with pytest.raises(TypeError):
            Agent(
                external_id="test_agent",
                name="Test Agent",
                tools={"name": "test_tool", "type": "test_type", "description": "A test tool"},
            )

    def test_as_apply(self) -> None:
        agent = Agent(
            external_id="test_agent",
            name="Test Agent",
            description="A test agent",
            instructions="Test instructions",
            model="gpt-4",
            tools=[AgentTool(name="test_tool", type="test_type", description="A test tool")],
        )

        write_agent = agent.as_apply()
        assert isinstance(write_agent, AgentApply)
        assert write_agent.external_id == agent.external_id
        assert write_agent.name == agent.name
        assert write_agent.description == agent.description
        assert write_agent.instructions == agent.instructions
        assert write_agent.model == agent.model
        assert len(write_agent.tools) == 1
        assert isinstance(write_agent.tools[0], AgentToolApply)


class TestAgentList:
    def test_as_apply(self) -> None:
        agents = [
            Agent(external_id="agent1", name="Agent 1"),
            Agent(external_id="agent2", name="Agent 2"),
        ]
        agent_list = AgentList(agents)

        write_list = agent_list.as_apply()
        assert isinstance(write_list, AgentApplyList)
        assert len(write_list) == 2
        assert all(isinstance(agent, AgentApply) for agent in write_list)
        assert write_list[0].external_id == "agent1"
        assert write_list[1].external_id == "agent2"
