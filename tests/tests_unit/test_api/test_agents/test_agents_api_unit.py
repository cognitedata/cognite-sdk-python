from __future__ import annotations

from typing import cast
from unittest.mock import MagicMock

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.agents import Agent, AgentList, AgentUpsert
from cognite.client.data_classes.agents.agent_tools import (
    DataModelInfo,
    InstanceSpaces,
    QueryKnowledgeGraphAgentToolConfiguration,
    QueryKnowledgeGraphAgentToolUpsert,
)


@pytest.fixture
def agent_response_body() -> dict:
    return {
        "items": [
            {
                "externalId": "agent_1",
                "name": "Agent 1",
                "description": "Description 1",
                "instructions": "Instructions 1",
                "model": "vendor/model_1",
                "tools": [
                    {
                        "name": "tool_1",
                        "type": "queryKnowledgeGraph",
                        "description": "Description 1",
                        "configuration": {
                            "dataModels": [
                                {
                                    "space": "space1",
                                    "externalId": "dmxid1",
                                    "version": "v1",
                                    "viewExternalIds": ["view1"],
                                }
                            ],
                            "instanceSpaces": {"type": "all"},
                        },
                    }
                ],
                "createdTime": 1234567890,
                "lastUpdatedTime": 1234567890,
                "ownerId": "owner_1!",
            }
        ]
    }


@pytest.fixture
def mock_agent_retrieve_response(
    rsps: MagicMock, cognite_client: CogniteClient, agent_response_body: dict
) -> MagicMock:
    url = cognite_client.agents._get_base_url_with_base_path() + cognite_client.agents._RESOURCE_PATH
    rsps.add(rsps.POST, url + "/byids", status=200, json=agent_response_body)
    yield rsps


@pytest.fixture
def mock_agent_list_response(rsps: MagicMock, cognite_client: CogniteClient, agent_response_body: dict) -> MagicMock:
    url = cognite_client.agents._get_base_url_with_base_path() + cognite_client.agents._RESOURCE_PATH
    rsps.add(rsps.GET, url, status=200, json=agent_response_body)
    yield rsps


@pytest.fixture
def mock_agent_delete_response(rsps: MagicMock, cognite_client: CogniteClient) -> MagicMock:
    url = cognite_client.agents._get_base_url_with_base_path() + cognite_client.agents._RESOURCE_PATH
    rsps.add(rsps.POST, url + "/delete", status=200, json={})
    yield rsps


@pytest.fixture
def mock_agent_upsert_response(rsps: MagicMock, cognite_client: CogniteClient, agent_response_body: dict) -> MagicMock:
    url = cognite_client.agents._get_base_url_with_base_path() + cognite_client.agents._RESOURCE_PATH
    rsps.add(rsps.POST, url, status=200, json=agent_response_body)
    yield rsps


class TestAgentsAPIUnit:
    def test_retrieve_single(self, cognite_client: CogniteClient, mock_agent_retrieve_response: MagicMock) -> None:
        retrieved_agent = cognite_client.agents.retrieve("agent_1")
        assert isinstance(retrieved_agent, Agent)
        assert retrieved_agent.external_id == "agent_1"

    def test_retrieve_multiple(self, cognite_client: CogniteClient, mock_agent_retrieve_response: MagicMock) -> None:
        retrieved_agents = cognite_client.agents.retrieve(["agent_1", "agent_2"])
        assert isinstance(retrieved_agents, AgentList)
        assert len(retrieved_agents) == 1
        assert retrieved_agents[0].external_id == "agent_1"

    def test_list(self, cognite_client: CogniteClient, mock_agent_list_response: MagicMock) -> None:
        agent_list = cognite_client.agents.list()
        assert isinstance(agent_list, AgentList)
        assert len(agent_list) == 1
        assert agent_list[0].external_id == "agent_1"

    def test_delete(self, cognite_client: CogniteClient, mock_agent_delete_response: MagicMock) -> None:
        cognite_client.agents.delete("agent_1")
        assert mock_agent_delete_response.calls[-1].request.url.endswith(
            cognite_client.agents._RESOURCE_PATH + "/delete"
        )

    def test_delete_multiple(self, cognite_client: CogniteClient, mock_agent_delete_response: MagicMock) -> None:
        cognite_client.agents.delete(["agent_1", "agent_2"])
        assert mock_agent_delete_response.calls[-1].request.url.endswith(
            cognite_client.agents._RESOURCE_PATH + "/delete"
        )

    def test_upsert_minimal(self, cognite_client: CogniteClient, mock_agent_upsert_response: MagicMock) -> None:
        agent_write = AgentUpsert(external_id="agent_1", name="Agent 1")
        created_agent = cognite_client.agents.upsert(agent_write)
        assert isinstance(created_agent, Agent)
        assert created_agent.external_id == "agent_1"
        assert cast(str, mock_agent_upsert_response.calls[-1].request.url).endswith(
            cognite_client.agents._RESOURCE_PATH
        )

    def test_upsert_full(self, cognite_client: CogniteClient, mock_agent_upsert_response: MagicMock) -> None:
        agent_write = AgentUpsert(
            external_id="agent_1",
            name="Agent 1",
            description="Description 1",
            instructions="Instructions 1",
            model="vendor/model_1",
            tools=[
                QueryKnowledgeGraphAgentToolUpsert(
                    name="tool_1",
                    description="Description 1",
                    configuration=QueryKnowledgeGraphAgentToolConfiguration(
                        data_models=[
                            DataModelInfo(
                                space="space1", external_id="dmxid1", version="v1", view_external_ids=["view1"]
                            )
                        ],
                        instance_spaces=InstanceSpaces(type="all"),
                    ),
                )
            ],
        )
        created_agent = cognite_client.agents.upsert(agent_write)
        assert isinstance(created_agent, Agent)
        assert created_agent.external_id == "agent_1"
        assert cast(str, mock_agent_upsert_response.calls[-1].request.url).endswith(
            cognite_client.agents._RESOURCE_PATH
        )

    def test_upsert_multiple(self, cognite_client: CogniteClient, mock_agent_upsert_response: MagicMock) -> None:
        agents_write = [
            AgentUpsert(external_id="agent_1", name="Agent 1"),
            AgentUpsert(external_id="agent_2", name="Agent 2"),
        ]
        created_agents = cognite_client.agents.upsert(agents_write)
        assert isinstance(created_agents, AgentList)
        # Since _CREATE_LIMIT is 1 for agents, each agent is processed individually
        # and the mock response (which has 1 agent) is returned for each request
        assert len(created_agents) >= 1
        assert created_agents[0].external_id == "agent_1"
        assert cast(str, mock_agent_upsert_response.calls[-1].request.url).endswith(
            cognite_client.agents._RESOURCE_PATH
        )
