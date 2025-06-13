from __future__ import annotations

from typing import cast
from unittest.mock import MagicMock

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.agents import Agent, AgentList, AgentWrite


@pytest.fixture
def agent_response_body() -> dict:
    return {
        "items": [
            {
                "externalId": "agent_1",
                "name": "Agent 1",
                "description": "Description 1",
                "createdTime": 1234567890,
                "id": 1,
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
def mock_agent_apply_response(rsps: MagicMock, cognite_client: CogniteClient, agent_response_body: dict) -> MagicMock:
    url = cognite_client.agents._get_base_url_with_base_path() + cognite_client.agents._RESOURCE_PATH
    rsps.add(rsps.POST, url, status=200, json=agent_response_body)
    yield rsps


class TestAgentsAPIUnit:
    def test_retrieve_single(self, cognite_client: CogniteClient, mock_agent_retrieve_response: MagicMock) -> None:
        retrieved_agent = cognite_client.agents.retrieve("agent_1")
        assert isinstance(retrieved_agent, Agent)
        assert retrieved_agent.external_id == "agent_1"

    def test_retrieve_multiple(self, cognite_client: CogniteClient, mock_agent_retrieve_response: MagicMock) -> None:
        retrieved_agents = cognite_client.agents.retrieve(["agent_1"])
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

    def test_apply(self, cognite_client: CogniteClient, mock_agent_apply_response: MagicMock) -> None:
        agent_write = AgentWrite(external_id="agent_1", name="Agent 1")
        created_agent = cognite_client.agents.apply(agent_write)
        assert isinstance(created_agent, Agent)
        assert created_agent.external_id == "agent_1"
        assert cast(str, mock_agent_apply_response.calls[-1].request.url).endswith(cognite_client.agents._RESOURCE_PATH)
