from __future__ import annotations

from collections.abc import Iterator

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import CogniteClient
from cognite.client.data_classes.agents import Agent, AgentList, AgentUpsert
from cognite.client.data_classes.agents.agent_tools import (
    DataModelInfo,
    InstanceSpaces,
    QueryKnowledgeGraphAgentToolConfiguration,
    QueryKnowledgeGraphAgentToolUpsert,
)
from tests.utils import get_url


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
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, agent_response_body: dict
) -> Iterator[HTTPXMock]:
    url = get_url(cognite_client.agents) + cognite_client.agents._RESOURCE_PATH
    httpx_mock.add_response(method="POST", url=url + "/byids", status_code=200, json=agent_response_body)
    yield httpx_mock


@pytest.fixture
def mock_agent_list_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, agent_response_body: dict
) -> Iterator[HTTPXMock]:
    url = get_url(cognite_client.agents) + cognite_client.agents._RESOURCE_PATH
    httpx_mock.add_response(method="GET", url=url, status_code=200, json=agent_response_body)
    yield httpx_mock


@pytest.fixture
def mock_agent_delete_response(httpx_mock: HTTPXMock, cognite_client: CogniteClient) -> Iterator[HTTPXMock]:
    url = get_url(cognite_client.agents) + cognite_client.agents._RESOURCE_PATH
    httpx_mock.add_response(method="POST", url=url + "/delete", status_code=200, json={})
    yield httpx_mock


@pytest.fixture
def mock_agent_upsert_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, agent_response_body: dict
) -> Iterator[HTTPXMock]:
    url = get_url(cognite_client.agents) + cognite_client.agents._RESOURCE_PATH
    httpx_mock.add_response(method="POST", url=url, status_code=200, json=agent_response_body)
    yield httpx_mock


class TestAgentsAPI:
    @pytest.mark.usefixtures("mock_agent_retrieve_response")
    def test_retrieve_single(self, cognite_client: CogniteClient) -> None:
        retrieved_agent = cognite_client.agents.retrieve("agent_1")
        assert isinstance(retrieved_agent, Agent)
        assert retrieved_agent.external_id == "agent_1"

    @pytest.mark.usefixtures("mock_agent_retrieve_response")
    def test_retrieve_multiple(self, cognite_client: CogniteClient) -> None:
        retrieved_agents = cognite_client.agents.retrieve(["agent_1", "agent_2"])
        assert isinstance(retrieved_agents, AgentList)
        assert len(retrieved_agents) == 1
        assert retrieved_agents[0].external_id == "agent_1"

    @pytest.mark.usefixtures("mock_agent_list_response")
    def test_list(self, cognite_client: CogniteClient) -> None:
        agent_list = cognite_client.agents.list()
        assert isinstance(agent_list, AgentList)
        assert len(agent_list) == 1
        assert agent_list[0].external_id == "agent_1"

    def test_delete(self, cognite_client: CogniteClient, mock_agent_delete_response: HTTPXMock) -> None:
        cognite_client.agents.delete("agent_1")
        url = str(mock_agent_delete_response.get_requests()[-1].url)
        assert url.endswith(cognite_client.agents._RESOURCE_PATH + "/delete")

    def test_delete_multiple(self, cognite_client: CogniteClient, mock_agent_delete_response: HTTPXMock) -> None:
        # Multiple deletes, so we need an additional response:
        url = get_url(cognite_client.agents) + cognite_client.agents._RESOURCE_PATH
        mock_agent_delete_response.add_response(method="POST", url=url + "/delete", status_code=200, json={})

        cognite_client.agents.delete(["agent_1", "agent_2"])
        url = str(mock_agent_delete_response.get_requests()[-1].url)
        assert url.endswith(cognite_client.agents._RESOURCE_PATH + "/delete")

    def test_upsert_minimal(self, cognite_client: CogniteClient, mock_agent_upsert_response: HTTPXMock) -> None:
        agent_write = AgentUpsert(external_id="agent_1", name="Agent 1")
        created_agent = cognite_client.agents.upsert(agent_write)
        assert isinstance(created_agent, Agent)
        assert created_agent.external_id == "agent_1"
        url = str(mock_agent_upsert_response.get_requests()[-1].url)
        assert url.endswith(cognite_client.agents._RESOURCE_PATH)

    def test_upsert_full(self, cognite_client: CogniteClient, mock_agent_upsert_response: HTTPXMock) -> None:
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
        url = str(mock_agent_upsert_response.get_requests()[-1].url)
        assert url.endswith(cognite_client.agents._RESOURCE_PATH)

    def test_upsert_multiple(
        self, cognite_client: CogniteClient, mock_agent_upsert_response: HTTPXMock, agent_response_body: dict
    ) -> None:
        # Since _CREATE_LIMIT is 1 for agents, each agent is processed individually
        # and the mock response (which has 1 agent) is returned for each request.
        # So we need to add an additional mock response:
        agent_response_body["items"][0]["externalId"] = "agent_2"
        agent_response_body["items"][0]["name"] = "Agent 2"
        url = get_url(cognite_client.agents) + cognite_client.agents._RESOURCE_PATH
        mock_agent_upsert_response.add_response(method="POST", url=url, status_code=200, json=agent_response_body)

        agents_write = [
            AgentUpsert(external_id="agent_1", name="Agent 1"),
            AgentUpsert(external_id="agent_2", name="Agent 2"),
        ]
        created_agents = cognite_client.agents.upsert(agents_write)
        assert isinstance(created_agents, AgentList)
        assert len(created_agents) >= 1
        assert {agent.external_id for agent in created_agents} == {"agent_1", "agent_2"}
        url = str(mock_agent_upsert_response.get_requests()[-1].url)
        assert url.endswith(cognite_client.agents._RESOURCE_PATH)

    def test_upsert_ensure_retry(
        self, cognite_client: CogniteClient, httpx_mock: HTTPXMock, agent_response_body: dict[str, object]
    ) -> None:
        from cognite.client.config import global_config

        with pytest.MonkeyPatch.context() as mp:
            # For unit tests, we have turned off retries so for this test we must allow 1 retry attempt:
            mp.setattr(global_config, "max_retries", 1)

            # Simulate a 503 response to ensure retry logic is triggered. Note we do not use 429 as that is always
            # retried, while 503 is only retried if the endpoint is marked as retryable.
            url = get_url(cognite_client.agents) + cognite_client.agents._RESOURCE_PATH
            httpx_mock.add_response(method="POST", url=url, status_code=503, json={"message": "Connection refused"})
            httpx_mock.add_response(method="POST", url=url, status_code=200, json=agent_response_body)

            created = cognite_client.agents.upsert(AgentUpsert(external_id="agent_1", name="Agent 1"))

            assert isinstance(created, Agent)
