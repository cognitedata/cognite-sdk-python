from __future__ import annotations

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.agents import Agent, AgentApply, AgentList, AgentTool
from cognite.client.exceptions import CogniteNotFoundError
from cognite.client.utils._text import random_string


@pytest.fixture(scope="function")
def new_agent(cognite_client: CogniteClient) -> Agent:
    agent = AgentApply(
        external_id=f"test_agent_{random_string(10)}",
        name="Test Agent",
        description="This is a test agent.",
        instructions="These are the instructions for the test agent.",
        model="azure/gpt-4o",
        tools=[
            AgentTool(
                name="test_tool",
                type="queryKnowledgeGraph",
                description="A test tool",
                configuration={
                    "dataModels": [
                        {
                            "space": "cdf_cdm",
                            "externalId": "CogniteCore",
                            "version": "v1",
                            "viewExternalIds": ["CogniteAsset"],
                        }
                    ]
                },
            ),
            AgentTool(name="test_tool2", type="summarizeDocument", description="A test tool"),
            AgentTool(name="test_tool3", type="askDocument", description="A test tool"),
            AgentTool(name="test_tool4", type="queryTimeSeriesDatapoints", description="A test tool"),
        ],
    )
    created_agent = cognite_client.agents.apply(agent)
    yield created_agent
    cognite_client.agents.delete(external_ids=created_agent.external_id, ignore_unknown_ids=True)


class TestAgentsAPI:
    def test_list_agents(self, cognite_client: CogniteClient, new_agent: Agent) -> None:
        agent_list = cognite_client.agents.list()
        assert isinstance(agent_list, AgentList)
        assert len(agent_list) > 0
        assert new_agent.external_id in [a.external_id for a in agent_list]

    def test_create_retrieve_and_delete_agent(self, cognite_client: CogniteClient) -> None:
        my_agent = AgentApply(
            external_id=f"my_new_agent_{random_string(10)}",
            name="My New Agent",
            description="This is part of the integration testing for the SDK.",
        )
        created_agent: Agent | None = None
        try:
            created_agent = cognite_client.agents.apply(my_agent)
            retrieved_agent = cognite_client.agents.retrieve(external_ids=my_agent.external_id)

            assert retrieved_agent is not None
            assert retrieved_agent.external_id == created_agent.external_id
            assert my_agent.name == retrieved_agent.name

            cognite_client.agents.delete(external_ids=my_agent.external_id, ignore_unknown_ids=True)
            assert cognite_client.agents.retrieve(external_ids=my_agent.external_id) is None
        finally:
            if created_agent:
                cognite_client.agents.delete(external_ids=my_agent.external_id, ignore_unknown_ids=True)

    def test_retrieve_multiple_agents(self, cognite_client: CogniteClient) -> None:
        agent1 = AgentApply(
            external_id=f"test_multi_agent_1_{random_string(10)}",
            name="Test Multi Agent 1",
        )
        agent2 = AgentApply(
            external_id=f"test_multi_agent_2_{random_string(10)}",
            name="Test Multi Agent 2",
        )
        created = cognite_client.agents.apply([agent1, agent2])
        try:
            retrieved_agents = cognite_client.agents.retrieve(external_ids=[a.external_id for a in created])
            assert isinstance(retrieved_agents, AgentList)
            assert len(retrieved_agents) == 2
            assert {a.external_id for a in retrieved_agents} == {a.external_id for a in created}
        finally:
            cognite_client.agents.delete(external_ids=[a.external_id for a in created], ignore_unknown_ids=True)

    def test_retrieve_non_existing_agent(self, cognite_client: CogniteClient) -> None:
        actual_retrieved = cognite_client.agents.retrieve(external_ids="notExistingAgent", ignore_unknown_ids=True)
        assert actual_retrieved is None

    def test_retrieve_non_existing_agent_raise_error(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(CogniteNotFoundError):
            cognite_client.agents.retrieve(external_ids=["notExistingAgent"], ignore_unknown_ids=False)

    def test_retrieve_some_existing_and_some_non_existing_agents_ignore_unknown(
        self, cognite_client: CogniteClient, new_agent: Agent
    ) -> None:
        retrieved = cognite_client.agents.retrieve(
            external_ids=[new_agent.external_id, "notExistingAgent"], ignore_unknown_ids=True
        )
        assert isinstance(retrieved, AgentList)
        assert len(retrieved) == 1
        assert retrieved[0].external_id == new_agent.external_id

    def test_retrieve_some_existing_and_some_non_existing_agents_raise_error(
        self, cognite_client: CogniteClient, new_agent: Agent
    ) -> None:
        with pytest.raises(CogniteNotFoundError) as e:
            cognite_client.agents.retrieve(
                external_ids=[new_agent.external_id, "notExistingAgent"], ignore_unknown_ids=False
            )
        assert {"externalId": "notExistingAgent"} in e.value.not_found
        assert cognite_client.agents.retrieve(external_ids=new_agent.external_id) is not None

    def test_delete_non_existing_agent(self, cognite_client: CogniteClient) -> None:
        cognite_client.agents.delete(external_ids="notExistingAgent", ignore_unknown_ids=True)
        # No error should be raised

    def test_delete_non_existing_agent_raise_error(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(CogniteNotFoundError):
            cognite_client.agents.delete(external_ids="notExistingAgent", ignore_unknown_ids=False)

    def test_delete_some_existing_and_some_non_existing_agents_ignore_unknown(
        self, cognite_client: CogniteClient, new_agent: Agent
    ) -> None:
        cognite_client.agents.delete(external_ids=[new_agent.external_id, "notExistingAgent"], ignore_unknown_ids=True)
        assert cognite_client.agents.retrieve(external_ids=new_agent.external_id) is None

    def test_delete_some_existing_and_some_non_existing_agents_raise_error(
        self, cognite_client: CogniteClient, new_agent: Agent
    ) -> None:
        with pytest.raises(CogniteNotFoundError) as e:
            cognite_client.agents.delete(
                external_ids=[new_agent.external_id, "notExistingAgent"], ignore_unknown_ids=False
            )
        assert cognite_client.agents.retrieve(external_ids=new_agent.external_id) is None
        assert {"externalId": "notExistingAgent"} in e.value.not_found
