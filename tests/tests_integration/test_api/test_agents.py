from __future__ import annotations

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.agents import (
    Agent,
    AgentChatResponse,
    AgentList,
    AgentUpsert,
    AskDocumentAgentToolUpsert,
    DataModelInfo,
    Message,
    QueryKnowledgeGraphAgentToolConfiguration,
    QueryKnowledgeGraphAgentToolUpsert,
    QueryTimeSeriesDatapointsAgentToolUpsert,
    SummarizeDocumentAgentToolUpsert,
)
from cognite.client.data_classes.agents.agent_tools import InstanceSpaces
from cognite.client.exceptions import CogniteNotFoundError
from cognite.client.utils._text import random_string


@pytest.fixture(scope="session")
def permanent_agent(cognite_client: CogniteClient) -> Agent:
    """Fixture for a permanent agent that should not be deleted."""
    agent = AgentUpsert(
        external_id="cognite_sdk_test_permanent_agent",
        name="Permanent Agent",
        description="This agent is used for permanent testing purposes.",
        instructions="Instructions for the permanent agent.",
        # NOTE: This model will eventually be retired and need updating again. To list native models
        # sorted by retirement date (pick one near the top), use the (alpha) services availability endpoint:
        #   res = client.get(
        #       f"/api/v1/projects/{client.config.project}/ai/services/availability",
        #       headers={"cdf-version": "20230101-alpha"},
        #   ).json()
        #   sorted(
        #       [model for model in res["languageModels"] if model["native"]],
        #       key=lambda model: model["earliestRetirementDate"],
        #       reverse=True,
        #   )
        # Or pick one manually from:
        # https://docs.cognite.com/cdf/atlas_ai/references/atlas_ai_agent_language_models
        model="gcp/claude-4.5-haiku",  # Note: Should be a fast and cheap like haiku or flash
        tools=[
            QueryKnowledgeGraphAgentToolUpsert(
                name="find_assets",
                description="Use this tool to find assets in the knowledge graph",
                configuration=QueryKnowledgeGraphAgentToolConfiguration(
                    data_models=[
                        DataModelInfo(
                            space="cdf_cdm",
                            external_id="CogniteCore",
                            version="v1",
                            view_external_ids=["CogniteAsset"],
                        )
                    ],
                    instance_spaces=None,
                    version="v2",
                ),
            )
        ],
    )
    existing = cognite_client.agents.retrieve(external_ids=agent.external_id, ignore_unknown_ids=True)
    if existing and existing.model == agent.model:
        return existing
    return cognite_client.agents.upsert(agent)


class TestAgentsAPI:
    def test_list_agents(self, cognite_client: CogniteClient, permanent_agent: Agent) -> None:
        agent_list = cognite_client.agents.list()
        assert isinstance(agent_list, AgentList)
        assert len(agent_list) > 0
        agent_external_ids = {a.external_id for a in agent_list}
        assert permanent_agent.external_id in agent_external_ids

    def test_create_retrieve_update_delete_agent(self, cognite_client: CogniteClient) -> None:
        agent = AgentUpsert(
            external_id=f"test_minimal_agent_{random_string(10)}",
            name="Minimal Test Agent",
            description="A minimal test agent without tools.",
            model="gcp/claude-4.5-haiku",
            instructions="This is a test agent for integration testing.",
            tools=[
                QueryKnowledgeGraphAgentToolUpsert(
                    name="find_assets",
                    description="Use this tool to find assets in the knowledge graph",
                    configuration=QueryKnowledgeGraphAgentToolConfiguration(
                        data_models=[
                            DataModelInfo(
                                space="cdf_cdm",
                                external_id="CogniteCore",
                                version="v1",
                                view_external_ids=["CogniteAsset"],
                            )
                        ],
                        instance_spaces=InstanceSpaces(type="all"),
                        version="v2",
                    ),
                ),
                SummarizeDocumentAgentToolUpsert(
                    name="summarize_document",
                    description="Use this tool to summarize documents",
                ),
                AskDocumentAgentToolUpsert(
                    name="ask_document",
                    description="Use this tool to ask questions about documents",
                ),
                QueryTimeSeriesDatapointsAgentToolUpsert(
                    name="query_timeseries",
                    description="Use this tool to query time series data points",
                ),
            ],
        )

        created_agent: Agent | None = None
        try:
            created_agent = cognite_client.agents.upsert(agent)
            agent.runtime_version = created_agent.runtime_version
            assert created_agent.as_write() == agent

            retrieved_agent = cognite_client.agents.retrieve(external_ids=created_agent.external_id)
            assert retrieved_agent is not None

            update = AgentUpsert._load(agent.dump())
            update.description = "Updated description"
            updated_agent = cognite_client.agents.upsert(update)
            assert updated_agent.description == "Updated description"

            cognite_client.agents.delete(external_ids=created_agent.external_id)

            assert (
                cognite_client.agents.retrieve(external_ids=created_agent.external_id, ignore_unknown_ids=True) is None
            )
        finally:
            if created_agent:
                cognite_client.agents.delete(external_ids=agent.external_id, ignore_unknown_ids=True)

    def test_retrieve_multiple_agents(self, cognite_client: CogniteClient, permanent_agent: Agent) -> None:
        results = cognite_client.agents.retrieve(
            [permanent_agent.external_id, "notExistingAgent"], ignore_unknown_ids=True
        )
        assert isinstance(results, AgentList)
        assert len(results) == 1
        assert results[0].external_id == permanent_agent.external_id

    def test_retrieve_non_existing_agent_raise_error(self, cognite_client: CogniteClient) -> None:
        """Test retrieving a non-existing agent with error raising."""
        with pytest.raises(CogniteNotFoundError):
            cognite_client.agents.retrieve(external_ids=["notExistingAgent"], ignore_unknown_ids=False)

    def test_chat_minimal(self, cognite_client: CogniteClient, permanent_agent: Agent) -> None:
        """Minimal happy-path chat call to verify the endpoint works end-to-end."""
        response = cognite_client.agents.chat(
            agent_external_id=permanent_agent.external_id,
            messages=Message("Hello from SDK integration test"),
        )

        assert isinstance(response, AgentChatResponse)
        assert response.agent_external_id == permanent_agent.external_id
        assert isinstance(response.type, str) and len(response.type) > 0
        assert response.messages is not None
