from __future__ import annotations

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.agents import (
    Agent,
    AgentList,
    AgentUpsert,
    AskDocumentAgentToolUpsert,
    DataModelInfo,
    QueryKnowledgeGraphAgentToolConfiguration,
    QueryKnowledgeGraphAgentToolUpsert,
    QueryTimeSeriesDatapointsAgentToolUpsert,
    SummarizeDocumentAgentToolUpsert,
)
from cognite.client.exceptions import CogniteNotFoundError
from cognite.client.utils._text import random_string


# Tool fixtures for different agent tool types
@pytest.fixture
def query_knowledge_graph_tool() -> QueryKnowledgeGraphAgentToolUpsert:
    """Fixture for QueryKnowledgeGraph tool with proper configuration."""
    return QueryKnowledgeGraphAgentToolUpsert(
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
            ]
        ),
    )


@pytest.fixture
def summarize_document_tool() -> SummarizeDocumentAgentToolUpsert:
    """Fixture for SummarizeDocument tool."""
    return SummarizeDocumentAgentToolUpsert(
        name="summarize_document",
        description="Use this tool to summarize documents",
    )


@pytest.fixture
def ask_document_tool() -> AskDocumentAgentToolUpsert:
    """Fixture for AskDocument tool."""
    return AskDocumentAgentToolUpsert(
        name="ask_document",
        description="Use this tool to ask questions about documents",
    )


@pytest.fixture
def query_timeseries_tool() -> QueryTimeSeriesDatapointsAgentToolUpsert:
    """Fixture for QueryTimeSeriesDatapoints tool."""
    return QueryTimeSeriesDatapointsAgentToolUpsert(
        name="query_timeseries",
        description="Use this tool to query time series data points",
    )


@pytest.fixture
def basic_agent_config() -> dict:
    """Fixture for basic agent configuration."""
    return {
        "name": "Test Agent",
        "description": "This is a test agent for integration testing.",
        "instructions": "These are the instructions for the test agent.",
        "model": "azure/gpt-4o",
    }


@pytest.fixture
def minimal_agent_config() -> dict:
    """Fixture for minimal agent configuration."""
    return {
        "name": "Minimal Test Agent",
        "description": "A minimal test agent.",
    }


@pytest.fixture
def agent_with_all_tools(
    basic_agent_config: dict,
    query_knowledge_graph_tool: QueryKnowledgeGraphAgentToolUpsert,
    summarize_document_tool: SummarizeDocumentAgentToolUpsert,
    ask_document_tool: AskDocumentAgentToolUpsert,
    query_timeseries_tool: QueryTimeSeriesDatapointsAgentToolUpsert,
) -> AgentUpsert:
    """Fixture for an agent with all tool types."""
    return AgentUpsert(
        external_id=f"test_agent_all_tools_{random_string(10)}",
        **basic_agent_config,
        tools=[
            query_knowledge_graph_tool,
            summarize_document_tool,
            ask_document_tool,
            query_timeseries_tool,
        ],
    )


@pytest.fixture
def agent_with_kg_tool(
    basic_agent_config: dict,
    query_knowledge_graph_tool: QueryKnowledgeGraphAgentToolUpsert,
) -> AgentUpsert:
    """Fixture for an agent with only knowledge graph tool."""
    return AgentUpsert(
        external_id=f"test_agent_kg_{random_string(10)}",
        **basic_agent_config,
        tools=[query_knowledge_graph_tool],
    )


@pytest.fixture
def minimal_agent(minimal_agent_config: dict) -> AgentUpsert:
    """Fixture for a minimal agent without tools."""
    return AgentUpsert(
        external_id=f"test_minimal_agent_{random_string(10)}",
        **minimal_agent_config,
    )


@pytest.fixture
def created_agent(cognite_client: CogniteClient, agent_with_all_tools: AgentUpsert) -> Agent:
    """Fixture that creates an agent and cleans it up after the test."""
    created_agent = cognite_client.agents.upsert(agent_with_all_tools)
    yield created_agent
    cognite_client.agents.delete(external_ids=created_agent.external_id, ignore_unknown_ids=True)


class TestAgentsAPI:
    """Test class for Agents API with comprehensive coverage."""

    def test_list_agents(self, cognite_client: CogniteClient, created_agent: Agent) -> None:
        """Test listing agents."""
        agent_list = cognite_client.agents.list()
        assert isinstance(agent_list, AgentList)
        assert len(agent_list) > 0
        assert created_agent.external_id in [a.external_id for a in agent_list]

    def test_create_and_retrieve_minimal_agent(self, cognite_client: CogniteClient, minimal_agent: AgentUpsert) -> None:
        """Test creating and retrieving a minimal agent without tools."""
        created_agent: Agent | None = None
        try:
            created_agent = cognite_client.agents.upsert(minimal_agent)
            assert created_agent is not None
            assert created_agent.external_id == minimal_agent.external_id
            assert created_agent.name == minimal_agent.name
            assert created_agent.description == minimal_agent.description
            assert created_agent.tools is None or len(created_agent.tools) == 0

            # Retrieve the agent
            retrieved_agent = cognite_client.agents.retrieve(external_ids=minimal_agent.external_id)
            assert retrieved_agent is not None
            assert retrieved_agent.external_id == created_agent.external_id
            assert retrieved_agent.name == minimal_agent.name
        finally:
            if created_agent:
                cognite_client.agents.delete(external_ids=minimal_agent.external_id, ignore_unknown_ids=True)

    def test_create_and_retrieve_agent_with_kg_tool(
        self, cognite_client: CogniteClient, agent_with_kg_tool: AgentUpsert
    ) -> None:
        """Test creating and retrieving an agent with knowledge graph tool."""
        created_agent: Agent | None = None
        try:
            created_agent = cognite_client.agents.upsert(agent_with_kg_tool)
            assert created_agent is not None
            assert created_agent.tools is not None
            assert len(created_agent.tools) == 1

            kg_tool = created_agent.tools[0]
            assert kg_tool.name == "find_assets"
            assert kg_tool.description == "Use this tool to find assets in the knowledge graph"

            # Retrieve the agent
            retrieved_agent = cognite_client.agents.retrieve(external_ids=agent_with_kg_tool.external_id)
            assert retrieved_agent is not None
            assert retrieved_agent.tools is not None
            assert len(retrieved_agent.tools) == 1
        finally:
            if created_agent:
                cognite_client.agents.delete(external_ids=agent_with_kg_tool.external_id, ignore_unknown_ids=True)

    def test_create_and_retrieve_agent_with_all_tools(
        self, cognite_client: CogniteClient, agent_with_all_tools: AgentUpsert
    ) -> None:
        """Test creating and retrieving an agent with all tool types."""
        created_agent: Agent | None = None
        try:
            created_agent = cognite_client.agents.upsert(agent_with_all_tools)
            assert created_agent is not None
            assert created_agent.tools is not None
            assert len(created_agent.tools) == 4

            # Verify all tools are present
            tool_names = {tool.name for tool in created_agent.tools}
            expected_names = {"find_assets", "summarize_document", "ask_document", "query_timeseries"}
            assert tool_names == expected_names

            # Retrieve the agent
            retrieved_agent = cognite_client.agents.retrieve(external_ids=agent_with_all_tools.external_id)
            assert retrieved_agent is not None
            assert retrieved_agent.tools is not None
            assert len(retrieved_agent.tools) == 4
        finally:
            if created_agent:
                cognite_client.agents.delete(external_ids=agent_with_all_tools.external_id, ignore_unknown_ids=True)

    def test_update_agent(self, cognite_client: CogniteClient, minimal_agent: AgentUpsert) -> None:
        """Test updating an agent."""
        created_agent: Agent | None = None
        try:
            # Create initial agent
            created_agent = cognite_client.agents.upsert(minimal_agent)
            assert created_agent is not None

            # Update the agent
            updated_agent = AgentUpsert(
                external_id=minimal_agent.external_id,
                name="Updated Agent Name",
                description="Updated description",
                instructions="Updated instructions",
                model="azure/gpt-4o",
            )

            updated_result = cognite_client.agents.upsert(updated_agent)
            assert updated_result.name == "Updated Agent Name"
            assert updated_result.description == "Updated description"
            assert updated_result.instructions == "Updated instructions"
            assert updated_result.model == "azure/gpt-4o"
        finally:
            if created_agent:
                cognite_client.agents.delete(external_ids=minimal_agent.external_id, ignore_unknown_ids=True)

    def test_retrieve_multiple_agents(self, cognite_client: CogniteClient) -> None:
        """Test retrieving multiple agents."""
        agent1 = AgentUpsert(
            external_id=f"test_multi_agent_1_{random_string(10)}",
            name="Test Multi Agent 1",
        )
        agent2 = AgentUpsert(
            external_id=f"test_multi_agent_2_{random_string(10)}",
            name="Test Multi Agent 2",
        )

        created_agents = None
        try:
            created_agents = cognite_client.agents.upsert([agent1, agent2])
            assert isinstance(created_agents, AgentList)
            assert len(created_agents) == 2

            # Retrieve multiple agents
            retrieved_agents = cognite_client.agents.retrieve(external_ids=[a.external_id for a in created_agents])
            assert isinstance(retrieved_agents, AgentList)
            assert len(retrieved_agents) == 2
            assert {a.external_id for a in retrieved_agents} == {a.external_id for a in created_agents}
        finally:
            if created_agents:
                cognite_client.agents.delete(
                    external_ids=[a.external_id for a in created_agents], ignore_unknown_ids=True
                )

    def test_retrieve_non_existing_agent(self, cognite_client: CogniteClient) -> None:
        """Test retrieving a non-existing agent."""
        actual_retrieved = cognite_client.agents.retrieve(external_ids="notExistingAgent", ignore_unknown_ids=True)
        assert actual_retrieved is None

    def test_retrieve_non_existing_agent_raise_error(self, cognite_client: CogniteClient) -> None:
        """Test retrieving a non-existing agent with error raising."""
        with pytest.raises(CogniteNotFoundError):
            cognite_client.agents.retrieve(external_ids=["notExistingAgent"], ignore_unknown_ids=False)

    def test_retrieve_some_existing_and_some_non_existing_agents_ignore_unknown(
        self, cognite_client: CogniteClient, created_agent: Agent
    ) -> None:
        """Test retrieving mix of existing and non-existing agents with ignore_unknown_ids=True."""
        retrieved = cognite_client.agents.retrieve(
            external_ids=[created_agent.external_id, "notExistingAgent"], ignore_unknown_ids=True
        )
        assert isinstance(retrieved, AgentList)
        assert len(retrieved) == 1
        assert retrieved[0].external_id == created_agent.external_id

    def test_retrieve_some_existing_and_some_non_existing_agents_raise_error(
        self, cognite_client: CogniteClient, created_agent: Agent
    ) -> None:
        """Test retrieving mix of existing and non-existing agents with error raising."""
        with pytest.raises(CogniteNotFoundError) as e:
            cognite_client.agents.retrieve(
                external_ids=[created_agent.external_id, "notExistingAgent"], ignore_unknown_ids=False
            )
        assert {"externalId": "notExistingAgent"} in e.value.not_found
        assert cognite_client.agents.retrieve(external_ids=created_agent.external_id) is not None

    def test_delete_non_existing_agent(self, cognite_client: CogniteClient) -> None:
        """Test deleting a non-existing agent."""
        cognite_client.agents.delete(external_ids="notExistingAgent", ignore_unknown_ids=True)
        # No error should be raised

    def test_delete_non_existing_agent_raise_error(self, cognite_client: CogniteClient) -> None:
        """Test deleting a non-existing agent with error raising."""
        with pytest.raises(CogniteNotFoundError):
            cognite_client.agents.delete(external_ids="notExistingAgent", ignore_unknown_ids=False)

    def test_delete_some_existing_and_some_non_existing_agents_ignore_unknown(
        self, cognite_client: CogniteClient, created_agent: Agent
    ) -> None:
        """Test deleting mix of existing and non-existing agents with ignore_unknown_ids=True."""
        cognite_client.agents.delete(
            external_ids=[created_agent.external_id, "notExistingAgent"], ignore_unknown_ids=True
        )
        assert cognite_client.agents.retrieve(external_ids=created_agent.external_id) is None

    def test_delete_some_existing_and_some_non_existing_agents_raise_error(
        self, cognite_client: CogniteClient, created_agent: Agent
    ) -> None:
        """Test deleting mix of existing and non-existing agents with error raising."""
        with pytest.raises(CogniteNotFoundError) as e:
            cognite_client.agents.delete(
                external_ids=[created_agent.external_id, "notExistingAgent"], ignore_unknown_ids=False
            )
        assert cognite_client.agents.retrieve(external_ids=created_agent.external_id) is None
        assert {"externalId": "notExistingAgent"} in e.value.not_found


class TestAgentToolTypes:
    """Test class specifically for testing different agent tool types."""

    def test_query_knowledge_graph_tool(
        self, cognite_client: CogniteClient, query_knowledge_graph_tool: QueryKnowledgeGraphAgentToolUpsert
    ) -> None:
        """Test agent with QueryKnowledgeGraph tool."""
        agent = AgentUpsert(
            external_id=f"test_kg_tool_{random_string(10)}",
            name="KG Tool Test Agent",
            tools=[query_knowledge_graph_tool],
        )

        created_agent = None
        try:
            created_agent = cognite_client.agents.upsert(agent)
            assert created_agent.tools is not None
            assert len(created_agent.tools) == 1

            kg_tool = created_agent.tools[0]
            assert kg_tool.name == "find_assets"
            assert "knowledge graph" in kg_tool.description.lower()
        finally:
            if created_agent:
                cognite_client.agents.delete(external_ids=agent.external_id, ignore_unknown_ids=True)

    def test_document_tools(
        self,
        cognite_client: CogniteClient,
        summarize_document_tool: SummarizeDocumentAgentToolUpsert,
        ask_document_tool: AskDocumentAgentToolUpsert,
    ) -> None:
        """Test agent with document-related tools."""
        agent = AgentUpsert(
            external_id=f"test_doc_tools_{random_string(10)}",
            name="Document Tools Test Agent",
            tools=[summarize_document_tool, ask_document_tool],
        )

        created_agent = None
        try:
            created_agent = cognite_client.agents.upsert(agent)
            assert created_agent.tools is not None
            assert len(created_agent.tools) == 2

            tool_names = {tool.name for tool in created_agent.tools}
            assert "summarize_document" in tool_names
            assert "ask_document" in tool_names
        finally:
            if created_agent:
                cognite_client.agents.delete(external_ids=agent.external_id, ignore_unknown_ids=True)

    def test_timeseries_tool(
        self, cognite_client: CogniteClient, query_timeseries_tool: QueryTimeSeriesDatapointsAgentToolUpsert
    ) -> None:
        """Test agent with QueryTimeSeriesDatapoints tool."""
        agent = AgentUpsert(
            external_id=f"test_ts_tool_{random_string(10)}",
            name="TimeSeries Tool Test Agent",
            tools=[query_timeseries_tool],
        )

        created_agent = None
        try:
            created_agent = cognite_client.agents.upsert(agent)
            assert created_agent.tools is not None
            assert len(created_agent.tools) == 1

            ts_tool = created_agent.tools[0]
            assert ts_tool.name == "query_timeseries"
            assert "time series" in ts_tool.description.lower()
        finally:
            if created_agent:
                cognite_client.agents.delete(external_ids=agent.external_id, ignore_unknown_ids=True)
