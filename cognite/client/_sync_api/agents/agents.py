"""
===============================================================================
7e14a554efcc7d388d9f90c968de31a6
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, overload

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.agents import Agent, AgentList, AgentUpsert
from cognite.client.data_classes.agents.chat import Action, ActionResult, AgentChatResponse, Message
from cognite.client.utils._async_helpers import run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncAgentsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def upsert(self, agents: AgentUpsert) -> Agent: ...

    @overload
    def upsert(self, agents: Sequence[AgentUpsert]) -> AgentList: ...

    def upsert(self, agents: AgentUpsert | Sequence[AgentUpsert]) -> Agent | AgentList:
        """
        `Create or update (upsert) one or more agents. <https://api-docs.cognite.com/20230101-beta/tag/Agents/operation/main_ai_agents_post/>`_

        Args:
            agents (AgentUpsert | Sequence[AgentUpsert]): Agent or list of agents to create or update.

        Returns:
            Agent | AgentList: The created or updated agent(s).

        Examples:

            Create a new agent with a query knowledge graph tool to find assets:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.agents import (
                ...     AgentUpsert,
                ...     QueryKnowledgeGraphAgentToolUpsert,
                ...     QueryKnowledgeGraphAgentToolConfiguration,
                ...     DataModelInfo
                ... )
                >>> client = CogniteClient()
                ...
                >>> find_assets_tool = QueryKnowledgeGraphAgentToolUpsert(
                ...     name="find assets",
                ...     description="Use this tool to find assets",
                ...     configuration=QueryKnowledgeGraphAgentToolConfiguration(
                ...         data_models=[
                ...             DataModelInfo(
                ...                 space="cdf_idm",
                ...                 external_id="CogniteProcessIndustries",
                ...                 version="v1",
                ...                 view_external_ids=["CogniteAsset"],
                ...             )
                ...         ]
                ...     )
                ... )
                >>> agent = AgentUpsert(
                ...     external_id="my_agent",
                ...     name="My Agent",
                ...     labels=["published"],
                ...     tools=[find_assets_tool]
                ... )
                >>> client.agents.upsert(agents=[agent])

            Create an agent with multiple different tools:

                >>> from cognite.client.data_classes.agents import (
                ...     AgentUpsert,
                ...     QueryKnowledgeGraphAgentToolUpsert,
                ...     QueryKnowledgeGraphAgentToolConfiguration,
                ...     DataModelInfo,
                ...     SummarizeDocumentAgentToolUpsert,
                ...     AskDocumentAgentToolUpsert,
                ...     QueryTimeSeriesDatapointsAgentToolUpsert
                ... )
                ...
                >>> find_assets_tool = QueryKnowledgeGraphAgentToolUpsert(
                ...     name="find assets",
                ...     description="Use this tool to query the knowledge graph for assets",
                ...     configuration=QueryKnowledgeGraphAgentToolConfiguration(
                ...         data_models=[
                ...             DataModelInfo(
                ...                 space="cdf_idm",
                ...                 external_id="CogniteProcessIndustries",
                ...                 version="v1",
                ...                 view_external_ids=["CogniteAsset"],
                ...             )
                ...         ]
                ...     )
                ... )
                >>> find_files_tool = QueryKnowledgeGraphAgentToolUpsert(
                ...     name="find files",
                ...     description="Use this tool to query the knowledge graph for files",
                ...     configuration=QueryKnowledgeGraphAgentToolConfiguration(
                ...         data_models=[
                ...             DataModelInfo(
                ...                 space="cdf_idm",
                ...                 external_id="CogniteProcessIndustries",
                ...                 version="v1",
                ...                 view_external_ids=["CogniteFile"],
                ...             )
                ...         ]
                ...     )
                ... )
                >>> find_time_series_tool = QueryKnowledgeGraphAgentToolUpsert(
                ...     name="find time series",
                ...     description="Use this tool to query the knowledge graph for time series",
                ...     configuration=QueryKnowledgeGraphAgentToolConfiguration(
                ...         data_models=[
                ...             DataModelInfo(
                ...                 space="cdf_idm",
                ...                 external_id="CogniteProcessIndustries",
                ...                 version="v1",
                ...                 view_external_ids=["CogniteTimeSeries"],
                ...             )
                ...         ]
                ...     )
                ... )
                >>> summarize_tool = SummarizeDocumentAgentToolUpsert(
                ...     name="summarize document",
                ...     description="Use this tool to get a summary of a document"
                ... )
                >>> ask_doc_tool = AskDocumentAgentToolUpsert(
                ...     name="ask document",
                ...     description="Use this tool to ask questions about specific documents"
                ... )
                >>> ts_tool = QueryTimeSeriesDatapointsAgentToolUpsert(
                ...     name="query time series",
                ...     description="Use this tool to query time series data points"
                ... )
                >>> agent = AgentUpsert(
                ...     external_id="my_agent",
                ...     name="My agent",
                ...     description="An agent with many tools",
                ...     instructions="You are a helpful assistant that can query knowledge graphs, summarize documents, answer questions about documents, and query time series data points.",
                ...     labels=["published"],
                ...     tools=[find_assets_tool, find_files_tool, find_time_series_tool, summarize_tool, ask_doc_tool, ts_tool]
                ... )
                >>> client.agents.upsert(agents=[agent])
        """
        return run_sync(self.__async_client.agents.upsert(agents=agents))

    @overload
    def retrieve(self, external_ids: str, ignore_unknown_ids: bool = False) -> Agent | None: ...

    @overload
    def retrieve(self, external_ids: SequenceNotStr[str], ignore_unknown_ids: bool = False) -> AgentList: ...

    def retrieve(
        self, external_ids: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> Agent | AgentList | None:
        """
        `Retrieve one or more agents by external ID. <https://api-docs.cognite.com/20230101-beta/tag/Agents/operation/get_agents_by_ids_ai_agents_byids_post/>`_

        Args:
            external_ids (str | SequenceNotStr[str]): The external id of the agent(s) to retrieve.
            ignore_unknown_ids (bool): Whether to ignore unknown IDs. Defaults to False.

        Returns:
            Agent | AgentList | None: The requested agent or agent list. `None` is returned if `ignore_unknown_ids` is `True` and the external ID is not found.

        Examples:

            Retrieve an agent by external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.agents.retrieve(external_ids="my_agent")

            Retrieve multiple agents:

                >>> res = client.agents.retrieve(external_ids=["my_agent_1", "my_agent_2"])
        """
        return run_sync(
            self.__async_client.agents.retrieve(external_ids=external_ids, ignore_unknown_ids=ignore_unknown_ids)
        )

    def delete(self, external_ids: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> None:
        """
        `Delete one or more agents. <https://api-docs.cognite.com/20230101-beta/tag/Agents/operation/agent_delete_ai_agents_delete_post/>`_

        Args:
            external_ids (str | SequenceNotStr[str]): External ID of the agent or a list of external ids.
            ignore_unknown_ids (bool): If `True`, the call will ignore unknown external IDs. Defaults to False.

        Examples:

            Delete an agent by external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.agents.delete(external_ids="my_agent")
        """
        return run_sync(
            self.__async_client.agents.delete(external_ids=external_ids, ignore_unknown_ids=ignore_unknown_ids)
        )

    def list(self) -> AgentList:
        """
        `List agents. <https://api-docs.cognite.com/20230101-beta/tag/Agents/operation/agent_list_ai_agents_get/>`_

        Returns:
            AgentList: The list of agents.

        Examples:

            List all agents:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> agent_list = client.agents.list()
        """
        return run_sync(self.__async_client.agents.list())

    def chat(
        self,
        agent_external_id: str,
        messages: Message | ActionResult | Sequence[Message | ActionResult],
        cursor: str | None = None,
        actions: Sequence[Action] | None = None,
    ) -> AgentChatResponse:
        """
        `Chat with an agent. <https://api-docs.cognite.com/20230101-beta/tag/Agents/operation/agent_session_ai_agents_chat_post/>`_

        Given a user query, the Atlas AI agent responds by reasoning and using the tools associated with it.
        Users can ensure conversation continuity by including the cursor from the previous response in subsequent requests.

        Args:
            agent_external_id (str): External ID that uniquely identifies the agent.
            messages (Message | ActionResult | Sequence[Message | ActionResult]): A list of one or many input messages to the agent. Can include regular messages and action results.
            cursor (str | None): The cursor to use for continuation of a conversation. Use this to
                create multi-turn conversations, as the cursor will keep track of the conversation state.
            actions (Sequence[Action] | None): A list of client-side actions that can be called by the agent.

        Returns:
            AgentChatResponse: The response from the agent.

        Examples:

            Start a simple chat with an agent:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.agents import Message
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> response = client.agents.chat(
                ...     agent_external_id="my_agent",
                ...     messages=Message("What can you help me with?")
                ... )
                >>> print(response.text)

            Continue a conversation using the cursor:

                >>> follow_up = client.agents.chat(
                ...     agent_external_id="my_agent",
                ...     messages=Message("Tell me more about that"),
                ...     cursor=response.cursor
                ... )

            Send multiple messages at once:

                >>> response = client.agents.chat(
                ...     agent_external_id="my_agent",
                ...     messages=[
                ...         Message("Help me find the 1st stage compressor."),
                ...         Message("Once you have found it, find related time series.")
                ...     ]
                ... )

            Chat with client-side actions:

                >>> from cognite.client.data_classes.agents import ClientToolAction, ClientToolResult
                >>> add_numbers_action = ClientToolAction(
                ...     name="add",
                ...     description="Add two numbers together",
                ...     parameters={
                ...         "type": "object",
                ...         "properties": {
                ...             "a": {"type": "number", "description": "First number"},
                ...             "b": {"type": "number", "description": "Second number"},
                ...         },
                ...         "required": ["a", "b"]
                ...     }
                ... )
                >>> response = client.agents.chat(
                ...     agent_external_id="my_agent",
                ...     messages=Message("What is 42 plus 58?"),
                ...     actions=[add_numbers_action]
                ... )
                >>> if response.action_calls:
                ...     for call in response.action_calls:
                ...         # Execute the action
                ...         result = call.arguments["a"] + call.arguments["b"]
                ...         # Send result back
                ...         response = client.agents.chat(
                ...             agent_external_id="my_agent",
                ...             messages=ClientToolResult(
                ...                 action_id=call.action_id,
                ...                 content=f"The result is {result}"
                ...             ),
                ...             cursor=response.cursor,
                ...             actions=[add_numbers_action]
                ...         )
        """
        return run_sync(
            self.__async_client.agents.chat(
                agent_external_id=agent_external_id, messages=messages, cursor=cursor, actions=actions
            )
        )
