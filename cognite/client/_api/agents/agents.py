from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes.agents import Agent, AgentList, AgentUpsert
from cognite.client.data_classes.agents.chat import AgentChatResponse, Message, MessageList
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class AgentsAPI(APIClient):
    _RESOURCE_PATH = "/ai/agents"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warnings = FeaturePreviewWarning(api_maturity="beta", sdk_maturity="alpha", feature_name="Agents")
        self._api_subversion = "beta"
        self._CREATE_LIMIT = 1
        self._DELETE_LIMIT = 1

    @overload
    def upsert(self, agents: AgentUpsert) -> Agent: ...

    @overload
    def upsert(self, agents: Sequence[AgentUpsert]) -> AgentList: ...

    def upsert(self, agents: AgentUpsert | Sequence[AgentUpsert]) -> Agent | AgentList:
        """`Create or update (upsert) one or more agents. <https://api-docs.cognite.com/20230101-beta/tag/Agents/operation/main_ai_agents_post/>`_

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
                ...     tools=[find_assets_tool, find_files_tool, find_time_series_tool, summarize_tool, ask_doc_tool, ts_tool]
                ... )
                >>> client.agents.upsert(agents=[agent])


        """
        self._warnings.warn()
        return self._create_multiple(
            list_cls=AgentList,
            resource_cls=Agent,
            items=agents,
            input_resource_cls=AgentUpsert,
        )

    @overload
    def retrieve(self, external_ids: str, ignore_unknown_ids: bool = False) -> Agent | None: ...

    @overload
    def retrieve(self, external_ids: SequenceNotStr[str], ignore_unknown_ids: bool = False) -> AgentList: ...

    def retrieve(
        self, external_ids: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> Agent | AgentList | None:
        """`Retrieve one or more agents by external ID. <https://api-docs.cognite.com/20230101-beta/tag/Agents/operation/get_agents_by_ids_ai_agents_byids_post/>`_

        Args:
            external_ids (str | SequenceNotStr[str]): The external id of the agent(s) to retrieve.
            ignore_unknown_ids (bool): Whether to ignore unknown IDs. Defaults to False.

        Returns:
            Agent | AgentList | None: The requested agent or agent list. `None` is returned if `ignore_unknown_ids` is `True` and the external ID is not found.

        Examples:

            Retrieve an agent by external id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.agents.retrieve(external_ids="my_agent")

            Retrieve multiple agents:

                >>> res = client.agents.retrieve(external_ids=["my_agent_1", "my_agent_2"])
        """
        self._warnings.warn()
        identifiers = IdentifierSequence.load(external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=AgentList,
            resource_cls=Agent,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    def delete(self, external_ids: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> None:
        """`Delete one or more agents. <https://api-docs.cognite.com/20230101-beta/tag/Agents/operation/agent_delete_ai_agents_delete_post/>`_

        Args:
            external_ids (str | SequenceNotStr[str]): External ID of the agent or a list of external ids.
            ignore_unknown_ids (bool): If `True`, the call will ignore unknown external IDs. Defaults to False.

        Examples:

            Delete an agent by external id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.agents.delete(external_ids="my_agent")

        """
        self._warnings.warn()
        self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_ids),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )

    def list(self) -> AgentList:  # The API does not yet support limit or pagination
        """`List agents. <https://api-docs.cognite.com/20230101-beta/tag/Agents/operation/agent_list_ai_agents_get/>`_

        Returns:
            AgentList: The list of agents.

        Examples:

            List all agents:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> agent_list = client.agents.list()

        """
        self._warnings.warn()
        res = self._get(url_path=self._RESOURCE_PATH)
        return AgentList._load(res.json()["items"], cognite_client=self._cognite_client)

    def chat(
        self,
        agent_id: str,
        messages: Message | Sequence[Message],
        cursor: str | None = None,
    ) -> AgentChatResponse:
        """`Chat with an agent. <https://api-docs.cognite.com/20230101-beta/tag/Agents/operation/agent_session_ai_agents_chat_post/>`_

        Given a user query, the Atlas AI agent responds by reasoning and using the tools associated with it.
        Users can ensure conversation continuity by including the cursor from the previous response in subsequent requests.

        Args:
            agent_id (str): External ID that uniquely identifies the agent.
            messages (Message | Sequence[Message]): A list of one or many input messages to the agent.
            cursor (str | None): The cursor to use for continuation of a conversation. Use this to
                create multi-turn conversations, as the cursor will keep track of the conversation state.

        Returns:
            AgentChatResponse: The response from the agent.

        Examples:

            Start a simple chat with an agent:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.agents import Message
                >>> client = CogniteClient()
                >>> response = client.agents.chat(
                ...     agent_id="my_agent",
                ...     messages=Message("What can you help me with?")
                ... )
                >>> print(response.text)

            Continue a conversation using the cursor:

                >>> follow_up = client.agents.chat(
                ...     agent_id="my_agent",
                ...     messages=Message("Tell me more about that"),
                ...     cursor=response.cursor
                ... )

            Send multiple messages at once:

                >>> response = client.agents.chat(
                ...     agent_id="my_agent",
                ...     messages=[
                ...         Message("Help me find the 1st stage compressor."),
                ...         Message("Once you have found it, find related time series.")
                ...     ]
                ... )
        """
        self._warnings.warn()

        # Convert single message to list
        if isinstance(messages, Message):
            messages = [messages]

        # Build request body
        body = {
            "agentId": agent_id,
            "messages": MessageList(messages).dump(camel_case=True),
        }

        if cursor is not None:
            body["cursor"] = cursor

        # Make the API call
        response = self._post(
            url_path=self._RESOURCE_PATH + "/chat",
            json=body,
        )

        return AgentChatResponse._load(response.json(), cognite_client=self._cognite_client)
