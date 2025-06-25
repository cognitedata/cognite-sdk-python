from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes.agents import Agent, AgentUpsert, AgentUpsertList
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
        self._warnings = FeaturePreviewWarning(api_maturity="alpha", sdk_maturity="alpha", feature_name="Agents")
        self._api_subversion = "alpha"
        self._CREATE_LIMIT = 1
        self._DELETE_LIMIT = 1

    @overload
    def apply(self, agents: AgentUpsert) -> Agent: ...

    @overload
    def apply(self, agents: Sequence[AgentUpsert]) -> AgentUpsertList: ...

    def apply(self, agents: AgentUpsert | Sequence[AgentUpsert]) -> Agent | AgentUpsertList:
        """`Create or update (upsert) one or more agents. <https://api-docs.cognite.com/20230101-alpha/tag/Agents/operation/main_api_v1_projects__projectName__ai_agents_post>`_

        Args:
            agents (AgentUpsert | Sequence[AgentUpsert]): Agent or list of agents to create or update.

        Returns:
            Agent | AgentUpsertList: The created or updated agent(s).

        Examples:

            Create a new agent:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.agents import AgentUpsert, AgentToolUpsert
                >>> client = CogniteClient()
                >>> find_assets_tool = AgentToolUpsert(
                ...     name="find assets",
                ...     type="queryKnowledgeGraph",
                ...     description="Use this tool to find assets",
                ...     configuration={
                ...         "dataModels": [
                ...             {
                ...                 "space": "cdf_cdm",
                ...                 "externalId": "CogniteCore",
                ...                 "version": "v1",
                ...                 "viewExternalIds": ["CogniteAsset"]
                ...             }
                ...         ],
                ...         "instanceSpaces": {"type": "all"}
                ...     }
                ... )
                >>> agent = AgentUpsert(
                ...     external_id="my_agent",
                ...     name="My Agent",
                ...     tools=[find_assets_tool]
                ... )
                >>> client.agents.apply(agents=[agent])

        """
        self._warnings.warn()
        return self._create_multiple(
            list_cls=AgentUpsertList,
            resource_cls=Agent,
            items=agents,
            input_resource_cls=AgentUpsert,
        )

    @overload
    def retrieve(self, external_ids: str, ignore_unknown_ids: bool = False) -> Agent | None: ...

    @overload
    def retrieve(self, external_ids: SequenceNotStr[str], ignore_unknown_ids: bool = False) -> AgentUpsertList: ...

    def retrieve(
        self, external_ids: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> Agent | AgentUpsertList | None:
        """`Retrieve one or more agents by external ID. <https://api-docs.cognite.com/20230101-alpha/tag/Agents/operation/get_agents_by_ids_api_v1_projects__projectName__ai_agents_byids_post>`_

        Args:
            external_ids (str | SequenceNotStr[str]): The external id of the agent(s) to retrieve.
            ignore_unknown_ids (bool): Whether to ignore unknown IDs. Defaults to False.

        Returns:
            Agent | AgentUpsertList | None: The requested agent or agent list. `None` is returned if `ignore_unknown_ids` is `True` and the external ID is not found.

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
            list_cls=AgentUpsertList,
            resource_cls=Agent,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    def delete(self, external_ids: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> None:
        """`Delete one or more agents. <https://api-docs.cognite.com/20230101-alpha/tag/Agents/operation/agent_delete_api_v1_projects__projectName__ai_agents_delete_post>`_

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

    def list(self) -> AgentUpsertList:  # The API does not yet support limit or pagination
        """`List agents. <https://api-docs.cognite.com/20230101-alpha/tag/Agents/operation/agent_list_api_v1_projects__projectName__ai_agents_get>`_

        Returns:
            AgentUpsertList: The list of agents.

        Examples:

            List all agents:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> agent_list = client.agents.list()

        """
        self._warnings.warn()
        res = self._get(url_path=self._RESOURCE_PATH)
        return AgentUpsertList._load(res.json()["items"], cognite_client=self._cognite_client)
