from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes.agents import Agent, AgentApply, AgentList
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
    def apply(self, agents: AgentApply) -> Agent: ...

    @overload
    def apply(self, agents: Sequence[AgentApply]) -> AgentList: ...

    def apply(self, agents: AgentApply | Sequence[AgentApply]) -> Agent | AgentList:
        """Create new agents.

        Args:
            agents (AgentApply | Sequence[AgentApply]): Agent or list of agents to create.

        Returns:
            Agent | AgentList: The created agent(s).

        """
        self._warnings.warn()
        return self._create_multiple(
            list_cls=AgentList,
            resource_cls=Agent,
            items=agents,
            input_resource_cls=AgentApply,
        )

    @overload
    def retrieve(self, external_ids: str, ignore_unknown_ids: bool = False) -> Agent | None: ...

    @overload
    def retrieve(self, external_ids: SequenceNotStr[str], ignore_unknown_ids: bool = False) -> AgentList: ...

    def retrieve(
        self, external_ids: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> Agent | AgentList | None:
        """Retrieve one or more agents.

        Args:
            external_ids (str | SequenceNotStr[str]): The external id of the agent(s) to retrieve.
            ignore_unknown_ids (bool): Whether to ignore unknown IDs.

        Returns:
            Agent | AgentList | None: The requested agent or agent list.

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
        """Delete one or more agents.

        Args:
            external_ids (str | SequenceNotStr[str]): External ID of the agent or a list of external ids.
            ignore_unknown_ids (bool): If `True`, the call will ignore unknown external IDs.

        """
        self._warnings.warn()
        self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_ids),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )

    def list(self) -> AgentList:  # The API does not yet support limit or pagination
        """List agents.

        Returns:
            AgentList: The list of agents.

        """
        self._warnings.warn()
        res = self._get(url_path=self._RESOURCE_PATH)
        return AgentList._load(res.json()["items"], cognite_client=self._cognite_client)
