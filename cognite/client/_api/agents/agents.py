from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes.agents import Agent, AgentList, AgentWrite
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class AgentsAPI(APIClient):
    _RESOURCE_PATH = "/ai/agents"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warnings = FeaturePreviewWarning(api_maturity="alpha", sdk_maturity="alpha", feature_name="Agents")

    @overload
    def apply(self, agent: AgentWrite) -> Agent: ...

    @overload
    def apply(self, agent: Sequence[AgentWrite]) -> AgentList: ...

    def apply(self, agent: AgentWrite | Sequence[AgentWrite]) -> Agent | AgentList:
        """Create a new agent.

        Args:
            agent (AgentWrite | Sequence[AgentWrite]): Agent or list of agents to create.

        Returns:
            Agent | AgentList: The created agent(s).

        """
        headers = {"cdf-version": "alpha"}
        self._warnings.warn()
        return self._create_multiple(
            list_cls=AgentList,
            resource_cls=Agent,
            items=agent,
            input_resource_cls=AgentWrite,
            headers=headers,
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
        is_single = isinstance(external_ids, str)
        body = self._create_body(external_ids, ignore_unknown_ids)

        self._warnings.warn()

        headers = {"cdf-version": "alpha"}
        res = self._post(url_path=self._RESOURCE_PATH + "/byids", json=body, headers=headers)
        lst = AgentList._load(res.json()["items"], cognite_client=self._cognite_client)
        if is_single:
            return lst[0] if lst else None
        return lst

    @staticmethod
    def _create_body(external_ids: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> dict:
        ids = [external_ids] if isinstance(external_ids, str) else external_ids
        body = {"items": [{"externalId": eid} for eid in ids], "ignoreUnknownIds": ignore_unknown_ids}
        return body

    def delete(self, external_ids: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> None:
        """Delete one or more agents.

        Args:
            external_ids (str | SequenceNotStr[str]): External ID of the agent or a list of external ids.
            ignore_unknown_ids (bool): If `True`, the call will ignore unknown external IDs.

        """
        body = self._create_body(external_ids, ignore_unknown_ids)
        self._warnings.warn()
        headers = {"cdf-version": "alpha"}
        self._post(url_path=self._RESOURCE_PATH + "/delete", json=body, headers=headers)

    def list(self) -> AgentList:
        """List agents.

        Returns:
            AgentList: The list of agents.

        """
        self._warnings.warn()
        headers = {"cdf-version": "alpha"}
        res = self._get(url_path=self._RESOURCE_PATH, headers=headers)
        return AgentList._load(res.json()["items"], cognite_client=self._cognite_client)
