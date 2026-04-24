from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from cognite.client.data_classes.agents.chat import (
    Action,
    ActionResult,
    AgentChatResponse,
    Message,
)

if TYPE_CHECKING:
    from cognite.client._api.agents.agents import AgentsAPI


class AgentSession:
    """Stateful session for multi-turn conversations with a Cognite agent.

    Created via :meth:`cognite.client._api.agents.agents.AgentsAPI.create_session`
    on :class:`~cognite.client.AsyncCogniteClient`. Do not instantiate directly.

    The session automatically threads the conversation cursor across successive
    :meth:`chat` calls, so callers do not need to track cursor state themselves.

    Each session is bound to one agent and one conversation; create a new session
    via ``create_session()`` to start a new conversation (there is no ``reset()``
    method). The session is not safe for concurrent ``await session.chat(...)``
    calls on the same instance — use separate ``AgentSession`` objects for
    parallel conversations.

    Args:
        agents_api (AgentsAPI): The async agents API used to make chat calls.
        agent_external_id (str): External ID of the agent bound to this session.
        actions (Sequence[Action] | None): Client-side actions available to the agent.
        cursor (str | None): Initial cursor (``None`` for a fresh conversation, or
            an existing cursor to resume a prior conversation).
    """

    def __init__(
        self,
        agents_api: AgentsAPI,
        agent_external_id: str,
        actions: Sequence[Action] | None,
        cursor: str | None,
    ) -> None:
        self._agents_api = agents_api
        self.agent_external_id = agent_external_id
        self.actions = actions
        self._cursor = cursor

    @property
    def cursor(self) -> str | None:
        """The current conversation cursor.

        ``None`` until the first successful :meth:`chat` call sets it. After each
        successful response the cursor advances; if a response has no cursor the
        previous non-null value is retained. If a chat request fails the cursor
        is unchanged so the call can be retried.
        """
        return self._cursor

    async def chat(
        self,
        messages: Message | ActionResult | Sequence[Message | ActionResult],
    ) -> AgentChatResponse:
        """Send messages to the agent and receive a response.

        The cursor from the previous response is threaded automatically into the
        outgoing request. On success the session's cursor advances to the response
        cursor (or is retained if the response has no cursor). On failure the
        cursor is unchanged.

        Args:
            messages (Message | ActionResult | Sequence[Message | ActionResult]): One or
                more messages and/or action results. Accepts the same types as
                :meth:`cognite.client._api.agents.agents.AgentsAPI.chat`.

        Returns:
            AgentChatResponse: The agent's response.
        """
        response = await self._agents_api.chat(
            agent_external_id=self.agent_external_id,
            messages=messages,
            cursor=self._cursor,
            actions=self.actions,
        )
        self._cursor = response.cursor or self._cursor
        return response
