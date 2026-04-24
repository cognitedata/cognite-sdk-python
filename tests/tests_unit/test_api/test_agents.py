from __future__ import annotations

import json
from collections.abc import Iterator
from unittest.mock import MagicMock

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.agents import (
    Agent,
    AgentList,
    AgentSession,
    AgentUpsert,
    ClientToolAction,
    ClientToolResult,
    Message,
    ToolConfirmationCall,
    ToolConfirmationResult,
)
from cognite.client.data_classes.agents.agent_tools import (
    DataModelInfo,
    InstanceSpaces,
    QueryKnowledgeGraphAgentToolConfiguration,
    QueryKnowledgeGraphAgentToolUpsert,
)
from cognite.client.exceptions import CogniteAPIError
from tests.utils import get_url, jsgz_load


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
                "runtimeVersion": "1.1.1",
                "labels": ["published"],
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
def agents_url(async_client: AsyncCogniteClient) -> str:
    return get_url(async_client.agents, async_client.agents._RESOURCE_PATH)


@pytest.fixture
def mock_agent_retrieve_response(
    httpx_mock: HTTPXMock, agents_url: str, agent_response_body: dict
) -> Iterator[HTTPXMock]:
    httpx_mock.add_response(method="POST", url=agents_url + "/byids", status_code=200, json=agent_response_body)
    yield httpx_mock


@pytest.fixture
def mock_agent_list_response(httpx_mock: HTTPXMock, agents_url: str, agent_response_body: dict) -> Iterator[HTTPXMock]:
    httpx_mock.add_response(method="GET", url=agents_url, status_code=200, json=agent_response_body)
    yield httpx_mock


@pytest.fixture
def mock_agent_delete_response(httpx_mock: HTTPXMock, agents_url: str) -> Iterator[HTTPXMock]:
    httpx_mock.add_response(method="POST", url=agents_url + "/delete", status_code=200, json={})
    yield httpx_mock


@pytest.fixture
def mock_agent_upsert_response(
    httpx_mock: HTTPXMock, agents_url: str, agent_response_body: dict, async_client: AsyncCogniteClient
) -> Iterator[HTTPXMock]:
    httpx_mock.add_response(method="POST", url=agents_url, status_code=200, json=agent_response_body)
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

    def test_delete(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, mock_agent_delete_response: HTTPXMock
    ) -> None:
        cognite_client.agents.delete("agent_1")
        url = str(mock_agent_delete_response.get_requests()[-1].url)
        assert url.endswith(async_client.agents._RESOURCE_PATH + "/delete")

    def test_delete_multiple(
        self,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        mock_agent_delete_response: HTTPXMock,
        agents_url: str,
    ) -> None:
        # Multiple deletes, so we need an additional response:

        mock_agent_delete_response.add_response(method="POST", url=agents_url + "/delete", status_code=200, json={})

        cognite_client.agents.delete(["agent_1", "agent_2"])
        url = str(mock_agent_delete_response.get_requests()[-1].url)
        assert url.endswith(async_client.agents._RESOURCE_PATH + "/delete")

    def test_upsert_minimal(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, mock_agent_upsert_response: HTTPXMock
    ) -> None:
        agent_write = AgentUpsert(external_id="agent_1", name="Agent 1")
        created_agent = cognite_client.agents.upsert(agent_write)
        assert isinstance(created_agent, Agent)
        assert created_agent.external_id == "agent_1"
        url = str(mock_agent_upsert_response.get_requests()[-1].url)
        assert url.endswith(async_client.agents._RESOURCE_PATH)

    def test_upsert_full(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, mock_agent_upsert_response: HTTPXMock
    ) -> None:
        agent_write = AgentUpsert(
            external_id="agent_1",
            name="Agent 1",
            description="Description 1",
            instructions="Instructions 1",
            model="vendor/model_1",
            runtime_version="1.1.1",
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
                        version="v2",
                    ),
                )
            ],
        )
        created_agent = cognite_client.agents.upsert(agent_write)
        assert isinstance(created_agent, Agent)
        assert created_agent.external_id == "agent_1"
        assert created_agent.runtime_version == "1.1.1"
        request_body = jsgz_load(mock_agent_upsert_response.get_requests()[-1].content)
        assert request_body["items"][0]["runtimeVersion"] == "1.1.1"
        url = str(mock_agent_upsert_response.get_requests()[-1].url)
        assert url.endswith(async_client.agents._RESOURCE_PATH)

    def test_upsert_multiple(
        self,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        mock_agent_upsert_response: HTTPXMock,
        agent_response_body: dict,
        agents_url: str,
    ) -> None:
        # Since _CREATE_LIMIT is 1 for agents, each agent is processed individually
        # and the mock response (which has 1 agent) is returned for each request.
        # So we need to add an additional mock response:
        agent_response_body["items"][0]["externalId"] = "agent_2"
        agent_response_body["items"][0]["name"] = "Agent 2"

        mock_agent_upsert_response.add_response(
            method="POST", url=agents_url, status_code=200, json=agent_response_body
        )

        agents_write = [
            AgentUpsert(external_id="agent_1", name="Agent 1"),
            AgentUpsert(external_id="agent_2", name="Agent 2"),
        ]
        created_agents = cognite_client.agents.upsert(agents_write)
        assert isinstance(created_agents, AgentList)
        assert len(created_agents) >= 1
        assert {agent.external_id for agent in created_agents} == {"agent_1", "agent_2"}
        url = str(mock_agent_upsert_response.get_requests()[-1].url)
        assert url.endswith(async_client.agents._RESOURCE_PATH)

    def test_upsert_ensure_retry(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        agent_response_body: dict[str, object],
        agents_url: str,
    ) -> None:
        from cognite.client.config import global_config

        with pytest.MonkeyPatch.context() as mp:
            # For unit tests, we have turned off retries so for this test we must allow 1 retry attempt:
            mp.setattr(global_config, "max_retries", 1)

            # Simulate a 503 response to ensure retry logic is triggered. Note we do not use 429 as that is always
            # retried, while 503 is only retried if the endpoint is marked as retryable.

            httpx_mock.add_response(
                method="POST", url=agents_url, status_code=503, json={"message": "Connection refused"}
            )
            httpx_mock.add_response(method="POST", url=agents_url, status_code=200, json=agent_response_body)

            created = cognite_client.agents.upsert(AgentUpsert(external_id="agent_1", name="Agent 1"))
            assert isinstance(created, Agent)

    def test_upsert_with_labels(self, cognite_client: CogniteClient, mock_agent_upsert_response: MagicMock) -> None:
        agent_write = AgentUpsert(
            external_id="agent_1",
            name="Agent 1",
            labels=["published"],
        )
        created_agent = cognite_client.agents.upsert(agent_write)
        assert isinstance(created_agent, Agent)
        assert created_agent.external_id == "agent_1"
        assert created_agent.labels == ["published"]

    def test_retrieve_agent_with_labels(
        self, cognite_client: CogniteClient, mock_agent_retrieve_response: MagicMock
    ) -> None:
        retrieved_agent = cognite_client.agents.retrieve("agent_1")
        assert isinstance(retrieved_agent, Agent)
        assert retrieved_agent.external_id == "agent_1"
        assert retrieved_agent.labels == ["published"]


class TestAgentSession:
    @pytest.fixture
    def chat_url(self, async_client: AsyncCogniteClient) -> str:
        return get_url(async_client.agents, async_client.agents._RESOURCE_PATH + "/chat")

    @staticmethod
    def _text_response(cursor: str | None = None, text: str = "ok") -> dict:
        return {
            "agentExternalId": "agent_1",
            "response": {
                "cursor": cursor,
                "type": "result",
                "messages": [
                    {
                        "role": "agent",
                        "content": {"type": "text", "text": text},
                    }
                ],
            },
        }

    @staticmethod
    def _client_tool_call_response(cursor: str, action_id: str, name: str = "add") -> dict:
        return {
            "agentExternalId": "agent_1",
            "response": {
                "cursor": cursor,
                "type": "result",
                "messages": [
                    {
                        "role": "agent",
                        "actions": [
                            {
                                "type": "clientTool",
                                "actionId": action_id,
                                "clientTool": {
                                    "name": name,
                                    "arguments": json.dumps({"a": 1, "b": 2}),
                                },
                            }
                        ],
                    }
                ],
            },
        }

    @staticmethod
    def _tool_confirmation_response(cursor: str, action_id: str) -> dict:
        return {
            "agentExternalId": "agent_1",
            "response": {
                "cursor": cursor,
                "type": "result",
                "messages": [
                    {
                        "role": "agent",
                        "actions": [
                            {
                                "type": "toolConfirmation",
                                "actionId": action_id,
                                "toolConfirmation": {
                                    "content": {"type": "text", "text": "Run this tool?"},
                                    "toolName": "run_func",
                                    "toolArguments": {"x": 1},
                                    "toolDescription": "Runs a function",
                                    "toolType": "callFunction",
                                },
                            }
                        ],
                    }
                ],
            },
        }

    # T008 [US2]
    def test_create_session_stores_config(self, async_client: AsyncCogniteClient) -> None:
        add = ClientToolAction(
            name="add",
            description="Add",
            parameters={"type": "object"},
        )
        session = async_client.agents.create_session(
            agent_external_id="my_agent",
            actions=[add],
            cursor="resume_cursor",
        )
        assert isinstance(session, AgentSession)
        assert session.agent_external_id == "my_agent"
        assert session.cursor == "resume_cursor"
        assert session.actions == [add]

    # T008 (companion): defaults when actions/cursor omitted
    def test_create_session_defaults(self, async_client: AsyncCogniteClient) -> None:
        session = async_client.agents.create_session(agent_external_id="x")
        assert session.agent_external_id == "x"
        assert session.cursor is None
        assert session.actions is None

    # T011 [US2]
    def test_create_session_ignores_unknown_kwargs(self, async_client: AsyncCogniteClient) -> None:
        session = async_client.agents.create_session(
            agent_external_id="x",
            bogus=True,
            dangerously_skip_user_confirmation=True,
        )
        assert isinstance(session, AgentSession)
        assert session.agent_external_id == "x"

    # T015
    def test_cursor_is_read_only(self, async_client: AsyncCogniteClient) -> None:
        session = async_client.agents.create_session(agent_external_id="x")
        with pytest.raises(AttributeError):
            session.cursor = "new"  # type: ignore[misc]

    # T016
    def test_sync_client_does_not_have_create_session(self, cognite_client: CogniteClient) -> None:
        assert not hasattr(cognite_client.agents, "create_session")

    # T020
    def test_session_has_no_reset_method(self, async_client: AsyncCogniteClient) -> None:
        session = async_client.agents.create_session(agent_external_id="x")
        assert not hasattr(session, "reset")

    # T005 [US1]
    async def test_chat_threads_cursor(
        self, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock, chat_url: str
    ) -> None:
        httpx_mock.add_response(
            method="POST", url=chat_url, status_code=200, json=self._text_response(cursor="c1", text="r1")
        )
        httpx_mock.add_response(
            method="POST", url=chat_url, status_code=200, json=self._text_response(cursor="c2", text="r2")
        )

        session = async_client.agents.create_session(agent_external_id="agent_1")

        r1 = await session.chat(Message("hi"))
        assert r1.text == "r1"
        assert session.cursor == "c1"

        r2 = await session.chat(Message("more"))
        assert r2.text == "r2"
        assert session.cursor == "c2"

        requests = httpx_mock.get_requests()
        assert len(requests) == 2
        body_1 = jsgz_load(requests[0].content)
        body_2 = jsgz_load(requests[1].content)
        assert "cursor" not in body_1
        assert body_2["cursor"] == "c1"

    # T009 [US2]
    async def test_first_request_no_cursor_when_cursor_none(
        self, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock, chat_url: str
    ) -> None:
        httpx_mock.add_response(method="POST", url=chat_url, status_code=200, json=self._text_response(cursor="c1"))
        session = async_client.agents.create_session(agent_external_id="agent_1", cursor=None)
        await session.chat(Message("hi"))
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert "cursor" not in body

    # T010 [US2]
    async def test_first_request_uses_resume_cursor(
        self, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock, chat_url: str
    ) -> None:
        httpx_mock.add_response(method="POST", url=chat_url, status_code=200, json=self._text_response(cursor="c2"))
        session = async_client.agents.create_session(agent_external_id="agent_1", cursor="resume_me")
        await session.chat(Message("continue"))
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body["cursor"] == "resume_me"

    # T007 [US1]
    async def test_null_response_cursor_retains_prior(
        self, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock, chat_url: str
    ) -> None:
        httpx_mock.add_response(method="POST", url=chat_url, status_code=200, json=self._text_response(cursor="c1"))
        httpx_mock.add_response(method="POST", url=chat_url, status_code=200, json=self._text_response(cursor=None))
        session = async_client.agents.create_session(agent_external_id="agent_1")
        await session.chat(Message("one"))
        assert session.cursor == "c1"
        await session.chat(Message("two"))
        assert session.cursor == "c1"

    # T006 [US1]
    async def test_cursor_not_advanced_on_failure(
        self, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock, chat_url: str
    ) -> None:
        httpx_mock.add_response(
            method="POST", url=chat_url, status_code=200, json=self._text_response(cursor="good_cursor")
        )
        httpx_mock.add_response(method="POST", url=chat_url, status_code=500, json={"error": {"message": "boom"}})

        session = async_client.agents.create_session(agent_external_id="agent_1")
        await session.chat(Message("hi"))
        assert session.cursor == "good_cursor"

        with pytest.raises(CogniteAPIError):
            await session.chat(Message("boom"))

        assert session.cursor == "good_cursor"

    # T012 [US2]
    async def test_actions_forwarded_to_every_chat(
        self, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock, chat_url: str
    ) -> None:
        httpx_mock.add_response(method="POST", url=chat_url, status_code=200, json=self._text_response(cursor="c1"))
        httpx_mock.add_response(method="POST", url=chat_url, status_code=200, json=self._text_response(cursor="c2"))

        add = ClientToolAction(
            name="add",
            description="Add",
            parameters={
                "type": "object",
                "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
                "required": ["a", "b"],
            },
        )
        session = async_client.agents.create_session(agent_external_id="agent_1", actions=[add])

        await session.chat(Message("one"))
        await session.chat(Message("two"))

        reqs = httpx_mock.get_requests()
        assert len(reqs) == 2
        expected_actions = [add.dump(camel_case=True)]
        for req in reqs:
            body = jsgz_load(req.content)
            assert body["actions"] == expected_actions

    # T013 [US3]
    async def test_action_result_threads_cursor(
        self, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock, chat_url: str
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=chat_url,
            status_code=200,
            json=self._client_tool_call_response(cursor="conv_1", action_id="a1"),
        )
        httpx_mock.add_response(
            method="POST", url=chat_url, status_code=200, json=self._text_response(cursor="conv_2", text="done")
        )

        session = async_client.agents.create_session(agent_external_id="agent_1")
        r = await session.chat(Message("calc"))
        assert r.action_calls is not None
        assert session.cursor == "conv_1"

        call = r.action_calls[0]
        await session.chat(ClientToolResult(action_id=call.action_id, content="3"))

        reqs = httpx_mock.get_requests()
        assert len(reqs) == 2
        assert jsgz_load(reqs[1].content)["cursor"] == "conv_1"

    # T014 [US3]
    async def test_multiple_action_results_in_one_call(
        self, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock, chat_url: str
    ) -> None:
        httpx_mock.add_response(method="POST", url=chat_url, status_code=200, json=self._text_response(cursor="final"))

        session = async_client.agents.create_session(agent_external_id="agent_1", cursor="after_calls")
        await session.chat(
            [
                ClientToolResult(action_id="a1", content="res1"),
                ClientToolResult(action_id="a2", content="res2"),
            ]
        )

        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body["cursor"] == "after_calls"
        assert len(body["messages"]) == 2

    # T019 [US3]
    async def test_tool_confirmation_pass_through_and_response(
        self, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock, chat_url: str
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=chat_url,
            status_code=200,
            json=self._tool_confirmation_response(cursor="confirm_c", action_id="conf_1"),
        )
        httpx_mock.add_response(
            method="POST", url=chat_url, status_code=200, json=self._text_response(cursor="after", text="executed")
        )

        session = async_client.agents.create_session(agent_external_id="agent_1")
        r = await session.chat(Message("run"))

        assert r.action_calls is not None
        assert len(r.action_calls) == 1
        assert isinstance(r.action_calls[0], ToolConfirmationCall)
        assert r.action_calls[0].action_id == "conf_1"
        assert len(httpx_mock.get_requests()) == 1  # no auto-confirmation

        await session.chat(ToolConfirmationResult(action_id="conf_1", status="ALLOW"))

        reqs = httpx_mock.get_requests()
        assert len(reqs) == 2
        assert jsgz_load(reqs[1].content)["cursor"] == "confirm_c"

    # T021 [US3]
    async def test_three_action_rounds_thread_cursor(
        self, async_client: AsyncCogniteClient, httpx_mock: HTTPXMock, chat_url: str
    ) -> None:
        for i in range(3):
            httpx_mock.add_response(
                method="POST",
                url=chat_url,
                status_code=200,
                json=self._client_tool_call_response(cursor=f"c{i + 1}", action_id=f"call_{i + 1}"),
            )
        httpx_mock.add_response(
            method="POST", url=chat_url, status_code=200, json=self._text_response(cursor="c_final", text="done")
        )

        session = async_client.agents.create_session(agent_external_id="agent_1")

        r = await session.chat(Message("step_1"))
        assert r.action_calls is not None
        assert session.cursor == "c1"

        r = await session.chat(ClientToolResult(action_id=r.action_calls[0].action_id, content="ok"))
        assert r.action_calls is not None
        assert session.cursor == "c2"

        r = await session.chat(ClientToolResult(action_id=r.action_calls[0].action_id, content="ok"))
        assert r.action_calls is not None
        assert session.cursor == "c3"

        r = await session.chat(ClientToolResult(action_id=r.action_calls[0].action_id, content="ok"))
        assert session.cursor == "c_final"

        reqs = httpx_mock.get_requests()
        assert len(reqs) == 4
        assert "cursor" not in jsgz_load(reqs[0].content)
        assert jsgz_load(reqs[1].content)["cursor"] == "c1"
        assert jsgz_load(reqs[2].content)["cursor"] == "c2"
        assert jsgz_load(reqs[3].content)["cursor"] == "c3"
