from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.agents import Message
from cognite.client.data_classes.agents.chat import (
    Action,
    ActionCall,
    AgentChatResponse,
    ClientToolAction,
    ClientToolCall,
    ClientToolResult,
    TextContent,
    UnknownActionCall,
)
from tests.utils import jsgz_load


@pytest.fixture
def action_call_response_body() -> dict:
    """Response body when agent requests an action."""
    return {
        "agentId": "my_agent",
        "agentExternalId": "my_agent",
        "response": {
            "cursor": "cursor_12345",
            "messages": [
                {
                    "content": {"text": "", "type": "text"},
                    "actions": [
                        {
                            "type": "clientTool",
                            "actionId": "call_abc123",
                            "clientTool": {
                                "name": "add",
                                "arguments": '{"a": 42, "b": 58}',
                            },
                        }
                    ],
                    "role": "agent",
                }
            ],
            "type": "result",
        },
    }


@pytest.fixture
def final_response_body() -> dict:
    """Final response after action execution."""
    return {
        "agentId": "my_agent",
        "agentExternalId": "my_agent",
        "response": {
            "cursor": "cursor_67890",
            "messages": [
                {
                    "content": {"text": "The result is 100.", "type": "text"},
                    "role": "agent",
                }
            ],
            "type": "result",
        },
    }


@pytest.fixture
def mock_action_call_response(
    rsps: MagicMock, cognite_client: CogniteClient, action_call_response_body: dict
) -> MagicMock:
    """Mock HTTP response for agent chat that returns an action call."""
    url = cognite_client.agents._get_base_url_with_base_path() + cognite_client.agents._RESOURCE_PATH + "/chat"
    rsps.add(rsps.POST, url, status=200, json=action_call_response_body)
    yield rsps


@pytest.fixture
def mock_final_response(rsps: MagicMock, cognite_client: CogniteClient, final_response_body: dict) -> MagicMock:
    """Mock HTTP response for final agent response."""
    url = cognite_client.agents._get_base_url_with_base_path() + cognite_client.agents._RESOURCE_PATH + "/chat"
    rsps.add(rsps.POST, url, status=200, json=final_response_body)
    yield rsps


class TestClientToolAction:
    def test_load_and_dump(self) -> None:
        data = {
            "type": "clientTool",
            "clientTool": {
                "name": "add",
                "description": "Add two numbers",
                "parameters": {"type": "object", "properties": {"a": {"type": "number"}}, "required": ["a"]},
            },
        }
        action = Action._load(data)
        assert isinstance(action, ClientToolAction)


class TestClientToolCall:
    def test_load_and_dump(self) -> None:
        data = {
            "type": "clientTool",
            "actionId": "call_456",
            "clientTool": {"name": "multiply", "arguments": '{"x": 5}'},
        }
        call = ActionCall._load(data)
        assert isinstance(call, ClientToolCall)


class TestClientToolResult:
    def test_init_with_string_converts_to_text_content(self) -> None:
        """Test that ClientToolResult accepts a string and converts it to TextContent."""
        result = ClientToolResult(action_id="call_456", content="Result: 100")
        assert isinstance(result.content, TextContent)
        assert result.content.text == "Result: 100"


class TestUnknownActionCall:
    def test_load_and_dump(self) -> None:
        data = {"type": "unknownActionType", "actionId": "call_999", "someField": "someValue"}
        call = ActionCall._load(data)
        assert isinstance(call, UnknownActionCall)


class TestChatWithActions:
    def test_chat_with_actions_parameter(
        self, cognite_client: CogniteClient, mock_action_call_response: MagicMock
    ) -> None:
        response = cognite_client.agents.chat(
            agent_external_id="my_agent",
            messages=Message("What is 42 plus 58?"),
            actions=[
                ClientToolAction(
                    name="add",
                    description="Add two numbers",
                    parameters={
                        "type": "object",
                        "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
                        "required": ["a", "b"],
                    },
                )
            ],
        )
        request_body = jsgz_load(mock_action_call_response.calls[-1].request.body)
        assert request_body["actions"][0]["clientTool"]["name"] == "add"
        assert isinstance(response, AgentChatResponse)
        assert len(response.action_calls) == 1
        assert isinstance(response.action_calls[0], ClientToolCall)

    def test_chat_with_action_result_message(
        self, cognite_client: CogniteClient, mock_final_response: MagicMock
    ) -> None:
        response = cognite_client.agents.chat(
            agent_external_id="my_agent",
            messages=ClientToolResult(action_id="call_abc123", content="The result is 100"),
            cursor="cursor_12345",
            actions=[ClientToolAction(name="add", description="Add two numbers", parameters={"type": "object"})],
        )
        request_body = jsgz_load(mock_final_response.calls[-1].request.body)
        assert request_body["cursor"] == "cursor_12345"
        assert request_body["messages"][0]["actionId"] == "call_abc123"
        assert response.text == "The result is 100."
