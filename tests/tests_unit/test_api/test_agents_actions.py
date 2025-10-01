from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.agents import Message
from cognite.client.data_classes.agents.chat import (
    ActionCall,
    AgentChatResponse,
    ClientToolAction,
    ClientToolCall,
    ClientToolResult,
    TextContent,
    UnknownActionCall,
)


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


class TestClientToolCall:
    def test_dump_serializes_arguments_to_json(self) -> None:
        call = ClientToolCall(
            action_id="call_456",
            name="multiply",
            arguments={"x": 5, "y": 7},
        )

        dumped = call.dump(camel_case=True)

        assert dumped["type"] == "clientTool"
        assert dumped["actionId"] == "call_456"
        assert dumped["clientTool"]["name"] == "multiply"
        # Arguments should be JSON string
        import json

        assert json.loads(dumped["clientTool"]["arguments"]) == {"x": 5, "y": 7}


class TestActionCallPolymorphism:
    def test_unknown_action_call_loaded_for_unknown_type(self) -> None:
        data = {
            "type": "unknownActionType",
            "actionId": "call_999",
            "someField": "someValue",
        }

        call = ActionCall._load(data)

        assert isinstance(call, UnknownActionCall)
        assert call.action_id == "call_999"
        assert call.type == "unknownActionType"


class TestClientToolResult:
    def test_init_with_message_content(self) -> None:
        text_content = TextContent(text="Result: 100")
        result = ClientToolResult(
            action_id="call_456",
            content=text_content,
        )

        assert result.action_id == "call_456"
        assert result.content is text_content
        assert result.content.text == "Result: 100"

    def test_load(self) -> None:
        data = {
            "type": "clientTool",
            "actionId": "call_abc",
            "role": "action",
            "content": {"text": "Done", "type": "text"},
            "data": [],
        }

        result = ClientToolResult._load_result(data)

        assert isinstance(result, ClientToolResult)
        assert result.action_id == "call_abc"
        assert result.content.text == "Done"


class TestChatWithActions:
    def test_chat_with_actions_parameter(self, cognite_client: CogniteClient, action_call_response_body: dict) -> None:
        cognite_client.agents._post = MagicMock(return_value=MagicMock(json=lambda: action_call_response_body))

        add_action = ClientToolAction(
            name="add",
            description="Add two numbers",
            parameters={
                "type": "object",
                "properties": {
                    "a": {"type": "number"},
                    "b": {"type": "number"},
                },
                "required": ["a", "b"],
            },
        )

        response = cognite_client.agents.chat(
            agent_id="my_agent",
            messages=Message("What is 42 plus 58?"),
            actions=[add_action],
        )

        # Verify actions were sent in request
        call_args = cognite_client.agents._post.call_args
        assert "actions" in call_args[1]["json"]
        assert len(call_args[1]["json"]["actions"]) == 1
        assert call_args[1]["json"]["actions"][0]["type"] == "clientTool"
        assert call_args[1]["json"]["actions"][0]["clientTool"]["name"] == "add"

        # Verify response
        assert isinstance(response, AgentChatResponse)
        assert response.action_calls is not None
        assert len(response.action_calls) == 1

    def test_chat_with_action_result_message(self, cognite_client: CogniteClient, final_response_body: dict) -> None:
        cognite_client.agents._post = MagicMock(return_value=MagicMock(json=lambda: final_response_body))

        add_action = ClientToolAction(
            name="add",
            description="Add two numbers",
            parameters={"type": "object", "properties": {"a": {"type": "number"}, "b": {"type": "number"}}},
        )

        result = ClientToolResult(
            action_id="call_abc123",
            content="The result is 100",
        )

        response = cognite_client.agents.chat(
            agent_id="my_agent",
            messages=result,
            cursor="cursor_12345",
            actions=[add_action],
        )

        # Verify action result was sent
        call_args = cognite_client.agents._post.call_args
        assert call_args[1]["json"]["cursor"] == "cursor_12345"
        assert len(call_args[1]["json"]["messages"]) == 1
        msg = call_args[1]["json"]["messages"][0]
        assert msg["role"] == "action"
        assert msg["type"] == "clientTool"
        assert msg["actionId"] == "call_abc123"

        # Verify response
        assert isinstance(response, AgentChatResponse)
        assert response.text == "The result is 100."
