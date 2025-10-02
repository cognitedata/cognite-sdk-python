from __future__ import annotations

import json
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
    ToolConfirmationCall,
    ToolConfirmationResult,
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
    def test_chat_with_actions_parameter(
        self, cognite_client: CogniteClient, mock_action_call_response: MagicMock
    ) -> None:
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
            agent_external_id="my_agent",
            messages=Message("What is 42 plus 58?"),
            actions=[add_action],
        )

        # Verify actions were sent in request
        request_body = jsgz_load(mock_action_call_response.calls[-1].request.body)
        assert "actions" in request_body
        assert len(request_body["actions"]) == 1
        assert request_body["actions"][0]["type"] == "clientTool"
        assert request_body["actions"][0]["clientTool"]["name"] == "add"

        # Verify response
        assert isinstance(response, AgentChatResponse)
        assert response.action_calls is not None
        assert len(response.action_calls) == 1

    def test_chat_with_action_result_message(
        self, cognite_client: CogniteClient, mock_final_response: MagicMock
    ) -> None:
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
            agent_external_id="my_agent",
            messages=result,
            cursor="cursor_12345",
            actions=[add_action],
        )

        # Verify action result was sent
        request_body = jsgz_load(mock_final_response.calls[-1].request.body)
        assert request_body["cursor"] == "cursor_12345"
        assert len(request_body["messages"]) == 1
        msg = request_body["messages"][0]
        assert msg["role"] == "action"
        assert msg["type"] == "clientTool"
        assert msg["actionId"] == "call_abc123"

        # Verify response
        assert isinstance(response, AgentChatResponse)
        assert response.text == "The result is 100."


class TestToolConfirmationCall:
    def test_load_and_dump(self) -> None:
        data = {
            "actionId": "call_123",
            "type": "toolConfirmation",
            "toolConfirmation": {
                "content": {"type": "text", "text": "Please confirm the action."},
                "toolName": "Add",
                "toolArguments": {"number1": 48, "number2": 82},
                "toolDescription": "This is a simple calculator that adds two numbers.",
                "toolType": "runPythonCode",
            },
        }

        call = ActionCall._load(data)

        assert isinstance(call, ToolConfirmationCall)
        assert call.action_id == "call_123"
        assert call.tool_name == "Add"
        assert call.tool_type == "runPythonCode"
        assert call.details is None
        assert call.dump(camel_case=True) == data

    def test_load_with_rest_api_details(self) -> None:
        data = {
            "actionId": "call_456",
            "type": "toolConfirmation",
            "toolConfirmation": {
                "content": {"type": "text", "text": "Confirm?"},
                "toolName": "Work with databases",
                "toolArguments": {"limit": 25},
                "toolDescription": "Tool to retrieve data.",
                "toolType": "callRestApi",
                "details": {"endpointMethod": "GET", "endpointPath": "/raw/dbs"},
            },
        }

        call = ActionCall._load(data)

        assert isinstance(call, ToolConfirmationCall)
        assert call.details == {"endpointMethod": "GET", "endpointPath": "/raw/dbs"}
        assert call.dump(camel_case=True) == data


class TestToolConfirmationResult:
    def test_load_and_dump_allow(self) -> None:
        data = {
            "role": "action",
            "type": "toolConfirmation",
            "actionId": "call_123",
            "status": "ALLOW",
        }

        result = ToolConfirmationResult._load_result(data)

        assert isinstance(result, ToolConfirmationResult)
        assert result.status == "ALLOW"
        assert result.dump(camel_case=True) == data

    def test_load_and_dump_deny(self) -> None:
        data = {
            "role": "action",
            "type": "toolConfirmation",
            "actionId": "call_456",
            "status": "DENY",
        }

        result = ToolConfirmationResult._load_result(data)

        assert isinstance(result, ToolConfirmationResult)
        assert result.status == "DENY"
        assert result.dump(camel_case=True) == data
