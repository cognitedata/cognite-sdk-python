from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.agents import Message
from cognite.client.data_classes.agents.chat import (
    AgentChatResponse,
    AgentDataItem,
    AgentMessage,
    AgentReasoningItem,
    TextContent,
)


@pytest.fixture
def chat_response_body() -> dict:
    return {
        "agentExternalId": "my_agent",
        "response": {
            "cursor": "cursor_12345",
            "messages": [
                {
                    "content": {
                        "text": "I can help you with various tasks related to your industrial data.",
                        "type": "text",
                    },
                    "data": [
                        {
                            "type": "instances",
                            "instances": {
                                "nodes": [
                                    {"space": "my_space", "externalId": "asset_1"},
                                    {"space": "my_space", "externalId": "asset_2"},
                                ]
                            },
                        }
                    ],
                    "reasoning": [
                        {
                            "content": [
                                {
                                    "text": "The user is asking about capabilities",
                                    "type": "text",
                                }
                            ]
                        }
                    ],
                    "role": "agent",
                }
            ],
            "type": "result",
        },
    }


class TestAgentChat:
    def test_chat_simple_message(self, cognite_client: CogniteClient, chat_response_body: dict) -> None:
        # Mock the API response
        cognite_client.agents._post = MagicMock(return_value=MagicMock(json=lambda: chat_response_body))

        # Test with simple string message
        response = cognite_client.agents.chat(
            agent_external_id="my_agent", messages=Message("What can you help me with?")
        )

        # Verify the request
        cognite_client.agents._post.assert_called_once()
        call_args = cognite_client.agents._post.call_args
        assert call_args[1]["url_path"] == "/ai/agents/chat"
        assert call_args[1]["json"]["agentExternalId"] == "my_agent"
        assert len(call_args[1]["json"]["messages"]) == 1
        assert call_args[1]["json"]["messages"][0]["content"]["text"] == "What can you help me with?"
        assert call_args[1]["json"]["messages"][0]["content"]["type"] == "text"
        assert call_args[1]["json"]["messages"][0]["role"] == "user"

        # Verify the response
        assert isinstance(response, AgentChatResponse)
        assert response.agent_external_id == "my_agent"
        assert response.cursor == "cursor_12345"
        assert response.type == "result"
        assert len(response.messages) == 1

        # Check the agent message
        agent_msg = response.messages[0]
        assert isinstance(agent_msg, AgentMessage)
        assert agent_msg.role == "agent"
        assert isinstance(agent_msg.content, TextContent)
        assert agent_msg.content.text == "I can help you with various tasks related to your industrial data."

        # Check data items
        assert agent_msg.data is not None
        assert len(agent_msg.data) == 1
        assert isinstance(agent_msg.data[0], AgentDataItem)
        assert agent_msg.data[0].type == "instances"

        # Check reasoning
        assert agent_msg.reasoning is not None
        assert len(agent_msg.reasoning) == 1
        assert isinstance(agent_msg.reasoning[0], AgentReasoningItem)
        assert len(agent_msg.reasoning[0].content) == 1
        assert agent_msg.reasoning[0].content[0].text == "The user is asking about capabilities"

        # Test convenience properties
        assert response.text == "I can help you with various tasks related to your industrial data."

    def test_chat_with_cursor(self, cognite_client: CogniteClient, chat_response_body: dict) -> None:
        cognite_client.agents._post = MagicMock(return_value=MagicMock(json=lambda: chat_response_body))

        # Test with cursor
        cognite_client.agents.chat(
            agent_external_id="my_agent",
            messages=Message("Tell me more"),
            cursor="previous_cursor_123",
        )

        # Verify cursor was included in request
        call_args = cognite_client.agents._post.call_args
        assert call_args[1]["json"]["cursor"] == "previous_cursor_123"

    def test_chat_multiple_messages(self, cognite_client: CogniteClient, chat_response_body: dict) -> None:
        cognite_client.agents._post = MagicMock(return_value=MagicMock(json=lambda: chat_response_body))

        # Test with multiple messages
        messages = [
            Message("I need help with time series data"),
            Message("Specifically about temperature sensors"),
        ]
        cognite_client.agents.chat(agent_external_id="my_agent", messages=messages)

        # Verify multiple messages were sent
        call_args = cognite_client.agents._post.call_args
        assert len(call_args[1]["json"]["messages"]) == 2
        assert call_args[1]["json"]["messages"][0]["content"]["text"] == "I need help with time series data"
        assert call_args[1]["json"]["messages"][1]["content"]["text"] == "Specifically about temperature sensors"

    def test_chat_response_without_optional_fields(self, cognite_client: CogniteClient) -> None:
        # Minimal response without data or reasoning
        minimal_response = {
            "agentExternalId": "my_agent",
            "response": {
                "cursor": None,
                "messages": [
                    {
                        "content": {
                            "text": "Simple response",
                            "type": "text",
                        },
                        "role": "agent",
                    }
                ],
                "type": "result",
            },
        }

        cognite_client.agents._post = MagicMock(return_value=MagicMock(json=lambda: minimal_response))

        response = cognite_client.agents.chat(agent_external_id="my_agent", messages=Message("Hello"))

        assert response.cursor is None
        assert response.messages[0].data is None
        assert response.messages[0].reasoning is None
        assert response.text == "Simple response"

    def test_message_creation_from_string(self) -> None:
        # Test that string is automatically converted to TextContent
        msg = Message("Hello world")
        assert isinstance(msg.content, TextContent)
        assert msg.content.text == "Hello world"
        assert msg.role == "user"

    def test_message_with_explicit_content(self) -> None:
        # Test with explicit TextContent
        content = TextContent(text="Hello world")
        msg = Message(content=content)
        assert msg.content is content
        assert msg.content.text == "Hello world"
