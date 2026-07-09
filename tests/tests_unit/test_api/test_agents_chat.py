from __future__ import annotations

import base64
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.agents import ImageContent, Message
from cognite.client.data_classes.agents.chat import (
    AgentChatResponse,
    AgentDataItem,
    AgentMessage,
    AgentReasoningItem,
    TextContent,
    ToolCallReasoningDataItem,
)
from tests.utils import get_url, jsgz_load

if TYPE_CHECKING:
    from cognite.client import CogniteClient


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
                            ],
                            "data": [
                                {
                                    "type": "toolCall",
                                    "toolCall": {
                                        "id": "tc_1",
                                        "name": "search_instances",
                                        "toolType": "query",
                                        "input": {
                                            "view_space": "cdf_cdm",
                                            "view_external_id": "CogniteAsset",
                                            "view_version": "v1",
                                            "query": "pump",
                                            "operator": "AND",
                                            "return_properties": ["name", "externalId"],
                                        },
                                        "result": {
                                            "result": {
                                                "items": [{"space": "my_space", "externalId": "pump_1"}],
                                                "count": 1,
                                            },
                                            "error": None,
                                        },
                                    },
                                }
                            ],
                        }
                    ],
                    "role": "agent",
                }
            ],
            "type": "result",
        },
    }


class TestAgentChat:
    def test_chat_simple_message(
        self,
        httpx_mock: HTTPXMock,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        chat_response_body: dict,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=get_url(async_client.agents, async_client.agents._RESOURCE_PATH + "/chat"),
            status_code=200,
            json=chat_response_body,
        )

        # Test with simple string message
        response = cognite_client.agents.chat(
            agent_external_id="my_agent", messages=Message("What can you help me with?")
        )

        request = httpx_mock.get_requests()[0]
        payload = jsgz_load(request.content)
        assert payload == {
            "agentExternalId": "my_agent",
            "messages": [{"content": {"text": "What can you help me with?", "type": "text"}, "role": "user"}],
        }
        assert request.url.path == "/api/v1/projects/dummy/ai/agents/chat"

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
        reasoning_item = agent_msg.reasoning[0]
        assert isinstance(reasoning_item, AgentReasoningItem)
        assert isinstance(reasoning_item.content[0], TextContent)
        assert reasoning_item.data is not None
        assert isinstance(reasoning_item.data[0], ToolCallReasoningDataItem)

        # Test convenience properties
        assert response.text == "I can help you with various tasks related to your industrial data."

    def test_chat_with_cursor(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, chat_response_body: dict
    ) -> None:
        async_client.agents._post = AsyncMock(return_value=MagicMock(json=lambda: chat_response_body))  # type: ignore[method-assign]

        # Test with cursor
        cognite_client.agents.chat(
            agent_external_id="my_agent",
            messages=Message("Tell me more"),
            cursor="previous_cursor_123",
        )

        # Verify cursor was included in request
        call_args = async_client.agents._post.call_args
        assert call_args[1]["json"]["cursor"] == "previous_cursor_123"

    def test_chat_multiple_messages(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient, chat_response_body: dict
    ) -> None:
        async_client.agents._post = AsyncMock(return_value=MagicMock(json=lambda: chat_response_body))  # type: ignore[method-assign]

        # Test with multiple messages
        messages = [
            Message("I need help with time series data"),
            Message("Specifically about temperature sensors"),
        ]
        cognite_client.agents.chat(agent_external_id="my_agent", messages=messages)

        # Verify multiple messages were sent
        call_args = async_client.agents._post.call_args
        assert len(call_args[1]["json"]["messages"]) == 2
        assert call_args[1]["json"]["messages"][0]["content"]["text"] == "I need help with time series data"
        assert call_args[1]["json"]["messages"][1]["content"]["text"] == "Specifically about temperature sensors"

    def test_chat_response_without_optional_fields(
        self, cognite_client: CogniteClient, async_client: AsyncCogniteClient
    ) -> None:
        # Minimal response without data or reasoning
        minimal_response = {
            "agentExternalId": "my_agent",
            "response": {
                "cursor": None,
                "type": "result",
                "messages": [
                    {
                        "content": {"text": "Simple response", "type": "text"},
                        "role": "agent",
                    }
                ],
            },
        }
        async_client.agents._post = AsyncMock(return_value=MagicMock(json=lambda: minimal_response))  # type: ignore[method-assign]

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
        assert isinstance(msg.content, TextContent)
        assert msg.content.text == "Hello world"


class TestImageContent:
    def test_from_bytes(self) -> None:
        image_bytes = b"fake-image-bytes"
        content = ImageContent.from_bytes(image_bytes, media_type="image/png")

        assert content.data == base64.b64encode(image_bytes).decode()
        assert content.media_type == "image/png"
        assert content.dump() == {
            "data": base64.b64encode(image_bytes).decode(),
            "mediaType": "image/png",
            "type": "image",
        }

    def test_from_bytes_invalid_media_type(self) -> None:
        with pytest.raises(ValueError, match="Unsupported media type"):
            ImageContent.from_bytes(b"data", media_type="image/gif")

    def test_from_file(self, tmp_path: Path) -> None:
        image_bytes = b"fake-image-bytes"
        image_path = tmp_path / "diagram.png"
        image_path.write_bytes(image_bytes)

        content = ImageContent.from_file(image_path)

        assert content.data == base64.b64encode(image_bytes).decode()
        assert content.media_type == "image/png"

    def test_from_file_with_explicit_media_type(self, tmp_path: Path) -> None:
        image_bytes = b"fake-image-bytes"
        image_path = tmp_path / "diagram.bin"
        image_path.write_bytes(image_bytes)

        content = ImageContent.from_file(image_path, media_type="image/webp")

        assert content.media_type == "image/webp"

    def test_from_file_unknown_suffix(self, tmp_path: Path) -> None:
        image_path = tmp_path / "diagram.bin"
        image_path.write_bytes(b"data")

        with pytest.raises(ValueError, match="Could not infer media type"):
            ImageContent.from_file(image_path)

    def test_load_from_api_response(self) -> None:
        data = {"type": "image", "data": "aGVsbG8=", "mediaType": "image/jpeg"}
        content = ImageContent._load(data)

        assert isinstance(content, ImageContent)
        assert content.data == "aGVsbG8="
        assert content.media_type == "image/jpeg"


class TestMultimodalMessage:
    def test_message_with_content_parts_dump(self) -> None:
        image_bytes = b"fake-image-bytes"
        message = Message(
            [
                TextContent(text="What's in this image?"),
                ImageContent.from_bytes(image_bytes, media_type="image/png"),
            ]
        )

        dumped = message.dump()

        assert dumped == {
            "role": "user",
            "content": [
                {"text": "What's in this image?", "type": "text"},
                {
                    "data": base64.b64encode(image_bytes).decode(),
                    "mediaType": "image/png",
                    "type": "image",
                },
            ],
        }

    def test_text_only_message_still_dumps_single_object(self) -> None:
        message = Message("Hello world")

        assert message.dump() == {
            "role": "user",
            "content": {"text": "Hello world", "type": "text"},
        }

    def test_message_with_string_content_parts(self) -> None:
        image_bytes = b"fake-image-bytes"
        message = Message(
            [
                "What's in this image?",
                ImageContent.from_bytes(image_bytes, media_type="image/png"),
            ]
        )

        assert isinstance(message.content, list)
        assert isinstance(message.content[0], TextContent)
        assert message.content[0].text == "What's in this image?"
        assert isinstance(message.content[1], ImageContent)

    def test_message_with_invalid_content_part_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="Expected str or MessageContent, got int"):
            Message([123])

    def test_chat_with_multimodal_message(
        self,
        httpx_mock: HTTPXMock,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        chat_response_body: dict,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=get_url(async_client.agents, async_client.agents._RESOURCE_PATH + "/chat"),
            status_code=200,
            json=chat_response_body,
        )

        image_bytes = b"fake-image-bytes"
        cognite_client.agents.chat(
            agent_external_id="my_agent",
            messages=Message(
                [
                    TextContent(text="Describe this image"),
                    ImageContent.from_bytes(image_bytes, media_type="image/jpeg"),
                ]
            ),
        )

        request = httpx_mock.get_requests()[0]
        payload = jsgz_load(request.content)
        assert payload == {
            "agentExternalId": "my_agent",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"text": "Describe this image", "type": "text"},
                        {
                            "data": base64.b64encode(image_bytes).decode(),
                            "mediaType": "image/jpeg",
                            "type": "image",
                        },
                    ],
                }
            ],
        }

    def test_load_agent_message_with_content_parts(self) -> None:
        response = AgentChatResponse._load(
            {
                "agentExternalId": "my_agent",
                "response": {
                    "type": "result",
                    "messages": [
                        {
                            "role": "agent",
                            "content": [
                                {"text": "I see a pump in the image.", "type": "text"},
                                {"data": "aGVsbG8=", "mediaType": "image/png", "type": "image"},
                            ],
                        }
                    ],
                },
            }
        )

        assert response.text == "I see a pump in the image."
        content = response.messages[0].content
        assert isinstance(content, list)
        assert isinstance(content[0], TextContent)
        assert isinstance(content[1], ImageContent)
        assert content[0].text == "I see a pump in the image."
        assert content[1].data == "aGVsbG8="
        assert content[1].media_type == "image/png"
