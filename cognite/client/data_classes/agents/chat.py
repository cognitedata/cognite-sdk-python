from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from cognite.client.data_classes._base import CogniteObject
from cognite.client.utils._text import convert_all_keys_to_camel_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient


@dataclass
class MessageContent(CogniteObject, ABC):
    """Base class for message content types."""

    type: str

    @abstractmethod
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the content to a dictionary."""
        ...


@dataclass
class TextContent(MessageContent):
    """Text content for messages.

    Args:
        text (str): The text content.
        type (Literal["text"]): The content type. Defaults to "text".
    """

    type: Literal["text"] = "text"
    text: str = ""

    def __init__(self, text: str, type: Literal["text"] = "text") -> None:
        super().__init__(type=type)
        self.text = text

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"text": self.text, "type": self.type}

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> TextContent:
        return cls(text=data["text"])


@dataclass
class UnknownContent(MessageContent):
    """Unknown content type for forward compatibility.

    Args:
        data (dict[str, Any]): The raw content data.
        type (str): The content type.
    """

    type: str = "unknown"
    data: dict[str, Any] = None  # type: ignore[assignment]

    def __init__(self, data: dict[str, Any], type: str = "unknown") -> None:
        super().__init__(type=type)
        self.data = data

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = self.data.copy()
        result["type"] = self.type
        return result

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> UnknownContent:
        content_type = data.get("type", "unknown")
        return cls(data=data, type=content_type)


MessageContentType = TextContent | UnknownContent


def load_message_content(data: dict[str, Any]) -> MessageContentType:
    """Load message content from a dictionary."""
    content_type = data.get("type")
    if content_type == "text":
        return TextContent._load(data)
    else:
        return UnknownContent._load(data)


@dataclass
class Message(CogniteObject):
    """A message to send to an agent.

    Args:
        content (str | MessageContentType): The message content. If a string is provided,
            it will be converted to TextContent.
        role (Literal["user"]): The role of the message sender. Defaults to "user".
    """

    content: MessageContentType | None = None
    role: Literal["user"] = "user"

    def __init__(self, content: str | MessageContentType, role: Literal["user"] = "user") -> None:
        if isinstance(content, str):
            self.content = TextContent(text=content)
        else:
            self.content = content
        self.role = role

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "content": self.content.dump(camel_case=camel_case) if self.content else None,
            "role": self.role,
        }

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> Message:
        content = load_message_content(data["content"])
        return cls(content=content, role=data.get("role", "user"))


class MessageList(list[Message]):
    """List of messages."""

    def dump(self, camel_case: bool = True) -> list[dict[str, Any]]:
        return [item.dump(camel_case=camel_case) for item in self]


@dataclass
class AgentDataItem(CogniteObject):
    """Data item in agent response.

    Args:
        type (str): The type of data item.
        data (dict[str, Any]): The data payload.
    """

    type: str
    data: dict[str, Any]

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = {"type": self.type}
        if self.data:
            result.update(self.data if not camel_case else convert_all_keys_to_camel_case(self.data))
        return result

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentDataItem:
        item_type = data.get("type", "unknown")
        item_data = {k: v for k, v in data.items() if k != "type"}
        return cls(type=item_type, data=item_data)


@dataclass
class AgentReasoningItem(CogniteObject):
    """Reasoning item in agent response.

    Args:
        content (list[MessageContentType]): The reasoning content.
    """

    content: list[MessageContentType]

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "content": [item.dump(camel_case=camel_case) for item in self.content],
        }

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentReasoningItem:
        content = [load_message_content(item) for item in data.get("content", [])]
        return cls(content=content)


@dataclass
class AgentMessage(CogniteObject):
    """A message from an agent.

    Args:
        content (MessageContentType | None): The message content.
        data (list[AgentDataItem] | None): Data items in the response.
        reasoning (list[AgentReasoningItem] | None): Reasoning items in the response.
        role (Literal["agent"]): The role of the message sender.
    """

    content: MessageContentType | None = None
    data: list[AgentDataItem] | None = None
    reasoning: list[AgentReasoningItem] | None = None
    role: Literal["agent"] = "agent"

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {"role": self.role}
        if self.content is not None:
            result["content"] = self.content.dump(camel_case=camel_case)
        if self.data is not None:
            result["data"] = [item.dump(camel_case=camel_case) for item in self.data]
        if self.reasoning is not None:
            result["reasoning"] = [item.dump(camel_case=camel_case) for item in self.reasoning]
        return result

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentMessage:
        content = load_message_content(data["content"]) if "content" in data else None
        data_items = [AgentDataItem._load(item, cognite_client) for item in data.get("data", [])]
        reasoning_items = [AgentReasoningItem._load(item, cognite_client) for item in data.get("reasoning", [])]
        return cls(
            content=content,
            data=data_items if data_items else None,
            reasoning=reasoning_items if reasoning_items else None,
            role=data.get("role", "agent"),
        )


class AgentMessageList(list[AgentMessage]):
    """List of agent messages."""


@dataclass
class AgentChatResponse(CogniteObject):
    """Response from agent chat.

    Args:
        agent_id (str): The ID of the agent.
        cursor (str | None): Cursor for conversation continuation.
        messages (list[AgentMessage] | AgentMessageList | None): The response messages from the agent.
        type (str): The response type.
    """

    agent_id: str
    cursor: str | None = None
    messages: AgentMessageList | None = None
    type: str = "result"

    def __init__(
        self,
        agent_id: str,
        cursor: str | None = None,
        messages: list[AgentMessage] | AgentMessageList | None = None,
        type: str = "result",
    ) -> None:
        self.agent_id = agent_id
        self.cursor = cursor
        if messages is None:
            self.messages = AgentMessageList()
        elif isinstance(messages, AgentMessageList):
            self.messages = messages
        else:
            self.messages = AgentMessageList(messages)
        self.type = type
        # Store unknown properties for forward compatibility
        self._unknown_properties: dict[str, Any] = {}

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = {
            "agentId" if camel_case else "agent_id": self.agent_id,
            "response": {
                "cursor": self.cursor,
                "messages": [msg.dump(camel_case=camel_case) for msg in self.messages] if self.messages else [],
                "type": self.type,
            },
        }
        if self._unknown_properties:
            result.update(self._unknown_properties)
        return result

    @property
    def text(self) -> str | None:
        """Get the text content from the first message with text content."""
        if self.messages:
            for message in self.messages:
                if message.content and isinstance(message.content, TextContent):
                    return message.content.text
        return None

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentChatResponse:
        response_data = data.get("response", {})
        messages = [AgentMessage._load(msg, cognite_client) for msg in response_data.get("messages", [])]

        instance = cls(
            agent_id=data["agentId"],
            cursor=response_data.get("cursor"),
            messages=messages,
            type=response_data.get("type", "result"),
        )

        # Store any unknown properties
        known_keys = {"agentId", "response"}
        instance._unknown_properties = {k: v for k, v in data.items() if k not in known_keys}

        return instance
