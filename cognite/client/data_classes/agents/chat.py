from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from cognite.client.data_classes._base import CogniteObject, CogniteResource, CogniteResourceList
from cognite.client.utils._text import convert_all_keys_to_camel_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient


@dataclass
class MessageContent(CogniteObject, ABC):
    """Base class for message content types."""

    _type: ClassVar[str]  # To be set by concrete classes

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> MessageContent:
        """Dispatch to the correct concrete content class based on `type`."""
        content_type = data.get("type", "")
        content_class = _MESSAGE_CONTENT_CLS_BY_TYPE.get(content_type, UnknownContent)
        # Delegate to the concrete class' loader
        return content_class._load_content(data)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the content to a dictionary."""
        output = super().dump(camel_case=camel_case)
        output["type"] = self._type
        return output

    @classmethod
    @abstractmethod
    def _load_content(cls, data: dict[str, Any]) -> MessageContent:
        """Create a concrete content instance from raw data."""
        ...


@dataclass
class TextContent(MessageContent):
    """Text content for messages.

    Args:
        text (str): The text content.
    """

    _type: ClassVar[str] = "text"
    text: str = ""

    @classmethod
    def _load_content(cls, data: dict[str, Any]) -> TextContent:
        return cls(text=data["text"])


@dataclass
class UnknownContent(MessageContent):
    """Unknown content type for forward compatibility.

    Args:
        data (dict[str, Any]): The raw content data.
        type (str): The content type.
    """

    type: str
    data: dict[str, Any] = field(default_factory=dict)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = self.data.copy()
        result["type"] = self.type
        return result

    @classmethod
    def _load_content(cls, data: dict[str, Any]) -> UnknownContent:
        content_type = data.get("type", "")
        return cls(data=data, type=content_type)


# Build the mapping AFTER concrete classes are defined
_MESSAGE_CONTENT_CLS_BY_TYPE: dict[str, type[MessageContent]] = {
    subclass._type: subclass  # type: ignore[type-abstract]
    for subclass in MessageContent.__subclasses__()
    if hasattr(subclass, "_type") and not getattr(subclass, "__abstractmethods__", None)
}


@dataclass
class Message(CogniteResource):
    """A message to send to an agent.

    Args:
        content (str | MessageContent): The message content. If a string is provided,
            it will be converted to TextContent.
        role (Literal["user"]): The role of the message sender. Defaults to "user".
    """

    content: MessageContent
    role: Literal["user"] = "user"

    def __init__(self, content: str | MessageContent, role: Literal["user"] = "user") -> None:
        if isinstance(content, str):
            self.content = TextContent(text=content)
        else:
            self.content = content
        self.role = role

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "content": self.content.dump(camel_case=camel_case),
            "role": self.role,
        }

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> Message:
        content = MessageContent._load(data["content"])
        return cls(content=content, role=data["role"])


class MessageList(CogniteResourceList[Message]):
    """List of messages."""

    _RESOURCE = Message


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
        item_type = data["type"]
        item_data = {k: v for k, v in data.items() if k != "type"}
        return cls(type=item_type, data=item_data)


@dataclass
class AgentReasoningItem(CogniteObject):
    """Reasoning item in agent response.

    Args:
        content (list[MessageContent]): The reasoning content.
    """

    content: list[MessageContent]

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "content": [item.dump(camel_case=camel_case) for item in self.content],
        }

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentReasoningItem:
        content = [MessageContent._load(item) for item in data.get("content", [])]
        return cls(content=content)


@dataclass
class AgentMessage(CogniteResource):
    """A message from an agent.

    Args:
        content (MessageContent | None): The message content.
        data (list[AgentDataItem] | None): Data items in the response.
        reasoning (list[AgentReasoningItem] | None): Reasoning items in the response.
        role (Literal["agent"]): The role of the message sender.
    """

    content: MessageContent | None = None
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
        content = MessageContent._load(data["content"]) if "content" in data else None
        data_items = [AgentDataItem._load(item, cognite_client) for item in data.get("data", [])]
        reasoning_items = [AgentReasoningItem._load(item, cognite_client) for item in data.get("reasoning", [])]
        return cls(
            content=content,
            data=data_items if data_items else None,
            reasoning=reasoning_items if reasoning_items else None,
            role=data["role"],
        )


class AgentMessageList(CogniteResourceList[AgentMessage]):
    """List of agent messages."""

    _RESOURCE = AgentMessage


class AgentChatResponse(CogniteResource):
    """Response from agent chat.

    Args:
        agent_id (str): The ID of the agent.
        messages (AgentMessageList): The response messages from the agent.
        type (str): The response type.
        cursor (str | None): Cursor for conversation continuation.
    """

    def __init__(
        self,
        agent_id: str,
        messages: AgentMessageList,
        type: str,
        cursor: str | None = None,
    ) -> None:
        self.agent_id = agent_id
        self.cursor = cursor
        self.messages = messages
        self.type = type

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = {
            "agentId" if camel_case else "agent_id": self.agent_id,
            "response": {
                "cursor": self.cursor,
                "messages": [msg.dump(camel_case=camel_case) for msg in self.messages] if self.messages else [],
                "type": self.type,
            },
        }
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
        response_data = data["response"]
        raw_messages = response_data["messages"]
        messages = [AgentMessage._load(msg, cognite_client) for msg in raw_messages]
        messages_list = AgentMessageList(messages)

        instance = cls(
            agent_id=data["agentId"],
            cursor=response_data.get("cursor"),
            messages=messages_list,
            type=response_data["type"],
        )

        return instance
