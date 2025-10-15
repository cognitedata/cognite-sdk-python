from __future__ import annotations

import json
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


@dataclass(frozen=True, slots=True)
class Action(CogniteObject, ABC):
    """Base class for all action types that can be provided to an agent."""

    _type: ClassVar[str]

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> Action:
        """Dispatch to the correct concrete action class based on `type`."""
        action_type = data.get("type", "")
        action_class = _ACTION_CLS_BY_TYPE.get(action_type, UnknownAction)
        return action_class._load_action(data, cognite_client)

    @classmethod
    @abstractmethod
    def _load_action(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> Action:
        """Create a concrete action instance from raw data."""
        ...


@dataclass(frozen=True, slots=True)
class ClientToolAction(Action):
    """A client-side tool definition that can be called by the agent.

    Args:
        name (str): The name of the client tool to call.
        description (str): A description of what the function does. The language model will use this description when selecting the function and interpreting its parameters.
        parameters (dict[str, object]): The parameters the function accepts, described as a JSON Schema object.
    """

    _type: ClassVar[str] = "clientTool"
    name: str
    description: str
    parameters: dict[
        str, object
    ]  # TODO: We do not want the user to have to handle this, instead e.g. set json schema from Pydantic classes, or function signatures, like ClientToolAction.from_function(func: Callable)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "type": self._type,
            self._type: {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }

    @classmethod
    def _load_action(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> ClientToolAction:
        client_tool = data[cls._type]
        return cls(
            name=client_tool["name"],
            description=client_tool["description"],
            parameters=client_tool["parameters"],
        )


@dataclass(frozen=True, slots=True)
class UnknownAction(Action):
    """Unknown action type for forward compatibility.

    Args:
        type (str): The action type.
        data (dict[str, object]): The raw action data.
    """

    type: str
    data: dict[str, object] = field(default_factory=dict)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = self.data.copy()
        result["type"] = self.type
        return result

    @classmethod
    def _load_action(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> UnknownAction:
        action_type = data.get("type", "")
        return cls(data=data, type=action_type)


# Build the mapping AFTER concrete classes are defined
_ACTION_CLS_BY_TYPE: dict[str, type[Action]] = {
    subclass._type: subclass  # type: ignore[type-abstract]
    for subclass in Action.__subclasses__()
    if subclass is not UnknownAction and hasattr(subclass, "_type")
}


@dataclass(frozen=True, slots=True)
class ActionCall(CogniteObject, ABC):
    """Base class for action calls requested by the agent."""

    _type: ClassVar[str]
    action_id: str

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> ActionCall:
        """Dispatch to the correct concrete action call class based on `type`."""
        action_type = data.get("type", "")
        action_class = _ACTION_CALL_CLS_BY_TYPE.get(action_type, UnknownActionCall)
        return action_class._load_call(data, cognite_client)

    @classmethod
    @abstractmethod
    def _load_call(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> ActionCall:
        """Create a concrete action call instance from raw data."""
        ...


@dataclass(frozen=True, slots=True)
class ClientToolCall(ActionCall):
    """A client tool call requested by the agent.

    Args:
        action_id (str): The unique identifier for this action call.
        name (str): The name of the client tool being called.
        arguments (dict[str, object]): The parsed arguments for the tool call.
    """

    _type: ClassVar[str] = "clientTool"
    action_id: str
    name: str
    arguments: dict[str, object]  # Always a json string, in line with OpenAI's API pattern

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "type": self._type,
            "actionId" if camel_case else "action_id": self.action_id,
            self._type: {
                "name": self.name,
                "arguments": json.dumps(self.arguments),
            },
        }

    @classmethod
    def _load_call(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> ClientToolCall:
        client_tool = data[cls._type]
        arguments_str = client_tool["arguments"]
        return cls(
            action_id=data["actionId"],
            name=client_tool["name"],
            arguments=json.loads(arguments_str),
        )


@dataclass(frozen=True, slots=True)
class ToolConfirmationCall(ActionCall):
    """A tool confirmation request from the agent.

    Args:
        action_id (str): The unique identifier for this action call.
        content (MessageContent): The confirmation message content.
        tool_name (str): The name of the tool requiring confirmation.
        tool_arguments (dict[str, object]): The arguments for the tool call.
        tool_description (str): Description of what the tool does.
        tool_type (str): The type of tool (e.g., "runPythonCode", "callRestApi").
        details (dict[str, object] | None): Optional additional details about the tool call.
    """

    _type: ClassVar[str] = "toolConfirmation"
    action_id: str
    content: MessageContent
    tool_name: str
    tool_arguments: dict[str, object]
    tool_description: str
    tool_type: str
    details: dict[str, object] | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {
            "type": self._type,
            "actionId" if camel_case else "action_id": self.action_id,
            self._type: {
                "content": self.content.dump(camel_case=camel_case),
                "toolName" if camel_case else "tool_name": self.tool_name,
                "toolArguments" if camel_case else "tool_arguments": self.tool_arguments,
                "toolDescription" if camel_case else "tool_description": self.tool_description,
                "toolType" if camel_case else "tool_type": self.tool_type,
            },
        }
        if self.details is not None:
            result[self._type]["details"] = self.details
        return result

    @classmethod
    def _load_call(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> ToolConfirmationCall:
        tool_confirmation = data[cls._type]
        content = MessageContent._load(tool_confirmation["content"])
        return cls(
            action_id=data["actionId"],
            content=content,
            tool_name=tool_confirmation["toolName"],
            tool_arguments=tool_confirmation["toolArguments"],
            tool_description=tool_confirmation["toolDescription"],
            tool_type=tool_confirmation["toolType"],
            details=tool_confirmation.get("details"),
        )


@dataclass(frozen=True, slots=True)
class UnknownActionCall(ActionCall):
    """Unknown action call type for forward compatibility.

    Args:
        action_id (str): The unique identifier for this action call.
        type (str): The action call type.
        data (dict[str, object]): The raw action call data.
    """

    action_id: str
    type: str
    data: dict[str, object] = field(default_factory=dict)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = self.data.copy()
        result["type"] = self.type
        result["actionId" if camel_case else "action_id"] = self.action_id
        return result

    @classmethod
    def _load_call(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> UnknownActionCall:
        action_type = data["type"]
        action_id = data["actionId"]
        return cls(action_id=action_id, data=data, type=action_type)


# Build the mapping AFTER concrete classes are defined
_ACTION_CALL_CLS_BY_TYPE: dict[str, type[ActionCall]] = {
    subclass._type: subclass  # type: ignore[type-abstract]
    for subclass in ActionCall.__subclasses__()
    if subclass is not UnknownActionCall and hasattr(subclass, "_type")
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


@dataclass(frozen=True, slots=True)
class ActionResult(CogniteObject, ABC):
    """Base class for action execution results."""

    _type: ClassVar[str]
    _role: ClassVar[Literal["action"]] = "action"
    action_id: str


@dataclass(frozen=True, slots=True)
class ClientToolResult(ActionResult):
    """Result of executing a client tool, for sending back to the agent.

    Args:
        action_id (str): The ID of the action being responded to.
        content (str | MessageContent): The result of executing the action.
        data (list[Any] | None): Optional structured data.
    """

    _type: ClassVar[str] = "clientTool"
    content: MessageContent
    data: list[Any] | None = None

    def __init__(self, action_id: str, content: str | MessageContent, data: list[Any] | None = None) -> None:
        object.__setattr__(self, "action_id", action_id)
        object.__setattr__(self, "content", TextContent(text=content) if isinstance(content, str) else content)
        object.__setattr__(self, "data", data)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "role": self._role,
            "type": self._type,
            "actionId" if camel_case else "action_id": self.action_id,
            "content": self.content.dump(camel_case=camel_case),
            "data": self.data or [],
        }

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> ClientToolResult:
        """Load from dumped data. Only used for testing."""
        content = MessageContent._load(data["content"], cognite_client)
        return cls(
            action_id=data["actionId"],
            content=content,
            data=data.get("data"),
        )


@dataclass(frozen=True, slots=True)
class ToolConfirmationResult(ActionResult):
    """Result of a tool confirmation request.

    Args:
        action_id (str): The ID of the action being responded to.
        status (Literal["ALLOW", "DENY"]): Whether to allow or deny the tool execution.
    """

    _type: ClassVar[str] = "toolConfirmation"
    status: Literal["ALLOW", "DENY"]

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "role": self._role,
            "type": self._type,
            "actionId" if camel_case else "action_id": self.action_id,
            "status": self.status,
        }

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> ToolConfirmationResult:
        """Load from dumped data. Not used to load from API response."""
        return cls(
            action_id=data.get("actionId", data.get("action_id", "")),
            status=data["status"],
        )


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
        actions (list[ActionCall] | None): Action calls requested by the agent.
        role (Literal["agent"]): The role of the message sender.
    """

    content: MessageContent | None = None
    data: list[AgentDataItem] | None = None
    reasoning: list[AgentReasoningItem] | None = None
    actions: list[ActionCall] | None = None
    role: Literal["agent"] = "agent"

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {"role": self.role}
        if self.content is not None:
            result["content"] = self.content.dump(camel_case=camel_case)
        if self.data is not None:
            result["data"] = [item.dump(camel_case=camel_case) for item in self.data]
        if self.reasoning is not None:
            result["reasoning"] = [item.dump(camel_case=camel_case) for item in self.reasoning]
        if self.actions is not None:
            result["actions"] = [item.dump(camel_case=camel_case) for item in self.actions]
        return result

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentMessage:
        content = MessageContent._load(data["content"]) if "content" in data else None
        data_items = [AgentDataItem._load(item, cognite_client) for item in data.get("data", [])]
        reasoning_items = [AgentReasoningItem._load(item, cognite_client) for item in data.get("reasoning", [])]
        action_calls = [ActionCall._load(item, cognite_client) for item in data.get("actions", [])]
        return cls(
            content=content,
            data=data_items if data_items else None,
            reasoning=reasoning_items if reasoning_items else None,
            actions=action_calls if action_calls else None,
            role=data["role"],
        )


class AgentMessageList(CogniteResourceList[AgentMessage]):
    """List of agent messages."""

    _RESOURCE = AgentMessage


class AgentChatResponse(CogniteResource):
    """Response from agent chat.

    Args:
        agent_external_id (str): The external ID of the agent.
        messages (AgentMessageList): The response messages from the agent.
        type (str): The response type.
        cursor (str | None): Cursor for conversation continuation.
    """

    def __init__(
        self,
        agent_external_id: str,
        messages: AgentMessageList,
        type: str,
        cursor: str | None = None,
    ) -> None:
        self.agent_external_id = agent_external_id
        self.cursor = cursor
        self.messages = messages
        self.type = type

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = {
            "agentExternalId" if camel_case else "agent_external_id": self.agent_external_id,
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

    @property
    def action_calls(self) -> list[ActionCall] | None:
        """Get all action calls from all messages."""
        if not self.messages:
            return None
        return [call for msgs in self.messages if msgs.actions for call in msgs.actions] or None

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentChatResponse:
        response_data = data["response"]
        raw_messages = response_data["messages"]
        messages = [AgentMessage._load(msg, cognite_client) for msg in raw_messages]
        messages_list = AgentMessageList(messages)

        instance = cls(
            agent_external_id=data["agentExternalId"],
            cursor=response_data.get("cursor"),
            messages=messages_list,
            type=response_data["type"],
        )

        return instance
