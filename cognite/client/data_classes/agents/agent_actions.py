from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from cognite.client import CogniteClient

from cognite.client.data_classes._base import CogniteObject, CogniteResource, CogniteResourceList


@dataclass
class ClientToolAction(CogniteObject):
    """Agent action for client tools.

    Args:
        type (Literal["clientTool"]): The type of the action.
        client_tool (dict[str, Any]): The client tool configuration with name, description, and parameters.
    """

    type: Literal["clientTool"] = "clientTool"
    client_tool: dict[str, Any] | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = {"type": self.type}
        if self.client_tool:
            key = "clientTool" if camel_case else "client_tool"
            result[key] = self.client_tool
        return result

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> ClientToolAction:
        return cls(
            type=data.get("type", "clientTool"),
            client_tool=data.get("clientTool"),
        )


@dataclass
class UnknownAction(CogniteObject):
    """Action for unknown/unrecognized action types.

    Args:
        type (str): The type of the action.
        data (dict[str, Any]): The raw action data.
    """

    type: str
    data: dict[str, Any]

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = self.data.copy()
        result["type"] = self.type
        return result

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> UnknownAction:
        return cls(
            type=data.get("type", "unknown"),
            data=data,
        )


class ActionList(CogniteResourceList[CogniteObject]):
    _RESOURCE = CogniteObject

    @classmethod
    def _load_list(cls, data: list[dict[str, Any]], cognite_client: CogniteClient | None = None) -> ActionList:
        """Load a list of actions from raw data."""
        actions = []
        for item in data:
            action_type = item.get("type", "")
            if action_type == "clientTool":
                actions.append(ClientToolAction._load(item, cognite_client))
            else:
                actions.append(UnknownAction._load(item, cognite_client))
        return cls(actions)


# Response-specific action classes

@dataclass
class ClientToolCall(CogniteObject):
    """Details of a client tool call in the response.

    Args:
        name (str): The name of the client tool.
        arguments (str): The JSON-encoded arguments for the tool call.
    """

    name: str
    arguments: str

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> ClientToolCall:
        return cls(
            name=data["name"],
            arguments=data["arguments"],
        )


@dataclass
class ClientToolResponseAction(CogniteResource):
    """A client tool action in the agent's response.

    Args:
        id (str): The ID of the action.
        type (Literal["clientTool"]): The type of the action.
        client_tool (ClientToolCall): The client tool call details.
    """

    id: str
    type: Literal["clientTool"]
    client_tool: ClientToolCall

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        key = "clientTool" if camel_case else "client_tool"
        return {
            "id": self.id,
            "type": self.type,
            key: self.client_tool.dump(camel_case=camel_case),
        }

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> ClientToolResponseAction:
        return cls(
            id=data["id"],
            type=data["type"],
            client_tool=ClientToolCall._load(data["clientTool"], cognite_client),
        )


@dataclass
class ToolConfirmationResponseAction(CogniteResource):
    """A tool confirmation action in the agent's response.

    Args:
        action_id (str): The ID of the action being confirmed.
        type (Literal["toolConfirmation"]): The type of the action.
        tool_confirmation (dict[str, Any]): The tool confirmation details.
    """

    action_id: str
    type: Literal["toolConfirmation"]
    tool_confirmation: dict[str, Any]

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "actionId" if camel_case else "action_id": self.action_id,
            "type": self.type,
            "toolConfirmation" if camel_case else "tool_confirmation": self.tool_confirmation,
        }

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> ToolConfirmationResponseAction:
        return cls(
            action_id=data["actionId"],
            type=data["type"],
            tool_confirmation=data["toolConfirmation"],
        )


@dataclass
class UnknownResponseAction(CogniteResource):
    """An unknown action type in the agent's response.

    Args:
        type (str): The type of the action.
        data (dict[str, Any]): The raw action data.
    """

    type: str
    data: dict[str, Any]

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = self.data.copy()
        result["type"] = self.type
        return result

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> UnknownResponseAction:
        return cls(
            type=data.get("type", "unknown"),
            data=data,
        )


class ResponseActionList(CogniteResourceList[CogniteResource]):
    _RESOURCE = CogniteResource

    @classmethod
    def _load_list(cls, data: list[dict[str, Any]], cognite_client: CogniteClient | None = None) -> ResponseActionList:
        """Load a list of response actions from raw data."""
        actions = []
        for item in data:
            action_type = item.get("type", "")
            if action_type == "clientTool":
                actions.append(ClientToolResponseAction._load(item, cognite_client))
            elif action_type == "toolConfirmation":
                actions.append(ToolConfirmationResponseAction._load(item, cognite_client))
            else:
                actions.append(UnknownResponseAction._load(item, cognite_client))
        return cls(actions)