from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal

if TYPE_CHECKING:
    from cognite.client import CogniteClient

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResource,
    CogniteResourceList,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)


@dataclass
class AgentActionCore(WriteableCogniteResource["AgentActionWrite"], ABC):
    """Core representation of an AI Agent Action in CDF.

    Args:
        type (str): The type of the agent action.
    """

    _type: ClassVar[str]  # Will be set by concrete classes
    type: str

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type."""
        result = super().dump(camel_case=camel_case)
        # Use the _type from the class if available, otherwise use instance type
        result["type"] = getattr(self.__class__, "_type", self.type)
        return result


@dataclass
class AgentActionWrite(AgentActionCore, ABC):
    """The write format of an agent action.

    Args:
        type (str): The type of the agent action.
    """

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type."""
        result = super().dump(camel_case=camel_case)
        # Use the _type from the class if available, otherwise use instance type
        result["type"] = getattr(self.__class__, "_type", getattr(self, "type", ""))
        return result

    def as_write(self) -> AgentActionWrite:
        return self

    @classmethod
    def _load_action(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentActionWrite:
        return cls(type=resource["type"])


@dataclass
class AgentAction(AgentActionCore, ABC):
    """The read format of an agent action.

    Args:
        type (str): The type of the agent action.
    """

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type."""
        result = super().dump(camel_case=camel_case)
        # Use the _type from the class if available, otherwise use instance type
        result["type"] = getattr(self.__class__, "_type", getattr(self, "type", ""))
        return result

    @abstractmethod
    def as_write(self) -> AgentActionWrite:
        """Convert this action to its write representation."""
        pass

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentAction:
        action_type = resource.get("type", "")

        # Get the appropriate class based on the action.type
        action_class = _AGENT_ACTION_CLS_BY_TYPE.get(action_type, UnknownAgentAction)

        return action_class._load_action(resource, cognite_client)

    @classmethod
    def _load_action(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentAction:
        """Default instance loading method for simple action types."""
        return cls(type=resource["type"])


@dataclass
class JSONSchemaParameterProperty(CogniteObject):
    """JSON Schema property definition for client tool parameters.

    This represents a property within a JSON Schema definition, supporting
    various validation constraints and type specifications.

    Args:
        type (str | None): The data type of the property (e.g., "string", "number", "object").
        description (str | None): A description of the property.
        enum (list[Any] | None): A list of valid enum values.
        default (Any | None): The default value for the property.
        examples (list[Any] | None): Example values for the property.
        format (str | None): The format of the property (e.g., "date-time", "email").
        pattern (str | None): A regex pattern for string validation.
        nullable (bool | None): Whether the property can be null.
        title (str | None): A title for the property.
        minimum (float | None): Minimum value for numeric properties.
        maximum (float | None): Maximum value for numeric properties.
        min_length (int | None): Minimum length for string properties.
        max_length (int | None): Maximum length for string properties.
        min_items (int | None): Minimum items for array properties.
        max_items (int | None): Maximum items for array properties.
        min_properties (int | None): Minimum properties for object types.
        max_properties (int | None): Maximum properties for object types.
        required (list[str] | None): Required property names (for object types).
        properties (dict[str, JSONSchemaParameterProperty] | None): Nested properties (for object types).
        items (JSONSchemaParameterProperty | None): Schema for array items.
        additional_properties (bool | None): Whether additional properties are allowed.
        any_of (list[dict[str, Any]] | None): List of schemas where at least one must validate.
        property_ordering (list[str] | None): Preferred ordering of properties.
    """

    type: str | None = None
    description: str | None = None
    enum: list[Any] | None = None
    default: Any | None = None
    examples: list[Any] | None = None
    format: str | None = None
    pattern: str | None = None
    nullable: bool | None = None
    title: str | None = None
    minimum: float | None = None
    maximum: float | None = None
    min_length: int | None = None
    max_length: int | None = None
    min_items: int | None = None
    max_items: int | None = None
    min_properties: int | None = None
    max_properties: int | None = None
    required: list[str] | None = None
    properties: dict[str, JSONSchemaParameterProperty] | None = None
    items: JSONSchemaParameterProperty | None = None
    additional_properties: bool | None = None
    any_of: list[dict[str, Any]] | None = None
    property_ordering: list[str] | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump to a dictionary, handling nested properties."""
        result: dict[str, Any] = {}
        
        # Simple fields
        for field_name, value in [
            ("type", self.type),
            ("description", self.description),
            ("enum", self.enum),
            ("default", self.default),
            ("examples", self.examples),
            ("format", self.format),
            ("pattern", self.pattern),
            ("nullable", self.nullable),
            ("title", self.title),
            ("minimum", self.minimum),
            ("maximum", self.maximum),
            ("required", self.required),
        ]:
            if value is not None:
                result[field_name] = value
        
        # Handle camelCase conversions
        for field_name, camel_name, value in [
            ("min_length", "minLength", self.min_length),
            ("max_length", "maxLength", self.max_length),
            ("min_items", "minItems", self.min_items),
            ("max_items", "maxItems", self.max_items),
            ("min_properties", "minProperties", self.min_properties),
            ("max_properties", "maxProperties", self.max_properties),
            ("additional_properties", "additionalProperties", self.additional_properties),
            ("any_of", "anyOf", self.any_of),
            ("property_ordering", "propertyOrdering", self.property_ordering),
        ]:
            if value is not None:
                key = camel_name if camel_case else field_name
                result[key] = value
        
        # Handle nested properties
        if self.properties is not None:
            result["properties"] = {
                k: v.dump(camel_case=camel_case) for k, v in self.properties.items()
            }
        
        # Handle items
        if self.items is not None:
            result["items"] = self.items.dump(camel_case=camel_case) if isinstance(self.items, JSONSchemaParameterProperty) else self.items
        
        return result

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> JSONSchemaParameterProperty:
        """Load from a dictionary, handling nested properties."""
        # Handle nested properties
        properties = None
        if "properties" in data:
            properties = {
                k: cls._load(v) for k, v in data["properties"].items()
            }
        
        # Handle items
        items = None
        if "items" in data:
            items = cls._load(data["items"]) if isinstance(data["items"], dict) else data["items"]
        
        return cls(
            type=data.get("type"),
            description=data.get("description"),
            enum=data.get("enum"),
            default=data.get("default"),
            examples=data.get("examples"),
            format=data.get("format"),
            pattern=data.get("pattern"),
            nullable=data.get("nullable"),
            title=data.get("title"),
            minimum=data.get("minimum"),
            maximum=data.get("maximum"),
            min_length=data.get("minLength"),
            max_length=data.get("maxLength"),
            min_items=data.get("minItems"),
            max_items=data.get("maxItems"),
            min_properties=data.get("minProperties"),
            max_properties=data.get("maxProperties"),
            required=data.get("required"),
            properties=properties,
            items=items,
            additional_properties=data.get("additionalProperties"),
            any_of=data.get("anyOf"),
            property_ordering=data.get("propertyOrdering"),
        )


@dataclass
class JSONSchemaParameters(CogniteObject):
    """JSON Schema parameters for client tools.

    Args:
        type (str): The type of the schema (typically "object").
        description (str | None): A description of the parameters.
        properties (dict[str, JSONSchemaParameterProperty]): The properties of the parameters.
        required (list[str] | None): List of required property names.
        property_ordering (list[str] | None): Preferred ordering of properties.
    """

    type: str
    properties: dict[str, JSONSchemaParameterProperty]
    description: str | None = None
    required: list[str] | None = None
    property_ordering: list[str] | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {"type": self.type}
        
        if self.description is not None:
            result["description"] = self.description
        
        result["properties"] = {
            k: v.dump(camel_case=camel_case) for k, v in self.properties.items()
        }
        
        if self.required is not None:
            result["required"] = self.required
        
        if self.property_ordering is not None:
            key = "propertyOrdering" if camel_case else "property_ordering"
            result[key] = self.property_ordering
        
        return result

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> JSONSchemaParameters:
        properties = {
            k: JSONSchemaParameterProperty._load(v) for k, v in data["properties"].items()
        }
        
        return cls(
            type=data["type"],
            description=data.get("description"),
            properties=properties,
            required=data.get("required"),
            property_ordering=data.get("propertyOrdering"),
        )


@dataclass
class ClientToolDetails(CogniteObject):
    """Details for a client tool action.

    Args:
        name (str): The name of the client tool.
        description (str): The description of the client tool.
        parameters (JSONSchemaParameters): The parameters schema for the client tool.
    """

    name: str
    description: str
    parameters: JSONSchemaParameters

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "parameters": self.parameters.dump(camel_case=camel_case),
        }

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> ClientToolDetails:
        return cls(
            name=data["name"],
            description=data["description"],
            parameters=JSONSchemaParameters._load(data["parameters"]),
        )


@dataclass
class ClientToolAgentAction(AgentAction):
    """Agent action for client tools.

    Args:
        client_tool (ClientToolDetails): The client tool details.
    """

    _type: ClassVar[str] = "clientTool"
    client_tool: ClientToolDetails

    def __init__(self, client_tool: ClientToolDetails) -> None:
        super().__init__(type=self._type)
        self.client_tool = client_tool

    def as_write(self) -> ClientToolAgentActionWrite:
        return ClientToolAgentActionWrite(client_tool=self.client_tool)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case=camel_case)
        key = "clientTool" if camel_case else "client_tool"
        result[key] = self.client_tool.dump(camel_case=camel_case)
        return result

    @classmethod
    def _load_action(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> ClientToolAgentAction:
        client_tool = ClientToolDetails._load(resource["clientTool"])
        return cls(client_tool=client_tool)


@dataclass
class ClientToolAgentActionWrite(AgentActionWrite):
    """Write version of client tool agent action.

    Args:
        client_tool (ClientToolDetails): The client tool details.
    """

    _type: ClassVar[str] = "clientTool"
    client_tool: ClientToolDetails

    def __init__(self, client_tool: ClientToolDetails) -> None:
        super().__init__(type=self._type)
        self.client_tool = client_tool

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case=camel_case)
        key = "clientTool" if camel_case else "client_tool"
        result[key] = self.client_tool.dump(camel_case=camel_case)
        return result

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> ClientToolAgentActionWrite:
        client_tool = ClientToolDetails._load(resource["clientTool"])
        return cls(client_tool=client_tool)


@dataclass
class UnknownAgentAction(AgentAction):
    """Agent action for unknown/unrecognized action types.

    Args:
        type (str): The type of the agent action.
        data (dict[str, Any] | None): The raw action data.
    """

    data: dict[str, Any] | None = None

    def as_write(self) -> UnknownAgentActionWrite:
        return UnknownAgentActionWrite(
            type=self.type,
            data=self.data,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        if self.data:
            result = self.data.copy()
            result["type"] = self.type
            return result
        return super().dump(camel_case=camel_case)

    @classmethod
    def _load_action(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> UnknownAgentAction:
        return cls(
            type=resource["type"],
            data=resource,
        )


@dataclass
class UnknownAgentActionWrite(AgentActionWrite):
    """Write version of unknown agent action.

    Args:
        type (str): The type of the agent action.
        data (dict[str, Any] | None): The raw action data.
    """

    data: dict[str, Any] | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        if self.data:
            result = self.data.copy()
            result["type"] = self.type
            return result
        return super().dump(camel_case=camel_case)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> UnknownAgentActionWrite:
        return cls(
            type=resource["type"],
            data=resource,
        )


class AgentActionWriteList(CogniteResourceList[AgentActionWrite]):
    _RESOURCE = AgentActionWrite


class AgentActionList(WriteableCogniteResourceList[AgentActionWrite, AgentAction]):
    _RESOURCE = AgentAction

    def as_write(self) -> AgentActionWriteList:
        """Returns this agent action list as a writeable instance"""
        return AgentActionWriteList([item.as_write() for item in self.data], cognite_client=self._get_cognite_client())


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

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "name": self.name,
            "arguments": self.arguments,
        }

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
            client_tool=ClientToolCall._load(data["clientTool"]),
        )


@dataclass
class ToolConfirmationContent(CogniteObject):
    """Content for tool confirmation.

    Args:
        type (str): The type of content (e.g., "text").
        text (str): The text content.
    """

    type: str
    text: str

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "type": self.type,
            "text": self.text,
        }

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> ToolConfirmationContent:
        return cls(
            type=data["type"],
            text=data["text"],
        )


@dataclass
class ToolConfirmationDetails(CogniteObject):
    """Details for tool confirmation.

    Args:
        content (ToolConfirmationContent): The confirmation content.
        tool_name (str): The name of the tool.
        tool_arguments (dict[str, Any]): The tool arguments.
        tool_description (str): The tool description.
        tool_type (str): The type of the tool.
        details (dict[str, Any] | None): Additional details.
    """

    content: ToolConfirmationContent
    tool_name: str
    tool_arguments: dict[str, Any]
    tool_description: str
    tool_type: str
    details: dict[str, Any] | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = {
            "content": self.content.dump(camel_case=camel_case),
            "toolName" if camel_case else "tool_name": self.tool_name,
            "toolArguments" if camel_case else "tool_arguments": self.tool_arguments,
            "toolDescription" if camel_case else "tool_description": self.tool_description,
            "toolType" if camel_case else "tool_type": self.tool_type,
        }
        if self.details is not None:
            result["details"] = self.details
        return result

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> ToolConfirmationDetails:
        return cls(
            content=ToolConfirmationContent._load(data["content"]),
            tool_name=data["toolName"],
            tool_arguments=data["toolArguments"],
            tool_description=data["toolDescription"],
            tool_type=data["toolType"],
            details=data.get("details"),
        )


@dataclass
class ToolConfirmationResponseAction(CogniteResource):
    """A tool confirmation action in the agent's response.

    Args:
        action_id (str): The ID of the action being confirmed.
        type (Literal["toolConfirmation"]): The type of the action.
        tool_confirmation (ToolConfirmationDetails): The tool confirmation details.
    """

    action_id: str
    type: Literal["toolConfirmation"]
    tool_confirmation: ToolConfirmationDetails

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "actionId" if camel_case else "action_id": self.action_id,
            "type": self.type,
            "toolConfirmation" if camel_case else "tool_confirmation": self.tool_confirmation.dump(camel_case=camel_case),
        }

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> ToolConfirmationResponseAction:
        return cls(
            action_id=data["actionId"],
            type=data["type"],
            tool_confirmation=ToolConfirmationDetails._load(data["toolConfirmation"]),
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
            type=data["type"],
            data=data,
        )


# Build the mapping AFTER all classes are defined
_AGENT_ACTION_CLS_BY_TYPE: dict[str, type[AgentAction]] = {
    subclass._type: subclass  # type: ignore[type-abstract]
    for subclass in AgentAction.__subclasses__()
    if hasattr(subclass, "_type") and not getattr(subclass, "__abstractmethods__", None)
}


# Helper function to load response actions
def _load_response_action(data: dict[str, Any], cognite_client: CogniteClient | None = None) -> CogniteResource:
    """Load a response action from raw data."""
    action_type = data.get("type", "")
    
    if action_type == "clientTool":
        return ClientToolResponseAction._load(data, cognite_client)
    elif action_type == "toolConfirmation":
        return ToolConfirmationResponseAction._load(data, cognite_client)
    else:
        return UnknownResponseAction._load(data, cognite_client)