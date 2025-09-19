"""Client tool data classes for agent actions."""

from __future__ import annotations

from typing import Any

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList


class ClientToolParameters(CogniteResource):
    """Parameters for a client tool call.

    Args:
        type (str): The type of the parameter, must be "object".
        properties (dict[str, Any] | None): Properties defining the parameter schema.
        required (list[str] | None): List of required property names.
        property_ordering (list[str] | None): Order of properties.
        description (str | None): Description of the parameter.
    """

    def __init__(
        self,
        type: str = "object",
        properties: dict[str, Any] | None = None,
        required: list[str] | None = None,
        property_ordering: list[str] | None = None,
        description: str | None = None,
    ) -> None:
        self.type = type
        self.properties = properties or {}
        self.required = required or []
        self.property_ordering = property_ordering or []
        self.description = description

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the parameters to a dictionary."""
        result = {
            "type": self.type,
            "properties": self.properties,
            "required": self.required,
            "propertyOrdering" if camel_case else "property_ordering": self.property_ordering,
        }
        if self.description:
            result["description"] = self.description
        return result

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: Any = None) -> ClientToolParameters:
        """Load parameters from a dictionary."""
        return cls(
            type=data.get("type", "object"),
            properties=data.get("properties", {}),
            required=data.get("required", []),
            property_ordering=data.get("propertyOrdering", data.get("property_ordering", [])),
            description=data.get("description"),
        )


class ClientTool(CogniteResource):
    """A client tool that can be called by the agent.

    Args:
        name (str): The name of the client tool to call.
        parameters (ClientToolParameters): The parameters the function accepts.
        description (str | None): A description of what the function does.
        type (str): The type of client action, always "clientTool".
    """

    def __init__(
        self,
        name: str,
        parameters: ClientToolParameters,
        description: str | None = None,
        type: str = "clientTool",
    ) -> None:
        self.name = name
        self.description = description
        self.parameters = parameters
        self.type = type

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the client tool to a dictionary."""
        result = {
            "name": self.name,
            "type": self.type,
            "parameters": self.parameters.dump(camel_case=camel_case),
        }
        if self.description:
            result["description"] = self.description
        return result

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: Any = None) -> ClientTool:
        """Load a client tool from a dictionary."""
        return cls(
            name=data["name"],
            description=data.get("description"),
            parameters=ClientToolParameters._load(data["parameters"]),
            type=data.get("type", "clientTool"),
        )


class ClientToolList(CogniteResourceList[ClientTool]):
    """List of client tools."""

    _RESOURCE = ClientTool
