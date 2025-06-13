from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from cognite.client import CogniteClient

from cognite.client.data_classes._base import (
    CogniteResourceList,
    InternalIdTransformerMixin,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)


@dataclass
class AgentToolCore(WriteableCogniteResource["AgentToolWrite"], ABC):
    """
    Representation of an AI Agent Tool in CDF.

    Args:
        name (str): The name of the tool.
        type (str): The type of the tool.
        description (str): The description of the tool.
        configuration (dict[str, Any] | None): The configuration of the tool.
    """

    name: str
    type: str
    description: str
    configuration: dict[str, Any] | None = None

    def as_write(self) -> AgentToolWrite:
        return AgentToolWrite(
            name=self.name,
            type=self.type,
            description=self.description,
            configuration=self.configuration,
        )


@dataclass
class AgentTool(AgentToolCore):
    """
    Representation of an AI Agent Tool in CDF.
    This is the read format of an agent tool.

    Args:
        name (str): The name of the tool.
        type (str): The type of the tool.
        description (str): The description of the tool.
        configuration (dict[str, Any] | None): The configuration of the tool.
    """

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentTool:
        return cls(
            name=resource["name"],
            type=resource["type"],
            description=resource["description"],
            configuration=resource.get("configuration"),
        )


@dataclass
class AgentToolWrite(AgentToolCore):
    """Representation of an AI Agent Tool in CDF.
    This is the write format of an agent tool.

    Args:
        name (str): The name of the tool.
        type (str): The type of the tool.
        description (str): The description of the tool.
        configuration (dict[str, Any] | None): The configuration of the tool.
    """

    pass


class AgentToolWriteList(CogniteResourceList[AgentToolWrite]):
    _RESOURCE = AgentToolWrite


class AgentToolList(
    WriteableCogniteResourceList[AgentToolWrite, AgentTool],
    InternalIdTransformerMixin,
):
    _RESOURCE = AgentTool

    def as_write(self) -> AgentToolWriteList:
        """Returns this AgentToolWriteList instance"""
        return AgentToolWriteList([item.as_write() for item in self.data], cognite_client=self._get_cognite_client())
