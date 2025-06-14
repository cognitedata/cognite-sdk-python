from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from cognite.client import CogniteClient

from cognite.client.data_classes._base import (
    CogniteResourceList,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)


@dataclass
class AgentToolApply(WriteableCogniteResource["AgentToolApply"]):
    """Representation of an AI Agent Tool in CDF.
    This is the write format of an agent tool.

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

    def as_apply(self) -> AgentToolApply:
        return self

    def as_write(self) -> AgentToolApply:
        return self.as_apply()

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentToolApply:
        return cls(
            name=resource["name"],
            type=resource["type"],
            description=resource["description"],
            configuration=resource.get("configuration"),
        )


@dataclass
class AgentTool(AgentToolApply):
    """
    Representation of an AI Agent Tool in CDF.
    This is the read format of an agent tool.

    Args:
        name (str): The name of the tool.
        type (str): The type of the tool.
        description (str): The description of the tool.
        configuration (dict[str, Any] | None): The configuration of the tool.
    """

    def as_apply(self) -> AgentToolApply:
        return AgentToolApply(
            name=self.name,
            type=self.type,
            description=self.description,
            configuration=self.configuration,
        )

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentTool:
        return cls(
            name=resource["name"],
            type=resource["type"],
            description=resource["description"],
            configuration=resource.get("configuration"),
        )


class AgentToolApplyList(CogniteResourceList[AgentToolApply]):
    _RESOURCE = AgentToolApply


class AgentToolList(
    WriteableCogniteResourceList[AgentToolApply, AgentTool],
):
    _RESOURCE = AgentTool

    def as_apply(self) -> AgentToolApplyList:
        """Returns this agent tool list as a writeable instance"""
        return AgentToolApplyList([item.as_apply() for item in self.data], cognite_client=self._get_cognite_client())

    def as_write(self) -> AgentToolApplyList:
        return self.as_apply()
