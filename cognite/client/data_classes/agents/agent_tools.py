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
class AgentToolCore(WriteableCogniteResource["AgentToolUpsert"]):
    """Core representation of an AI Agent Tool in CDF."""

    name: str
    type: str
    description: str
    configuration: dict[str, Any] | None = None


@dataclass
class AgentToolUpsert(AgentToolCore):
    """Representation of an AI Agent Tool in CDF.
    This is the write format of an agent tool.
    """

    def as_write(self) -> AgentToolUpsert:
        return self

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentToolUpsert:
        return cls(
            name=resource["name"],
            type=resource["type"],
            description=resource["description"],
            configuration=resource.get("configuration"),
        )


@dataclass
class AgentTool(AgentToolCore):
    """
    Representation of an AI Agent Tool in CDF.
    This is the read format of an agent tool.
    """

    def as_write(self) -> AgentToolUpsert:
        return AgentToolUpsert(
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


class AgentToolUpsertList(CogniteResourceList[AgentToolUpsert]):
    _RESOURCE = AgentToolUpsert


class AgentToolList(
    WriteableCogniteResourceList[AgentToolUpsert, AgentTool],
):
    _RESOURCE = AgentTool

    def as_write(self) -> AgentToolUpsertList:
        """Returns this agent tool list as a writeable instance"""
        return AgentToolUpsertList([item.as_write() for item in self.data], cognite_client=self._get_cognite_client())
