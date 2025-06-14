from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from cognite.client import CogniteClient

from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
    ExternalIDTransformerMixin,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.agents.agent_tools import AgentTool, AgentToolWrite


@dataclass
class AgentCore(CogniteResource, ABC):
    """Representation of an AI Agent in CDF.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): The name of the agent.
        description (str | None): The description of the agent.
        instructions (str | None): Instructions for the agent.
        model (str | None): Name of the language model to use.

    """

    external_id: str
    name: str
    description: str | None = None
    instructions: str | None = None
    model: str | None = None


@dataclass
class Agent(AgentCore, WriteableCogniteResource["AgentWrite"]):
    """Representation of an AI Agent in CDF.
    This is the read format of an agent.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): The name of the agent, for use in user interfaces.
        description (str | None): The human readable description of the agent.
        instructions (str | None): Instructions for the agent.
        model (str | None): Name of the language model to use.
        tools (list[AgentTool] | None): List of tools for the agent.
        created_time (int | None): The time the agent was created, in milliseconds since Thursday, 1 January 1970 00:00:00 UTC, minus leap seconds.
        last_updated_time (int | None): The time the agent was last updated, in milliseconds since Thursday, 1 January 1970 00:00:00 UTC, minus leap seconds.
        owner_id (str | None): The ID of the user who owns the agent.
    """

    tools: list[AgentTool] | None = None
    created_time: int | None = None
    last_updated_time: int | None = None
    owner_id: str | None = None

    def __post_init__(self) -> None:
        if self.tools is not None:
            if not isinstance(self.tools, list) or not all(isinstance(tool, AgentTool) for tool in self.tools):
                raise TypeError("Tools must be a list of AgentTool instances.")

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case=camel_case)
        if self.tools:
            result["tools"] = [item.dump(camel_case=camel_case) for item in self.tools]
        return result

    def as_write(self) -> AgentWrite:
        """Returns this Agent in its writeable format"""
        return AgentWrite(
            external_id=self.external_id,
            name=self.name,
            description=self.description,
            instructions=self.instructions,
            model=self.model,
            tools=[tool.as_write() for tool in self.tools] if self.tools else None,
        )

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Agent:
        tools = (
            [AgentTool._load(item) for item in resource.get("tools", [])]
            if isinstance(resource.get("tools"), list)
            else None
        )

        return cls(
            external_id=resource["externalId"],
            name=resource["name"],
            description=resource.get("description"),
            instructions=resource.get("instructions"),
            model=resource.get("model"),
            tools=tools,
            created_time=resource.get("createdTime"),
            last_updated_time=resource.get("lastUpdatedTime"),
            owner_id=resource.get("ownerId"),
        )


@dataclass
class AgentWrite(AgentCore):
    """Representation of an AI Agent in CDF.
    This is the write format of an agent.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): The name of the agent.
        description (str | None): The description of the agent.
        instructions (str | None): Instructions for the agent.
        model (str | None): Name of the language model to use.
        tools (list[AgentToolWrite] | None): List of tools for the agent.

    """

    tools: list[AgentToolWrite] | None = None

    def __post_init__(self) -> None:
        if self.tools is not None:
            if not isinstance(self.tools, list) or not all(isinstance(tool, AgentToolWrite) for tool in self.tools):
                raise TypeError("Tools must be a list of AgentToolWrite instances.")

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case=camel_case)
        if self.tools:
            result["tools"] = [item.dump(camel_case=camel_case) for item in self.tools]
        return result

    def as_write(self) -> AgentWrite:
        """Returns this AgentWrite instance."""
        return self

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentWrite:
        tools = (
            [AgentToolWrite._load(item) for item in resource.get("tools", [])]
            if isinstance(resource.get("tools"), list)
            else None
        )

        return cls(
            external_id=resource["externalId"],
            name=resource["name"],
            description=resource.get("description"),
            instructions=resource.get("instructions"),
            model=resource.get("model"),
            tools=tools,
        )


class AgentWriteList(CogniteResourceList[AgentWrite], ExternalIDTransformerMixin):
    _RESOURCE = AgentWrite


class AgentList(
    WriteableCogniteResourceList[AgentWrite, Agent],
    ExternalIDTransformerMixin,
):
    _RESOURCE = Agent

    def as_write(self) -> AgentWriteList:
        """Returns this AgentList as writeableinstance"""
        return AgentWriteList([item.as_write() for item in self.data], cognite_client=self._get_cognite_client())
