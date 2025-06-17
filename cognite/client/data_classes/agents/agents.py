from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from cognite.client import CogniteClient

from cognite.client.data_classes._base import (
    CogniteResourceList,
    ExternalIDTransformerMixin,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.agents.agent_tools import AgentTool, AgentToolUpsert


@dataclass
class AgentCore(WriteableCogniteResource["AgentUpsert"]):
    """Core representation of an AI agent.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): The name of the agent.
        description (str | None): The description of the agent.
        instructions (str | None): Instructions for the agent.
        model (str | None): Name of the language model to use. For example, "azure/gpt-4o", "gcp/gemini-2.0" or "aws/claude-3.5-sonnet".
    """

    external_id: str
    name: str
    description: str | None = None
    instructions: str | None = None
    model: str | None = None


@dataclass
class AgentUpsert(AgentCore):
    """Representation of an AI agent.
    This is the write format of an agent.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): The name of the agent, for use in user interfaces.
        description (str | None): The human readable description of the agent.
        instructions (str | None): Instructions for the agent.
        model (str | None): Name of the language model to use. For example, "azure/gpt-4o", "gcp/gemini-2.0" or "aws/claude-3.5-sonnet".
        tools (Sequence[AgentToolUpsert] | None): List of tools for the agent.

    """

    tools: Sequence[AgentToolUpsert] | None = None

    def __post_init__(self) -> None:
        if self.tools is not None:
            if not isinstance(self.tools, Sequence) or not all(
                isinstance(tool, AgentToolUpsert) for tool in self.tools
            ):
                raise TypeError("Tools must be a sequence of AgentToolUpsert instances.")

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case=camel_case)
        if self.tools:
            result["tools"] = [item.dump(camel_case=camel_case) for item in self.tools]
        return result

    def as_write(self) -> AgentUpsert:
        """Returns this AgentUpsert in its writeable format"""
        return self

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentUpsert:
        tools = (
            [AgentToolUpsert._load(item) for item in resource.get("tools", [])]
            if isinstance(resource.get("tools"), Sequence)
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


@dataclass
class Agent(AgentCore):
    """Representation of an AI agent.
    This is the read format of an agent.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): The name of the agent, for use in user interfaces.
        description (str | None): The human readable description of the agent.
        instructions (str | None): Instructions for the agent.
        model (str | None): Name of the language model to use. For example, "azure/gpt-4o", "gcp/gemini-2.0" or "aws/claude-3.5-sonnet".
        tools (Sequence[AgentTool] | None): List of tools for the agent.
        created_time (int | None): The time the agent was created, in milliseconds since Thursday, 1 January 1970 00:00:00 UTC, minus leap seconds.
        last_updated_time (int | None): The time the agent was last updated, in milliseconds since Thursday, 1 January 1970 00:00:00 UTC, minus leap seconds.
        owner_id (str | None): The ID of the user who owns the agent.
    """

    tools: Sequence[AgentTool] | None = None
    created_time: int | None = None
    last_updated_time: int | None = None
    owner_id: str | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case=camel_case)
        if self.tools:
            result["tools"] = [item.dump(camel_case=camel_case) for item in self.tools]
        return result

    def __post_init__(self) -> None:
        if self.tools is not None:
            if not isinstance(self.tools, Sequence) or not all(isinstance(tool, AgentTool) for tool in self.tools):
                raise TypeError("Tools must be a sequence of AgentTool instances.")

    def as_write(self) -> AgentUpsert:
        """Returns this Agent in its writeable format"""
        return AgentUpsert(
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
            if isinstance(resource.get("tools"), Sequence)
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


class AgentUpsertList(CogniteResourceList[AgentUpsert], ExternalIDTransformerMixin):
    _RESOURCE = AgentUpsert


class AgentList(
    WriteableCogniteResourceList[AgentUpsert, Agent],
    ExternalIDTransformerMixin,
):
    _RESOURCE = Agent

    def as_write(self) -> AgentUpsertList:
        """Returns this AgentList as writeableinstance"""
        return AgentUpsertList([item.as_write() for item in self.data], cognite_client=self._get_cognite_client())
