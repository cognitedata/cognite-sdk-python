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
from cognite.client.data_classes.agents.agent_tools import (
    AgentTool,
    AgentToolList,
    AgentToolUpsert,
    AgentToolUpsertList,
)


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

    def __init__(
        self,
        external_id: str,
        name: str,
        description: str | None = None,
        instructions: str | None = None,
        model: str | None = None,
        tools: Sequence[AgentToolUpsert] | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id, name=name, description=description, instructions=instructions, model=model
        )
        self.tools: AgentToolUpsertList | None = AgentToolUpsertList(tools) if tools is not None else None
        # This stores any unknown properties that are not part of the defined fields.
        # This is useful while the API is evolving and new fields are added.
        self._unknown_properties: dict[str, object] = {}

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case=camel_case)
        if self.tools:
            result["tools"] = [item.dump(camel_case=camel_case) for item in self.tools]
        if self._unknown_properties:
            result.update(self._unknown_properties)
        return result

    def as_write(self) -> AgentUpsert:
        """Returns this AgentUpsert in its writeable format"""
        return self

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentUpsert:
        tools = (
            [AgentTool._load(item, cognite_client).as_write() for item in resource.get("tools", [])]
            if isinstance(resource.get("tools"), Sequence)
            else None
        )

        instances = cls(
            external_id=resource["externalId"],
            name=resource["name"],
            description=resource.get("description"),
            instructions=resource.get("instructions"),
            model=resource.get("model"),
            tools=tools,
        )
        existing = set(instances.dump(camel_case=True).keys())
        instances._unknown_properties = {key: value for key, value in resource.items() if key not in existing}
        return instances


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

    def __init__(
        self,
        external_id: str,
        name: str,
        description: str | None = None,
        instructions: str | None = None,
        model: str | None = None,
        tools: Sequence[AgentTool] | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        owner_id: str | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id, name=name, description=description, instructions=instructions, model=model
        )
        self.tools: AgentToolList | None = AgentToolList(tools) if tools is not None else None
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.owner_id = owner_id
        # This stores any unknown properties that are not part of the defined fields.
        # This is useful while the API is evolving and new fields are added.
        self._unknown_properties: dict[str, object] = {}

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case=camel_case)
        if self.tools:
            result["tools"] = [item.dump(camel_case=camel_case) for item in self.tools]
        if self._unknown_properties:
            result.update(self._unknown_properties)
        return result

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

        instance = cls(
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
        existing = set(instance.dump(camel_case=True).keys())
        instance._unknown_properties = {key: value for key, value in resource.items() if key not in existing}
        return instance


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
