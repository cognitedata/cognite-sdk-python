from __future__ import annotations

from abc import ABC
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal

if TYPE_CHECKING:
    from cognite.client import CogniteClient

from cognite.client.data_classes._base import (
    CogniteResourceList,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)


@dataclass
class AgentToolCore(WriteableCogniteResource["AgentToolUpsert"], ABC):
    """Core representation of an AI Agent Tool in CDF."""

    _type: ClassVar[str]  # Will be set by concrete classes
    name: str
    description: str

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type."""
        result = super().dump(camel_case=camel_case)
        # Only add type if this class has _type defined
        if hasattr(self.__class__, "_type"):
            result["type"] = self._type
        return result


@dataclass
class AgentToolUpsert(AgentToolCore):
    """Representation of an AI Agent Tool in CDF.
    This is the write format of an agent tool.
    """

    type: str = ""  # Instance field for UnknownAgentToolUpsert - will be overridden by concrete classes

    def __init__(self, name: str, description: str, type: str = ""):
        self.name = name
        self.description = description
        # Use ClassVar if available, otherwise use passed type
        self.type = getattr(self.__class__, "_type", type)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type."""
        result = super().dump(camel_case=camel_case)
        # Override with instance type to handle UnknownAgentToolUpsert
        result["type"] = self.type
        return result

    def as_write(self) -> AgentToolUpsert:
        return self

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentToolUpsert:
        return cls(
            name=resource["name"],
            type=resource["type"],
            description=resource["description"],
        )


@dataclass
class AgentTool(AgentToolCore):
    """
    Representation of an AI Agent Tool in CDF.
    This is the read format of an agent tool.
    """

    type: str = ""  # Instance field for UnknownAgentTool - will be overridden by concrete classes
    created_time: int | None = None
    last_updated_time: int | None = None
    owner_id: str | None = None

    def __init__(
        self,
        name: str,
        description: str,
        type: str = "",
        created_time: int | None = None,
        last_updated_time: int | None = None,
        owner_id: str | None = None,
    ):
        self.name = name
        self.description = description
        # Use ClassVar if available, otherwise use passed type
        self.type = getattr(self.__class__, "_type", type)
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.owner_id = owner_id

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type."""
        result = super().dump(camel_case=camel_case)
        # Override with instance type to handle UnknownAgentTool
        result["type"] = self.type
        return result

    def as_write(self) -> AgentToolUpsert:
        # Handle configuration dynamically - only if the tool has it
        config = getattr(self, "configuration", None)
        config_for_upsert = config.dump() if config is not None and hasattr(config, "dump") else config

        # Create base upsert
        upsert = AgentToolUpsert(
            name=self.name,
            type=self.type,
            description=self.description,
        )

        # Add configuration if it exists
        if config is not None:
            setattr(upsert, "configuration", config_for_upsert)

        return upsert

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentTool:
        tool_type = resource.get("type", "")

        # Get the appropriate class based on the tool type
        tool_class = _AGENT_TOOL_CLS_BY_TYPE.get(tool_type, UnknownAgentTool)

        # Let each tool class handle its own loading logic
        return tool_class._load_tool(resource, cognite_client)

    @classmethod
    def _load_tool(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentTool:
        """Default instance loading method for simple tool types."""
        return cls(
            name=resource["name"],
            type=resource["type"],
            description=resource["description"],
            created_time=resource.get("created_time"),
            last_updated_time=resource.get("last_updated_time"),
            owner_id=resource.get("owner_id"),
        )


@dataclass
class DataModelInfo(WriteableCogniteResource):
    """Information about a data model used in knowledge graph queries."""

    space: str
    external_id: str
    version: str
    view_external_ids: Sequence[str] | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> DataModelInfo:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            version=resource["version"],
            view_external_ids=resource.get("viewExternalIds"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case=camel_case)
        return result

    def as_write(self) -> DataModelInfo:
        return self


@dataclass
class InstanceSpaces(WriteableCogniteResource):
    """Configuration for instance spaces in knowledge graph queries."""

    type: Literal["manual", "all"]
    spaces: Sequence[str] | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> InstanceSpaces:
        return cls(
            type=resource["type"],
            spaces=resource.get("spaces"),
        )

    def as_write(self) -> InstanceSpaces:
        return self


@dataclass
class QueryKnowledgeGraphAgentToolConfiguration(WriteableCogniteResource):
    """Configuration for knowledge graph query agent tools."""

    data_models: Sequence[DataModelInfo] | None = None
    instance_spaces: InstanceSpaces | None = None

    @classmethod
    def _load(
        cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None
    ) -> QueryKnowledgeGraphAgentToolConfiguration:
        data_models = None
        if "dataModels" in resource:
            data_models = [DataModelInfo._load(dm) for dm in resource["dataModels"]]

        instance_spaces = None
        if "instanceSpaces" in resource:
            instance_spaces = InstanceSpaces._load(resource["instanceSpaces"])

        return cls(
            data_models=data_models,
            instance_spaces=instance_spaces,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.data_models:
            key = "dataModels" if camel_case else "data_models"
            result[key] = [dm.dump(camel_case=camel_case) for dm in self.data_models]
        if self.instance_spaces:
            key = "instanceSpaces" if camel_case else "instance_spaces"
            result[key] = self.instance_spaces.dump(camel_case=camel_case)
        return result

    def as_write(self) -> QueryKnowledgeGraphAgentToolConfiguration:
        return self


@dataclass
class SummarizeDocumentAgentTool(AgentTool):
    """Agent tool for summarizing documents."""

    _type: ClassVar[str] = "summarizeDocument"

    def __init__(
        self,
        name: str,
        description: str,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        owner_id: str | None = None,
    ):
        super().__init__(
            name=name,
            description=description,
            created_time=created_time,
            last_updated_time=last_updated_time,
            owner_id=owner_id,
        )
        self.configuration = None

    def as_write(self) -> SummarizeDocumentAgentToolUpsert:
        return SummarizeDocumentAgentToolUpsert(
            name=self.name,
            description=self.description,
        )

    @classmethod
    def _load_tool(
        cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None
    ) -> SummarizeDocumentAgentTool:
        return cls(
            name=resource["name"],
            description=resource["description"],
            created_time=resource.get("created_time"),
            last_updated_time=resource.get("last_updated_time"),
            owner_id=resource.get("owner_id"),
        )


@dataclass
class SummarizeDocumentAgentToolUpsert(AgentToolUpsert):
    """Upsert version of document summarization agent tool."""

    _type: ClassVar[str] = "summarizeDocument"

    def __init__(self, name: str, description: str):
        super().__init__(name=name, description=description)
        self.configuration = None


@dataclass
class AskDocumentAgentTool(AgentTool):
    """Agent tool for asking questions about documents."""

    _type: ClassVar[str] = "askDocument"

    def __init__(
        self,
        name: str,
        description: str,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        owner_id: str | None = None,
    ):
        super().__init__(
            name=name,
            description=description,
            created_time=created_time,
            last_updated_time=last_updated_time,
            owner_id=owner_id,
        )
        self.configuration = None

    def as_write(self) -> AskDocumentAgentToolUpsert:
        return AskDocumentAgentToolUpsert(
            name=self.name,
            description=self.description,
        )

    @classmethod
    def _load_tool(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> AskDocumentAgentTool:
        return cls(
            name=resource["name"],
            description=resource["description"],
            created_time=resource.get("created_time"),
            last_updated_time=resource.get("last_updated_time"),
            owner_id=resource.get("owner_id"),
        )


@dataclass
class AskDocumentAgentToolUpsert(AgentToolUpsert):
    """Upsert version of document question agent tool."""

    _type: ClassVar[str] = "askDocument"

    def __init__(self, name: str, description: str):
        super().__init__(name=name, description=description)
        self.configuration = None


@dataclass
class QueryKnowledgeGraphAgentTool(AgentTool):
    """Agent tool for querying knowledge graphs."""

    _type: ClassVar[str] = "queryKnowledgeGraph"
    configuration: QueryKnowledgeGraphAgentToolConfiguration | None = None

    def __init__(
        self,
        name: str,
        description: str,
        configuration: QueryKnowledgeGraphAgentToolConfiguration | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        owner_id: str | None = None,
    ):
        super().__init__(
            name=name,
            description=description,
            created_time=created_time,
            last_updated_time=last_updated_time,
            owner_id=owner_id,
        )
        self.configuration = configuration

    @classmethod
    def _load_tool(
        cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None
    ) -> QueryKnowledgeGraphAgentTool:
        # Parse the configuration specifically for this tool type
        configuration = None
        if resource.get("configuration"):
            configuration = QueryKnowledgeGraphAgentToolConfiguration._load(resource["configuration"])

        return cls(
            name=resource["name"],
            description=resource["description"],
            configuration=configuration,
            created_time=resource.get("created_time"),
            last_updated_time=resource.get("last_updated_time"),
            owner_id=resource.get("owner_id"),
        )

    def as_write(self) -> QueryKnowledgeGraphAgentToolUpsert:
        return QueryKnowledgeGraphAgentToolUpsert(
            name=self.name,
            description=self.description,
            configuration=self.configuration,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case=camel_case)
        if self.configuration:
            result["configuration"] = self.configuration.dump(camel_case=camel_case)
        return result


@dataclass
class QueryKnowledgeGraphAgentToolUpsert(AgentToolUpsert):
    """Upsert version of knowledge graph query agent tool."""

    _type: ClassVar[str] = "queryKnowledgeGraph"
    configuration: QueryKnowledgeGraphAgentToolConfiguration | None = None

    def __init__(
        self, name: str, description: str, configuration: QueryKnowledgeGraphAgentToolConfiguration | None = None
    ):
        super().__init__(name=name, description=description)
        self.configuration = configuration

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case=camel_case)
        if self.configuration:
            result["configuration"] = self.configuration.dump(camel_case=camel_case)
        return result


@dataclass
class QueryTimeSeriesDatapointsAgentTool(AgentTool):
    """Agent tool for querying time series datapoints."""

    _type: ClassVar[str] = "queryTimeSeriesDatapoints"

    def __init__(
        self,
        name: str,
        description: str,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        owner_id: str | None = None,
    ):
        super().__init__(
            name=name,
            description=description,
            created_time=created_time,
            last_updated_time=last_updated_time,
            owner_id=owner_id,
        )
        self.configuration = None

    def as_write(self) -> QueryTimeSeriesDatapointsAgentToolUpsert:
        return QueryTimeSeriesDatapointsAgentToolUpsert(
            name=self.name,
            description=self.description,
        )

    @classmethod
    def _load_tool(
        cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None
    ) -> QueryTimeSeriesDatapointsAgentTool:
        return cls(
            name=resource["name"],
            description=resource["description"],
            created_time=resource.get("created_time"),
            last_updated_time=resource.get("last_updated_time"),
            owner_id=resource.get("owner_id"),
        )


@dataclass
class QueryTimeSeriesDatapointsAgentToolUpsert(AgentToolUpsert):
    """Upsert version of time series datapoints query agent tool."""

    _type: ClassVar[str] = "queryTimeSeriesDatapoints"

    def __init__(self, name: str, description: str):
        super().__init__(name=name, description=description)
        self.configuration = None


@dataclass
class UnknownAgentTool(AgentTool):
    """Agent tool for unknown/unrecognized tool types."""

    type: str
    configuration: dict[str, Any] | None = None  # Unknown tools can have any dict config
    # Note: UnknownAgentTool still requires type parameter since it can be anything

    @classmethod
    def _load_tool(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> UnknownAgentTool:
        # Unknown tools need to handle configuration from the resource
        return cls(
            name=resource["name"],
            type=resource["type"],
            description=resource["description"],
            configuration=resource.get("configuration"),
            created_time=resource.get("created_time"),
            last_updated_time=resource.get("last_updated_time"),
            owner_id=resource.get("owner_id"),
        )


@dataclass
class UnknownAgentToolUpsert(AgentToolUpsert):
    """Upsert version of unknown agent tool."""

    type: str
    configuration: dict[str, Any] | None = None
    # Note: UnknownAgentToolUpsert still requires type parameter since it can be anything


class AgentToolUpsertList(CogniteResourceList[AgentToolUpsert]):
    _RESOURCE = AgentToolUpsert


class AgentToolList(
    WriteableCogniteResourceList[AgentToolUpsert, AgentTool],
):
    _RESOURCE = AgentTool

    def as_write(self) -> AgentToolUpsertList:
        """Returns this agent tool list as a writeable instance"""
        return AgentToolUpsertList([item.as_write() for item in self.data], cognite_client=self._get_cognite_client())


# Build the mapping AFTER all classes are defined
_AGENT_TOOL_CLS_BY_TYPE: dict[str, type[AgentTool]] = {
    subclass._type: subclass for subclass in AgentTool.__subclasses__() if hasattr(subclass, "_type")
}
