from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast

if TYPE_CHECKING:
    from cognite.client import CogniteClient

from cognite.client.data_classes._base import (
    CogniteResourceList,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)


@dataclass
class AgentToolCore(WriteableCogniteResource["AgentToolUpsert"], ABC):
    """Core representation of an AI Agent Tool in CDF.

    Args:
        name (str): The name of the agent tool. Used by the agent to decide when to use this tool.
        description (str): The description of the agent tool. Used by the agent to decide when to use this tool.
    """

    _type: ClassVar[str]  # Will be set by concrete classes
    name: str
    description: str

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type."""
        result = super().dump(camel_case=camel_case)

        if hasattr(self.__class__, "_type"):
            result["type"] = self._type
        return result


@dataclass
class AgentToolUpsert(AgentToolCore, ABC):
    """The write format of an agent tool.

    Args:
        name (str): The name of the agent tool. Used by the agent to decide when to use this tool.
        description (str): The description of the agent tool. Used by the agent to decide when to use this tool.
    """

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type."""
        result = super().dump(camel_case=camel_case)
        # Use the _type from the class if available, otherwise use instance type
        result["type"] = getattr(self.__class__, "_type", getattr(self, "type", ""))
        return result

    def as_write(self) -> AgentToolUpsert:
        return self

    @classmethod
    def _load_tool(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentToolUpsert:
        return cls(
            name=resource["name"],
            description=resource["description"],
        )


@dataclass
class AgentTool(AgentToolCore, ABC):
    """The read format of an agent tool.

    Args:
        name (str): The name of the agent tool. Used by the agent to decide when to use this tool.
        description (str): The description of the agent tool. Used by the agent to decide when to use this tool.
    """

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type."""
        result = super().dump(camel_case=camel_case)
        # Use the _type from the class if available, otherwise use instance type
        result["type"] = getattr(self.__class__, "_type", getattr(self, "type", ""))
        return result

    @abstractmethod
    def as_write(self) -> AgentToolUpsert:
        """Convert this tool to its upsert representation."""
        pass

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentTool:
        tool_type = resource.get("type", "")

        # Get the appropriate class based on the tool.type
        tool_class = _AGENT_TOOL_CLS_BY_TYPE.get(tool_type, UnknownAgentTool)

        return tool_class._load_tool(resource, cognite_client)

    @classmethod
    def _load_tool(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentTool:
        """Default instance loading method for simple tool types."""
        return cls(
            name=resource["name"],
            description=resource["description"],
        )


@dataclass
class DataModelInfo(WriteableCogniteResource):
    """Information about a data model used in knowledge graph queries.

    Args:
        space (str): The space of the data model.
        external_id (str): The external ID of the data model.
        version (str): The version of the data model.
        view_external_ids (Sequence[str] | None): The external IDs of the views of the data model.
    """

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
    """Configuration for instance spaces in knowledge graph queries.

    Args:
        type (Literal["manual", "all"]): The type of instance spaces.
        spaces (Sequence[str] | None): The spaces of the instance spaces.
    """

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
    """Configuration for knowledge graph query agent tools.

    Args:
        data_models (Sequence[DataModelInfo] | None): The data models and views to query.
        instance_spaces (InstanceSpaces | None): The instance spaces to query.
    """

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
    """Agent tool for summarizing documents.

    Args:
        name (str): The name of the agent tool. Used by the agent to decide when to use this tool.
        description (str): The description of the agent tool. Used by the agent to decide when to use this tool.
    """

    _type: ClassVar[str] = "summarizeDocument"

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
        )


@dataclass
class SummarizeDocumentAgentToolUpsert(AgentToolUpsert):
    """Upsert version of document summarization agent tool.

    Args:
        name (str): The name of the agent tool. Used by the agent to decide when to use this tool.
        description (str): The description of the agent tool. Used by the agent to decide when to use this tool.
    """

    _type: ClassVar[str] = "summarizeDocument"


@dataclass
class AskDocumentAgentTool(AgentTool):
    """Agent tool for asking questions about documents.

    Args:
        name (str): The name of the agent tool. Used by the agent to decide when to use this tool.
        description (str): The description of the agent tool. Used by the agent to decide when to use this tool.
    """

    _type: ClassVar[str] = "askDocument"

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
        )


@dataclass
class AskDocumentAgentToolUpsert(AgentToolUpsert):
    """Upsert version of document question agent tool.

    Args:
        name (str): The name of the agent tool. Used by the agent to decide when to use this tool.
        description (str): The description of the agent tool. Used by the agent to decide when to use this tool.
    """

    _type: ClassVar[str] = "askDocument"


@dataclass
class QueryKnowledgeGraphAgentTool(AgentTool):
    """Agent tool for querying knowledge graphs.

    Args:
        name (str): The name of the agent tool. Used by the agent to decide when to use this tool.
        description (str): The description of the agent tool. Used by the agent to decide when to use this tool.
        configuration (QueryKnowledgeGraphAgentToolConfiguration | None): The configuration of the knowledge graph query agent tool.
    """

    _type: ClassVar[str] = "queryKnowledgeGraph"
    configuration: QueryKnowledgeGraphAgentToolConfiguration | None = None

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
    """Upsert version of knowledge graph query agent tool.

    Args:
        name (str): The name of the agent tool. Used by the agent to decide when to use this tool.
        description (str): The description of the agent tool. Used by the agent to decide when to use this tool.
        configuration (QueryKnowledgeGraphAgentToolConfiguration | None): The configuration of the knowledge graph query agent tool.
    """

    _type: ClassVar[str] = "queryKnowledgeGraph"
    configuration: QueryKnowledgeGraphAgentToolConfiguration | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case=camel_case)
        if self.configuration:
            result["configuration"] = self.configuration.dump(camel_case=camel_case)
        return result


@dataclass
class QueryTimeSeriesDatapointsAgentTool(AgentTool):
    """Agent tool for querying time series datapoints.

    Args:
        name (str): The name of the agent tool. Used by the agent to decide when to use this tool.
        description (str): The description of the agent tool. Used by the agent to decide when to use this tool.
    """

    _type: ClassVar[str] = "queryTimeSeriesDatapoints"

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
        )


@dataclass
class QueryTimeSeriesDatapointsAgentToolUpsert(AgentToolUpsert):
    """Upsert version of time series datapoints query agent tool.

    Args:
        name (str): The name of the agent tool. Used by the agent to decide when to use this tool.
        description (str): The description of the agent tool. Used by the agent to decide when to use this tool.
    """

    _type: ClassVar[str] = "queryTimeSeriesDatapoints"


@dataclass
class UnknownAgentTool(AgentTool):
    """Agent tool for unknown/unrecognized tool types.

    Args:
        name (str): The name of the agent tool. Used by the agent to decide when to use this tool.
        description (str): The description of the agent tool. Used by the agent to decide when to use this tool.
        type (str): The type of the agent tool.
        configuration (dict[str, Any] | None): The configuration of the agent tool.
    """

    type: str
    configuration: dict[str, Any] | None = None

    def as_write(self) -> UnknownAgentToolUpsert:
        return UnknownAgentToolUpsert(
            name=self.name,
            type=self.type,
            description=self.description,
            configuration=self.configuration,
        )

    @classmethod
    def _load_tool(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> UnknownAgentTool:
        return cls(
            name=resource["name"],
            type=resource["type"],
            description=resource["description"],
            configuration=resource.get("configuration"),
        )


@dataclass
class UnknownAgentToolUpsert(AgentToolUpsert):
    """Upsert version of unknown agent tool.

    Args:
        name (str): The name of the agent tool. Used by the agent to decide when to use this tool.
        type (str): The type of the agent tool.
        description (str): The description of the agent tool. Used by the agent to decide when to use this tool.
        configuration (dict[str, Any] | None): The configuration of the agent tool.
    """

    type: str
    configuration: dict[str, Any] | None = None


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
    subclass._type: cast(type[AgentTool], subclass)
    for subclass in AgentTool.__subclasses__()
    if hasattr(subclass, "_type") and not getattr(subclass, "__abstractmethods__", None)
}
