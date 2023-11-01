from __future__ import annotations

import enum
import json
from abc import ABC
from dataclasses import asdict, dataclass, field
from typing import Any, ClassVar, Sequence, cast

from typing_extensions import Self

from cognite.client.utils._text import (
    convert_all_keys_to_camel_case,
    convert_all_keys_to_snake_case,
    to_camel_case,
    to_snake_case,
)


@dataclass(frozen=True)
class ScopeBase(ABC):
    _scope_name: ClassVar[str]

    @classmethod
    def load(cls, resource: dict | str) -> Self:
        resource = resource if isinstance(resource, dict) else json.loads(resource)
        ((name, data),) = resource.items()
        data = convert_all_keys_to_snake_case(data)
        if scope_cls := _SCOPE_CLASS_BY_NAME.get(name):
            return cast(Self, scope_cls(**data))
        return cast(Self, UnknownScope(name=name, data=data))

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        if isinstance(self, UnknownScope):
            return {self.name: self.data}

        data = asdict(self)
        if camel_case:
            data = convert_all_keys_to_camel_case(data)
        return {self._scope_name: data}


@dataclass(frozen=True)
class AllScope(ScopeBase):
    _scope_name = "all"


@dataclass(frozen=True)
class CurrentUserScope(ScopeBase):
    _scope_name = "currentuserScope"


@dataclass(frozen=True)
class DataSetScope(ScopeBase):
    _scope_name = "datasetScope"
    data_set_ids: list[int]


@dataclass(frozen=True)
class IDScope(ScopeBase):
    _scope_name = "idScope"
    ids: list[int]


@dataclass(frozen=True)
class ExtractionPipelineScope(IDScope):
    _scope_name = "extractionPipelineScope"


@dataclass
class DatabaseTableScope:
    database_name: str
    table_names: list[str]


@dataclass(frozen=True)
class TableScope(ScopeBase):
    _scope_name = "tableScope"
    dbs_to_tables: dict[str, DatabaseTableScope]


@dataclass(frozen=True)
class AssetRootIDScope(IDScope):
    _scope_name = "assetRootIdScope"
    root_ids: list[int]


@dataclass(frozen=True)
class ExperimentsScope(ScopeBase):
    _scope_name = "experimentscope"
    experiments: list[str]


@dataclass(frozen=True)
class SpaceIDScope(ScopeBase):
    _scope_name = "spaceIdScope"
    ids: list[str]


@dataclass(frozen=True)
class UnknownScope(ScopeBase):
    """
    This class is used for scopes that are not implemented in this version of the SDK.
    Typically, experimental scopes or new scopes that have recently been added to the API.
    """

    name: str
    data: dict[str, Any]

    def __getitem__(self, item: str) -> Any:
        return self.data[item]


_SCOPE_CLASS_BY_NAME: dict[str, type[ScopeBase]] = {
    c._scope_name: c for c in ScopeBase.__subclasses__() if not issubclass(c, UnknownScope)
}


@dataclass
class Capability(ABC):
    _capability_name: ClassVar[str]

    class Action(enum.Enum):
        ...

    class Scope:
        ...

    actions: Sequence
    scope: ScopeBase

    @classmethod
    def load(cls, resource: dict | str) -> Self:
        resource = resource if isinstance(resource, dict) else json.loads(resource)
        ((name, data),) = resource.items()
        if capability_cls := _CAPABILITY_CLASS_BY_NAME.get(name):
            return cast(
                Self,
                capability_cls(
                    actions=[capability_cls.Action(action) for action in data["actions"]],
                    scope=ScopeBase.load(data["scope"]),
                ),
            )

        return cast(
            Self, UnknownAcl(capability_name=name, actions=data["actions"], scope=ScopeBase.load(data["scope"]))
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        if isinstance(self, UnknownAcl):
            return {self.capability_name: {"actions": self.actions, "scope": self.scope.dump(camel_case=camel_case)}}
        data = {
            "actions": [action.value for action in self.actions],
            "scope": self.scope.dump(camel_case=camel_case),
        }
        capability_name = self._capability_name
        return {to_camel_case(capability_name) if camel_case else to_snake_case(capability_name): data}


@dataclass
class UnknownAcl(Capability):
    """
    This class is used for capabilities that are not implemented in this version of the SDK.
    Typically, experimental capabilities or new capabilities that have recently been added to the API.
    """

    capability_name: str
    actions: list[str]
    scope: ScopeBase


@dataclass
class AnalyticsAcl(Capability):
    _capability_name = "analyticsAcl"

    class Action(enum.Enum):
        Read = "READ"
        Execute = "EXECUTE"
        List = "LIST"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class AnnotationsAcl(Capability):
    _capability_name = "annotationsAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"
        Suggest = "SUGGEST"
        Review = "REVIEW"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class AssetsAcl(Capability):
    _capability_name = "assetsAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope

    actions: Sequence[Action]
    scope: AllScope | DataSetScope


@dataclass
class DataSetsAcl(Capability):
    _capability_name = "datasetsAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"
        Owner = "OWNER"

    class Scope:
        All = AllScope
        DataSet = DataSetScope

    actions: Sequence[Action]
    scope: AllScope | DataSetScope


@dataclass
class DigitalTwinAcl(Capability):
    _capability_name = "digitalTwinAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class EntityMatchingAcl(Capability):
    _capability_name = "entitymatchingAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class EventsAcl(Capability):
    _capability_name = "eventsAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope

    actions: Sequence[Action]
    scope: AllScope | DataSetScope


@dataclass
class ExtractionPipelinesAcl(Capability):
    _capability_name = "extractionPipelinesAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        ID = IDScope
        DataSet = DataSetScope

    actions: Sequence[Action]
    scope: AllScope | IDScope | DataSetScope


@dataclass
class ExtractionsRunAcl(Capability):
    _capability_name = "extractionRunsAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope
        ExtractionPipeline = ExtractionPipelineScope

    actions: Sequence[Action]
    scope: AllScope | DataSetScope | ExtractionPipelineScope


@dataclass
class FilesAcl(Capability):
    _capability_name = "filesAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope

    actions: Sequence[Action]
    scope: AllScope | DataSetScope


@dataclass
class FunctionsAcl(Capability):
    _capability_name = "functionsAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class GeospatialAcl(Capability):
    _capability_name = "geospatialAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class GeospatialCrsAcl(Capability):
    _capability_name = "geospatialCrsAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class GroupsAcl(Capability):
    _capability_name = "groupsAcl"

    class Action(enum.Enum):
        Create = "CREATE"
        Delete = "DELETE"
        Read = "READ"
        List = "LIST"
        Update = "UPDATE"

    class Scope:
        All = AllScope
        CurrentUser = CurrentUserScope

    actions: Sequence[Action]
    scope: AllScope | CurrentUserScope


@dataclass
class LabelsAcl(Capability):
    _capability_name = "labelsAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope

    actions: Sequence[Action]
    scope: AllScope | DataSetScope


@dataclass
class ProjectsAcl(Capability):
    _capability_name = "projectsAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"
        List = "LIST"
        Update = "UPDATE"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class RawAcl(Capability):
    _capability_name = "rawAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"
        List = "LIST"

    class Scope:
        All = AllScope
        Table = TableScope

    actions: Sequence[Action]
    scope: AllScope | TableScope


@dataclass
class RelationshipsAcl(Capability):
    _capability_name = "relationshipsAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope

    actions: Sequence[Action]
    scope: AllScope | DataSetScope


@dataclass
class RoboticsAcl(Capability):
    _capability_name = "roboticsAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"
        Create = "CREATE"
        Update = "UPDATE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope

    actions: Sequence[Action]
    scope: AllScope | DataSetScope


@dataclass
class SecurityCategoriesAcl(Capability):
    _capability_name = "securityCategoriesAcl"

    class Action(enum.Enum):
        MemberOf = "MEMBEROF"
        List = "LIST"
        Create = "CREATE"
        Update = "UPDATE"
        Delete = "DELETE"

    class Scope:
        All = AllScope
        ID = IDScope

    actions: Sequence[Action]
    scope: AllScope | IDScope


@dataclass
class SeismicAcl(Capability):
    _capability_name = "seismicAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class SequencesAcl(Capability):
    _capability_name = "sequencesAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope

    actions: Sequence[Action]
    scope: AllScope | DataSetScope


@dataclass
class SessionsAcl(Capability):
    _capability_name = "sessionsAcl"

    class Action(enum.Enum):
        List = "LIST"
        Create = "CREATE"
        Delete = "DELETE"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class ThreeDAcl(Capability):
    _capability_name = "threedAcl"

    class Action(enum.Enum):
        Read = "READ"
        Create = "CREATE"
        Update = "UPDATE"
        Delete = "DELETE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope

    actions: Sequence[Action]
    scope: AllScope | DataSetScope


@dataclass
class TimeSeriesAcl(Capability):
    _capability_name = "timeSeriesAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope
        ID = IDScope
        AssetRootID = AssetRootIDScope

    actions: Sequence[Action]
    scope: AllScope | DataSetScope | IDScope | AssetRootIDScope


@dataclass
class TimeSeriesSubscriptionsAcl(Capability):
    _capability_name = "timeSeriesSubscriptionsAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class TransformationsAcl(Capability):
    _capability_name = "transformationsAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope

    actions: Sequence[Action]
    scope: AllScope | DataSetScope


@dataclass
class TypesAcl(Capability):
    _capability_name = "typesAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class WellsAcl(Capability):
    _capability_name = "wellsAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class ExperimentsAcl(Capability):
    _capability_name = "experimentAcl"

    class Action(enum.Enum):
        Use = "USE"

    class Scope:
        ...

    actions: Sequence[Action]


@dataclass
class TemplateGroupsAcl(Capability):
    _capability_name = "templateGroupsAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class TemplateInstancesAcl(Capability):
    _capability_name = "templateInstancesAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class DataModelInstancesAcl(Capability):
    _capability_name = "dataModelInstancesAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        SpaceID = SpaceIDScope

    actions: Sequence[Action]
    scope: AllScope | SpaceIDScope


@dataclass
class DataModelsAcl(Capability):
    _capability_name = "dataModelsAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        SpaceID = SpaceIDScope

    actions: Sequence[Action]
    scope: AllScope | SpaceIDScope


@dataclass
class WorkflowOrchestrationAcl(Capability):
    _capability_name = "workflowOrchestrationAcl"

    class Action(enum.Enum):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


_CAPABILITY_CLASS_BY_NAME: dict[str, type[Capability]] = {
    c._capability_name: c for c in Capability.__subclasses__() if not issubclass(c, UnknownAcl)
}
