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


class _ActionBase(enum.Enum):
    ...


@dataclass(frozen=True)
class Scope(ABC):
    _scope_name: ClassVar[str]

    @classmethod
    def load(cls, resource: dict | str) -> Self:
        resource = resource if isinstance(resource, dict) else json.loads(resource)
        ((name, data),) = resource.items()
        scope_cls = _SCOPE_CLASS_BY_NAME.get(name, UnknownScope)
        if scope_cls is UnknownScope:
            data["name"] = name
        data = convert_all_keys_to_snake_case(data)

        return cast(Self, scope_cls(**data))

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        data = asdict(self)
        if camel_case:
            data = convert_all_keys_to_camel_case(data)
        if isinstance(self, UnknownScope):
            scope_name = self.name
        else:
            scope_name = self._scope_name
        return {scope_name: data}


@dataclass(frozen=True)
class AllScope(Scope):
    _scope_name = "all"


@dataclass(frozen=True)
class CurrentUserScope(Scope):
    _scope_name = "currentuserScope"


@dataclass(frozen=True)
class DataSetScope(Scope):
    _scope_name = "datasetScope"
    data_set_ids: list[int]


@dataclass(frozen=True)
class IDScope(Scope):
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
class TableScope(Scope):
    _scope_name = "tableScope"
    dbs_to_tables: dict[str, DatabaseTableScope]


@dataclass(frozen=True)
class AssetRootIDScope(IDScope):
    _scope_name = "assetRootIdScope"
    root_ids: list[int]


@dataclass(frozen=True)
class ExperimentsScope(Scope):
    _scope_name = "experimentscope"
    experiments: list[str]


@dataclass(frozen=True)
class SpaceIDScope(Scope):
    _scope_name = "spaceIdScope"
    ids: list[str]


@dataclass(frozen=True)
class UnknownScope(Scope):
    """
    This class is used for scopes that are not implemented in this version of the SDK.
    Typically, experimental scopes or new scopes that have recently been added to the API.
    """

    name: str
    data: dict[str, Any]


_SCOPE_CLASS_BY_NAME: dict[str, type[Scope]] = {
    c._scope_name: c for c in Scope.__subclasses__() if not issubclass(c, UnknownScope)
}


@dataclass
class Capability(ABC):
    class Action(_ActionBase):
        ...

    _valid_scopes: ClassVar[frozenset[type[Scope]]]
    _capability_name: ClassVar[str]
    actions: Sequence[_ActionBase]
    scope: Scope

    @property
    def available_actions(self) -> list[_ActionBase]:
        return list(self.Action)

    @property
    def available_scopes(self) -> frozenset[type[Scope]]:
        return self._valid_scopes

    @classmethod
    def load(cls, resource: dict | str) -> Self:
        resource = resource if isinstance(resource, dict) else json.loads(resource)
        ((name, data),) = resource.items()
        capability_cls = _CAPABILITY_CLASS_BY_NAME.get(name, UnknownAcl)
        try:
            args: dict[str, Any] = dict(
                actions=[capability_cls.Action(action) for action in data["actions"]], scope=Scope.load(data["scope"])
            )
        except ValueError:
            raise

        if capability_cls is UnknownAcl:
            args["capability_name"] = name
        return cast(Self, capability_cls(**args))

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        data = {
            "actions": [action.value for action in self.actions],
            "scope": self.scope.dump(camel_case=camel_case),
        }
        if isinstance(self, UnknownAcl):
            capability_name = self.capability_name
        else:
            capability_name = self._capability_name
        return {to_camel_case(capability_name) if camel_case else to_snake_case(capability_name): data}


@dataclass
class UnknownAcl(Capability):
    """
    This class is used for capabilities that are not implemented in this version of the SDK.
    Typically, experimental capabilities or new capabilities that have recently been added to the API.
    """

    _valid_scopes = frozenset()
    capability_name: str


@dataclass
class AnalyticsAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Execute = "EXECUTE"
        List = "LIST"

    _valid_scopes = frozenset({AllScope})
    _capability_name = "analyticsAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class AnnotationsAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"
        Suggest = "SUGGEST"
        Review = "REVIEW"

    _valid_scopes = frozenset({AllScope})
    _capability_name = "annotationsAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class AssetsAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope, DataSetScope})
    _capability_name = "assetsAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope


@dataclass
class DataSetsAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"
        Owner = "OWNER"

    _valid_scopes = frozenset({AllScope, DataSetScope})
    _capability_name = "datasetsAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope


@dataclass
class DigitalTwinAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope})
    _capability_name = "digitalTwinAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class EntityMatchingAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope})
    _capability_name = "entitymatchingAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class EventsAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope, DataSetScope})
    _capability_name = "eventsAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope


@dataclass
class ExtractionPipelinesAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope, IDScope, DataSetScope})
    _capability_name = "extractionPipelinesAcl"
    actions: Sequence[Action]
    scope: AllScope | IDScope | DataSetScope


@dataclass
class ExtractionsRunAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope, DataSetScope, ExtractionPipelineScope})
    _capability_name = "extractionRunsAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope | ExtractionPipelineScope


@dataclass
class FilesAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope, DataSetScope})
    _capability_name = "filesAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope


@dataclass
class FunctionsAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope})
    _capability_name = "functionsAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class GeospatialAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope})
    _capability_name = "geospatialAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class GeospatialCrsAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope})
    _capability_name = "geospatialCrsAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class GroupsAcl(Capability):
    class Action(_ActionBase):
        Create = "CREATE"
        Delete = "DELETE"
        Read = "READ"
        List = "LIST"
        Update = "UPDATE"

    _valid_scopes = frozenset({AllScope, CurrentUserScope})
    _capability_name = "groupsAcl"
    actions: Sequence[Action]
    scope: AllScope | CurrentUserScope


@dataclass
class LabelsAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope, DataSetScope})
    _capability_name = "labelsAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope


@dataclass
class ModelHostingAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope})
    _capability_name = "modelHostingAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class ProjectsAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"
        List = "LIST"
        Update = "UPDATE"

    _valid_scopes = frozenset({AllScope})
    _capability_name = "projectsAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class RawAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"
        List = "LIST"

    _valid_scopes = frozenset({AllScope, TableScope})
    _capability_name = "rawAcl"
    actions: Sequence[Action]
    scope: AllScope | TableScope


@dataclass
class RelationshipsAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope, DataSetScope})
    _capability_name = "relationshipsAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope


@dataclass
class RoboticsAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"
        Create = "CREATE"
        Update = "UPDATE"

    _valid_scopes = frozenset({AllScope, DataSetScope})
    _capability_name = "roboticsAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope


@dataclass
class SecurityCategoriesAcl(Capability):
    class Action(_ActionBase):
        MemberOf = "MEMBEROF"
        List = "LIST"
        Create = "CREATE"
        Update = "UPDATE"
        Delete = "DELETE"

    _valid_scopes = frozenset({AllScope, IDScope})
    _capability_name = "securityCategoriesAcl"
    actions: Sequence[Action]
    scope: AllScope | IDScope


@dataclass
class SeismicAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope})
    _capability_name = "seismicAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class SequencesAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope, DataSetScope})
    _capability_name = "sequencesAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope


@dataclass
class SessionsAcl(Capability):
    class Action(_ActionBase):
        List = "LIST"
        Create = "CREATE"
        Delete = "DELETE"

    _valid_scopes = frozenset({AllScope})
    _capability_name = "sessionsAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class ThreeDAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Create = "CREATE"
        Update = "UPDATE"
        Delete = "DELETE"

    _valid_scopes = frozenset({AllScope, DataSetScope})
    _capability_name = "threedAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope


@dataclass
class TimeSeriesAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope, DataSetScope, IDScope, AssetRootIDScope})
    _capability_name = "timeSeriesAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope | IDScope | AssetRootIDScope


@dataclass
class TimeSeriesSubscriptionsAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope})
    _capability_name = "timeSeriesSubscriptionsAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class TransformationsAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope, DataSetScope})
    _capability_name = "transformationsAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope


@dataclass
class TypesAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope})
    _capability_name = "typesAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class WellsAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope})
    _capability_name = "wellsAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class ExperimentsAcl(Capability):
    class Action(_ActionBase):
        Use = "USE"

    _valid_scopes = frozenset({})
    _capability_name = "experimentAcl"
    actions: Sequence[Action]


@dataclass
class TemplateGroupsAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope})
    _capability_name = "templateGroupsAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class TemplateInstancesAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope})
    _capability_name = "templateInstancesAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class DataModelInstancesAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope, SpaceIDScope})
    _capability_name = "dataModelInstancesAcl"
    actions: Sequence[Action]
    scope: AllScope | SpaceIDScope


@dataclass
class DataModelsAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope, SpaceIDScope})
    _capability_name = "dataModelsAcl"
    actions: Sequence[Action]
    scope: AllScope | SpaceIDScope


@dataclass
class WorkflowOrchestrationAcl(Capability):
    class Action(_ActionBase):
        Read = "READ"
        Write = "WRITE"

    _valid_scopes = frozenset({AllScope})
    _capability_name = "workflowOrchestrationAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


_CAPABILITY_CLASS_BY_NAME: dict[str, type[Capability]] = {
    c._capability_name: c for c in Capability.__subclasses__() if not issubclass(c, UnknownAcl)
}
