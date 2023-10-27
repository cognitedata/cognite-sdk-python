from __future__ import annotations

import enum
import json
from abc import ABC
from dataclasses import asdict, dataclass, field
from typing import Any, ClassVar, cast

from typing_extensions import Self

from cognite.client.utils._text import (
    convert_all_keys_to_camel_case,
    convert_all_keys_to_snake_case,
    to_camel_case,
    to_snake_case,
)


class Action(enum.Enum):
    Read = enum.auto()
    Write = enum.auto()
    List = enum.auto()
    Owner = enum.auto()
    Create = enum.auto()
    Update = enum.auto()
    Delete = enum.auto()
    Suggest = enum.auto()
    Review = enum.auto()
    MemberOf = enum.auto()
    Use = enum.auto()
    Execute = enum.auto()


@dataclass(frozen=True)
class Scope(ABC):
    _scope_name: ClassVar[str]

    @classmethod
    def load(cls, resource: dict | str) -> Self:
        resource = resource if isinstance(resource, dict) else json.loads(resource)
        ((name, data),) = resource.keys()
        scope_cls = _SCOPE_CLASS_BY_NAME.get(name, UnknownScope)
        if scope_cls is UnknownScope:
            data["name"] = name
        data = convert_all_keys_to_snake_case(resource[name])
        return cast(Self, scope_cls(**data))

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        data = asdict(self)
        if camel_case:
            data = convert_all_keys_to_camel_case(data)
        class_name = type(self).__name__
        return {to_camel_case(class_name) if camel_case else to_snake_case(class_name): data}


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
class ExperimentScope(Scope):
    _scope_name = "experimentScope"
    experiments: list[str]


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
    _valid_scopes: ClassVar[frozenset[type[Scope]]]
    _valid_actions: ClassVar[frozenset[Action]]
    _capability_name: ClassVar[str]
    actions: list[Action]
    scope: Scope

    @property
    def available_actions(self) -> frozenset[Action]:
        return self._valid_actions

    @property
    def available_scopes(self) -> frozenset[type[Scope]]:
        return self._valid_scopes

    @classmethod
    def load(cls, resource: dict | str) -> Self:
        resource = resource if isinstance(resource, dict) else json.loads(resource)
        ((name, data),) = resource.items()
        args: dict[str, Any] = dict(
            actions=[Action[action] for action in data["actions"]], scope=Scope.load(data["scope"])
        )
        capability_cls = _CAPABILITY_CLASS_BY_NAME.get(name, UnknownAcl)
        if capability_cls is UnknownAcl:
            args["capability_name"] = name
        return cast(Self, capability_cls(**args))

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        data = {
            "actions": [action.name if isinstance(action, Action) else action for action in self.actions],
            "scope": self.scope.dump(camel_case=camel_case) if isinstance(self.scope, Scope) else self.scope,
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
    _valid_actions = frozenset()
    capability_name: str


@dataclass
class AnalyticsAcl(Capability):
    _valid_scopes = frozenset({AllScope})
    _valid_actions = frozenset({Action.Read, Action.Execute, Action.List})
    _capability_name = "analyticsAcl"
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class AnnotationsAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write, Action.Suggest, Action.Review})
    _valid_scopes = frozenset({AllScope})
    _capability_name = "annotationsAcl"
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class AssetsAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write})
    _valid_scopes = frozenset({AllScope, DataSetScope})
    _capability_name = "assetsAcl"
    scope: AllScope | DataSetScope


@dataclass
class DataSetsAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write, Action.Owner})
    _valid_scopes = frozenset({AllScope, DataSetScope})
    _capability_name = "dataSetsAcl"
    scope: AllScope | DataSetScope


@dataclass
class DigitalTwinAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write})
    _valid_scopes = frozenset({AllScope})
    _capability_name = "digitalTwinAcl"
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class EntityMatchingAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write})
    _valid_scopes = frozenset({AllScope})
    _capability_name = "entityMatchingAcl"
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class EventsAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write})
    _valid_scopes = frozenset({AllScope, DataSetScope})
    _capability_name = "eventsAcl"
    scope: AllScope | DataSetScope


@dataclass
class ExtractionPipelineAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write})
    _valid_scopes = frozenset({AllScope, IDScope, DataSetScope})
    _capability_name = "extractionPipelineAcl"
    scope: AllScope | IDScope | DataSetScope


@dataclass
class ExtractionRunAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write})
    _valid_scopes = frozenset({AllScope, DataSetScope, ExtractionPipelineScope})
    _capability_name = "extractionRunAcl"
    scope: AllScope | DataSetScope | ExtractionPipelineScope


@dataclass
class FilesAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write})
    _valid_scopes = frozenset({AllScope, DataSetScope})
    _capability_name = "filesAcl"
    scope: AllScope | DataSetScope


@dataclass
class FunctionsAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write})
    _valid_scopes = frozenset({AllScope})
    _capability_name = "functionsAcl"
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class GeospatialAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write})
    _valid_scopes = frozenset({AllScope})
    _capability_name = "geospatialAcl"
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class GeospatialCrsAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write})
    _valid_scopes = frozenset({AllScope})
    _capability_name = "geospatialCrsAcl"
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class GroupsAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write, Action.Create, Action.Update, Action.Delete})
    _valid_scopes = frozenset({AllScope, CurrentUserScope})
    _capability_name = "groupsAcl"
    scope: AllScope | CurrentUserScope


@dataclass
class LabelsAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write})
    _valid_scopes = frozenset({AllScope, DataSetScope})
    _capability_name = "labelsAcl"
    scope: AllScope | DataSetScope


@dataclass
class ModelHostingAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write})
    _valid_scopes = frozenset({AllScope})
    _capability_name = "modelHostingAcl"
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class ProjectsAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write, Action.List, Action.Update})
    _valid_scopes = frozenset({AllScope})
    _capability_name = "projectsAcl"
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class RawAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write, Action.List})
    _valid_scopes = frozenset({AllScope, TableScope})
    _capability_name = "rawAcl"
    scope: AllScope | TableScope


@dataclass
class RelationshipsAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write})
    _valid_scopes = frozenset({AllScope, DataSetScope})
    _capability_name = "relationshipsAcl"
    scope: AllScope | DataSetScope


@dataclass
class RoboticsAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write, Action.Create, Action.Update})
    _valid_scopes = frozenset({AllScope, DataSetScope})
    _capability_name = "roboticsAcl"
    scope: AllScope | DataSetScope


@dataclass
class SecurityCategoriesAcl(Capability):
    _valid_actions = frozenset({Action.MemberOf, Action.List, Action.Create, Action.Delete})
    _valid_scopes = frozenset({AllScope, IDScope})
    _capability_name = "securityCategoriesAcl"
    scope: AllScope | IDScope


@dataclass
class SeismicAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write})
    _valid_scopes = frozenset({AllScope})
    _capability_name = "seismicAcl"
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class SequencesAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write})
    _valid_scopes = frozenset({AllScope, DataSetScope})
    _capability_name = "sequencesAcl"
    scope: AllScope | DataSetScope


@dataclass
class SessionsAcl(Capability):
    _valid_actions = frozenset({Action.List, Action.Create})
    _valid_scopes = frozenset({AllScope})
    _capability_name = "sessionsAcl"
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class ThreeDAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write, Action.Update, Action.Delete})
    _valid_scopes = frozenset({AllScope, DataSetScope})
    _capability_name = "threeDAcl"
    scope: AllScope | DataSetScope


@dataclass
class TimeSeriesAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write})
    _valid_scopes = frozenset({AllScope, DataSetScope, IDScope, AssetRootIDScope})
    _capability_name = "timeSeriesAcl"
    scope: AllScope | DataSetScope | IDScope | AssetRootIDScope


@dataclass
class TimeSeriesSubscriptionAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write})
    _valid_scopes = frozenset({AllScope})
    _capability_name = "timeSeriesSubscriptionAcl"
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class TransformationsAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write})
    _valid_scopes = frozenset({AllScope, DataSetScope})
    _capability_name = "transformationsAcl"
    scope: AllScope | DataSetScope


@dataclass
class TypesAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write})
    _valid_scopes = frozenset({AllScope})
    _capability_name = "typesAcl"
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class WellsAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write})
    _valid_scopes = frozenset({AllScope})
    _capability_name = "wellsAcl"
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class ExperimentsAcl(Capability):
    _valid_actions = frozenset({Action.Use})
    _valid_scopes = frozenset({})
    _capability_name = "experimentsAcl"


_CAPABILITY_CLASS_BY_NAME: dict[str, type[Capability]] = {
    c._capability_name: c for c in Capability.__subclasses__() if not issubclass(c, UnknownAcl)
}
