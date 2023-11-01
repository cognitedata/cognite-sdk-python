from __future__ import annotations

import enum
import inspect
import json
import logging
from abc import ABC
from dataclasses import asdict, dataclass, field
from typing import Any, ClassVar, Sequence, cast

from typing_extensions import Self

from cognite.client.utils._auxiliary import rename_and_exclude_keys
from cognite.client.utils._text import (
    convert_all_keys_to_camel_case,
    convert_all_keys_to_snake_case,
    to_camel_case,
    to_snake_case,
)

logger = logging.getLogger(__name__)


@dataclass
class Capability(ABC):
    _capability_name: ClassVar[str]

    class Action(enum.Enum):
        ...

    @dataclass(frozen=True)
    class Scope(ABC):
        _scope_name: ClassVar[str]

        @classmethod
        def load(cls, resource: dict | str) -> Self:
            resource = resource if isinstance(resource, dict) else json.loads(resource)
            ((name, data),) = resource.items()
            data = convert_all_keys_to_snake_case(data)
            if scope_cls := _SCOPE_CLASS_BY_NAME.get(name):
                try:
                    return cast(Self, scope_cls(**data))
                except TypeError:
                    not_supported = set(data).difference(inspect.signature(scope_cls).parameters)
                    logger.warning(
                        f"For '{scope_cls.__name__}', the following unknown fields were ignored: {not_supported}. "
                        "Try updating to the latest SDK version, or create an issue on Github!"
                    )
                    return cast(Self, scope_cls(**rename_and_exclude_keys(data, exclude=not_supported)))
            return cast(Self, UnknownScope(name=name, data=data))

        def dump(self, camel_case: bool = False) -> dict[str, Any]:
            if isinstance(self, UnknownScope):
                return {self.name: self.data}

            data = asdict(self)
            if camel_case:
                data = convert_all_keys_to_camel_case(data)
            return {self._scope_name: data}

    actions: Sequence[Action]
    scope: Scope

    @classmethod
    def load(cls, resource: dict | str) -> Self:
        resource = resource if isinstance(resource, dict) else json.loads(resource)
        ((name, data),) = resource.items()
        if capability_cls := _CAPABILITY_CLASS_BY_NAME.get(name):
            return cast(
                Self,
                capability_cls(
                    actions=[capability_cls.Action(action) for action in data["actions"]],
                    scope=Capability.Scope.load(data["scope"]),
                ),
            )

        return cast(
            Self,
            UnknownAcl(
                capability_name=name,
                actions=[UnknownAcl.Action.UNKNOWN for _ in data["actions"]],
                scope=Capability.Scope.load(data["scope"]),
                raw_data=resource,
            ),
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        if isinstance(self, UnknownAcl):
            return self.raw_data
        data = {
            "actions": [action.value for action in self.actions],
            "scope": self.scope.dump(camel_case=camel_case),
        }
        capability_name = self._capability_name
        return {to_camel_case(capability_name) if camel_case else to_snake_case(capability_name): data}


@dataclass(frozen=True)
class AllScope(Capability.Scope):
    _scope_name = "all"


@dataclass(frozen=True)
class CurrentUserScope(Capability.Scope):
    _scope_name = "currentuserScope"


@dataclass(frozen=True)
class IDScope(Capability.Scope):
    _scope_name = "idScope"
    ids: list[int]


@dataclass(frozen=True)
class ExtractionPipelineScope(Capability.Scope):
    _scope_name = "extractionPipelineScope"
    ids: list[int]


@dataclass(frozen=True)
class DataSetScope(Capability.Scope):
    _scope_name = "datasetScope"
    ids: list[int]


@dataclass
class DatabaseTableScope:
    database_name: str
    table_names: list[str]


@dataclass(frozen=True)
class TableScope(Capability.Scope):
    _scope_name = "tableScope"
    dbs_to_tables: dict[str, DatabaseTableScope]


@dataclass(frozen=True)
class AssetRootIDScope(IDScope):
    _scope_name = "assetRootIdScope"
    root_ids: list[int]


@dataclass(frozen=True)
class ExperimentsScope(Capability.Scope):
    _scope_name = "experimentscope"
    experiments: list[str]


@dataclass(frozen=True)
class SpaceIDScope(Capability.Scope):
    _scope_name = "spaceIdScope"
    ids: list[str]


@dataclass(frozen=True)
class UnknownScope(Capability.Scope):
    """
    This class is used for scopes that are not implemented in this version of the SDK.
    Typically, experimental scopes or new scopes that have recently been added to the API.
    """

    name: str
    data: dict[str, Any]

    def __getitem__(self, item: str) -> Any:
        return self.data[item]


_SCOPE_CLASS_BY_NAME: dict[str, type[Capability.Scope]] = {
    c._scope_name: c for c in Capability.Scope.__subclasses__() if not issubclass(c, UnknownScope)
}


@dataclass
class UnknownAcl(Capability):
    """
    This class is used for capabilities that are not implemented in this version of the SDK.
    Typically, experimental capabilities or new capabilities that have recently been added to the API.
    """

    class Action(Capability.Action):
        UNKNOWN = "UNKNOWN"

    capability_name: str
    raw_data: dict[str, Any]


@dataclass
class AnalyticsAcl(Capability):
    _capability_name = "analyticsAcl"

    class Action(Capability.Action):
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

    class Action(Capability.Action):
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

    class Action(Capability.Action):
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

    class Action(Capability.Action):
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

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class EntityMatchingAcl(Capability):
    _capability_name = "entitymatchingAcl"

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class EventsAcl(Capability):
    _capability_name = "eventsAcl"

    class Action(Capability.Action):
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

    class Action(Capability.Action):
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

    class Action(Capability.Action):
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

    class Action(Capability.Action):
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

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class GeospatialAcl(Capability):
    _capability_name = "geospatialAcl"

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class GeospatialCrsAcl(Capability):
    _capability_name = "geospatialCrsAcl"

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class GroupsAcl(Capability):
    _capability_name = "groupsAcl"

    class Action(Capability.Action):
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

    class Action(Capability.Action):
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

    class Action(Capability.Action):
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

    class Action(Capability.Action):
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

    class Action(Capability.Action):
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

    class Action(Capability.Action):
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

    class Action(Capability.Action):
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

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class SequencesAcl(Capability):
    _capability_name = "sequencesAcl"

    class Action(Capability.Action):
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

    class Action(Capability.Action):
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

    class Action(Capability.Action):
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

    class Action(Capability.Action):
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

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class TransformationsAcl(Capability):
    _capability_name = "transformationsAcl"

    class Action(Capability.Action):
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

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class WellsAcl(Capability):
    _capability_name = "wellsAcl"

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class ExperimentsAcl(Capability):
    _capability_name = "experimentAcl"

    class Action(Capability.Action):
        Use = "USE"

    class Scope:
        ...

    actions: Sequence[Action]


@dataclass
class TemplateGroupsAcl(Capability):
    _capability_name = "templateGroupsAcl"

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class TemplateInstancesAcl(Capability):
    _capability_name = "templateInstancesAcl"

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class DataModelInstancesAcl(Capability):
    _capability_name = "dataModelInstancesAcl"

    class Action(Capability.Action):
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

    class Action(Capability.Action):
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

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope

    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)


_CAPABILITY_CLASS_BY_NAME: dict[str, type[Capability]] = {
    c._capability_name: c for c in Capability.__subclasses__() if not issubclass(c, UnknownAcl)
}
