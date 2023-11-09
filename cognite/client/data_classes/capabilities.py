from __future__ import annotations

import enum
import inspect
import logging
from abc import ABC
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, Sequence, cast

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from cognite.client.utils._auxiliary import load_yaml_or_json, rename_and_exclude_keys
from cognite.client.utils._text import (
    convert_all_keys_to_camel_case,
    convert_all_keys_to_snake_case,
    to_camel_case,
    to_snake_case,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient

logger = logging.getLogger(__name__)


@dataclass
class Capability(ABC):
    _capability_name: ClassVar[str]
    actions: Sequence[Action]
    scope: Scope

    class Action(enum.Enum):
        ...

    @dataclass(frozen=True)
    class Scope(ABC):
        _scope_name: ClassVar[str]

        @classmethod
        def load(cls, resource: dict | str) -> Self:
            resource = resource if isinstance(resource, dict) else load_yaml_or_json(resource)
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

        def dump(self, camel_case: bool = True) -> dict[str, Any]:
            if isinstance(self, UnknownScope):
                return {self.name: self.data}

            data = asdict(self)
            if camel_case:
                data = convert_all_keys_to_camel_case(data)
            return {self._scope_name: data}

        def is_within(self, other: Self) -> bool:
            raise NotImplementedError

    @classmethod
    def load(cls, resource: dict | str) -> Self:
        resource = resource if isinstance(resource, dict) else load_yaml_or_json(resource)
        known_acls = set(resource).intersection(_CAPABILITY_CLASS_BY_NAME)
        if len(known_acls) == 1:
            (name,) = known_acls
            capability_cls = _CAPABILITY_CLASS_BY_NAME[name]
            # TODO: We ignore extra keys that might be present like 'projectUrlNames' - are these needed?
            return cast(
                Self,
                capability_cls(
                    actions=[capability_cls.Action(action) for action in resource[name]["actions"]],
                    scope=Capability.Scope.load(resource[name]["scope"]),
                ),
            )
        elif not known_acls and len(resource) == 1:
            # We infer this as an unknown acl not yet added to the SDK:
            ((name, data),) = resource.items()
            return cast(
                Self,
                UnknownAcl(
                    capability_name=name,
                    actions=[UnknownAcl.Action.Unknown for _ in data["actions"]],
                    scope=Capability.Scope.load(data["scope"]),
                    raw_data=resource,
                ),
            )
        # We got more than one acl (highly unlikely) or we got an unknown acl + extra keys in the response:
        raise ValueError(
            "Unable to parse capability from API-response, please create an issue on Github: "
            "https://github.com/cognitedata/cognite-sdk-python"
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        if isinstance(self, UnknownAcl):
            return self.raw_data
        data = {
            "actions": [action.value for action in self.actions],
            "scope": self.scope.dump(camel_case=camel_case),
        }
        capability_name = self._capability_name
        return {to_camel_case(capability_name) if camel_case else to_snake_case(capability_name): data}

    def has_capability(self, other: Capability) -> bool:
        if not isinstance(self, type(other)):
            return False
        if not other.scope.is_within(self.scope):
            return False
        return not set(other.actions) - set(self.actions)


class ProjectScope(ABC):
    name: ClassVar[str] = "projectScope"

    @classmethod
    def load(cls, resource: dict | str) -> ProjectsScope | AllProjectsScope:
        resource = resource if isinstance(resource, dict) else load_yaml_or_json(resource)
        ((name, data),) = resource.items()
        if name != cls.name:
            raise ValueError(f"Expected '{cls.name}', got '{name}'")

        if "projects" in data:
            return ProjectsScope(projects=data["projects"])
        elif "allProjects" in data:
            return AllProjectsScope()
        raise ValueError(f"Unknown project scope: {data}")

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        if isinstance(self, AllProjectsScope):
            return {self.name: {"allProjects": {}}}
        elif isinstance(self, ProjectsScope):
            return {self.name: {"projects": self.projects}}
        raise ValueError(f"Unknown project scope: {self}")


@dataclass(frozen=True)
class AllProjectsScope(ProjectScope):
    ...


@dataclass(frozen=True)
class ProjectsScope(ProjectScope):
    projects: list[str]


@dataclass
class ProjectCapability(CogniteResource):
    """This represents an capability scoped for a project(s)."""

    capability: Capability

    class Scope:
        All = AllProjectsScope
        Projects = ProjectScope

    project_scope: AllProjectsScope | ProjectScope

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        capability = next(key for key in resource if key != "projectScope")

        return cls(
            capability=Capability.load({capability: resource[capability]}),
            project_scope=ProjectScope.load({ProjectScope.name: resource[ProjectScope.name]}),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped = self.capability.dump(camel_case=camel_case)
        dumped.update(self.project_scope.dump(camel_case=camel_case))
        return dumped


class ProjectCapabilitiesList(CogniteResourceList[ProjectCapability]):
    _RESOURCE = ProjectCapability


@dataclass(frozen=True)
class AllScope(Capability.Scope):
    _scope_name = "all"

    def is_within(self, other: Self) -> bool:
        return isinstance(other, AllScope)


@dataclass(frozen=True)
class CurrentUserScope(Capability.Scope):
    _scope_name = "currentuserscope"

    def is_within(self, other: Self) -> bool:
        return isinstance(other, (AllScope, CurrentUserScope))


@dataclass(frozen=True)
class IDScope(Capability.Scope):
    _scope_name = "idScope"
    ids: list[int]

    def is_within(self, other: Self) -> bool:
        return isinstance(other, AllScope) or type(self) is type(other) and set(self.ids).issubset(other.ids)


@dataclass(frozen=True)
class ExtractionPipelineScope(Capability.Scope):
    _scope_name = "extractionPipelineScope"
    ids: list[int]

    def is_within(self, other: Self) -> bool:
        return isinstance(other, AllScope) or type(self) is type(other) and set(self.ids).issubset(other.ids)


@dataclass(frozen=True)
class DataSetScope(Capability.Scope):
    _scope_name = "datasetScope"
    ids: list[int]

    def is_within(self, other: Self) -> bool:
        return isinstance(other, AllScope) or type(self) is type(other) and set(self.ids).issubset(other.ids)


@dataclass
class DatabaseTableScope:
    database_name: str
    table_names: list[str]


@dataclass(frozen=True)
class TableScope(Capability.Scope):
    _scope_name = "tableScope"
    dbs_to_tables: dict[str, DatabaseTableScope]

    def is_within(self, other: Self) -> bool:
        if isinstance(other, AllScope):
            return True
        if not isinstance(other, TableScope):
            return False

        for db in self.dbs_to_tables.values():
            if db.database_name not in other.dbs_to_tables:
                return False
            if not set(db.table_names).issubset(other.dbs_to_tables[db.database_name].table_names):
                return False
        return True


@dataclass(frozen=True)
class AssetRootIDScope(Capability.Scope):
    _scope_name = "assetRootIdScope"
    root_ids: list[int]

    def is_within(self, other: Self) -> bool:
        return isinstance(other, AllScope) or type(self) is type(other) and set(self.root_ids).issubset(other.root_ids)


@dataclass(frozen=True)
class ExperimentsScope(Capability.Scope):
    _scope_name = "experimentscope"
    experiments: list[str]

    def is_within(self, other: Self) -> bool:
        return (
            isinstance(other, AllScope)
            or type(self) is type(other)
            and set(self.experiments).issubset(other.experiments)
        )


@dataclass(frozen=True)
class SpaceIDScope(Capability.Scope):
    _scope_name = "spaceIdScope"
    space_ids: list[str]

    def is_within(self, other: Self) -> bool:
        return (
            isinstance(other, AllScope) or type(self) is type(other) and set(self.space_ids).issubset(other.space_ids)
        )


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

    def is_within(self, other: Self) -> bool:
        raise NotImplementedError("Unknown scopes cannot be compared")


_SCOPE_CLASS_BY_NAME: dict[str, type[Capability.Scope]] = {
    c._scope_name: c for c in Capability.Scope.__subclasses__() if not issubclass(c, UnknownScope)
}
# Manual additions because of lack of API standardisation:
_SCOPE_CLASS_BY_NAME["idscope"] = IDScope


@dataclass
class UnknownAcl(Capability):
    """
    This class is used for capabilities that are not implemented in this version of the SDK.
    Typically, experimental capabilities or new capabilities that have recently been added to the API.
    """

    capability_name: str
    raw_data: dict[str, Any]

    class Action(Capability.Action):
        Unknown = "UNKNOWN"


@dataclass
class AnalyticsAcl(Capability):
    _capability_name = "analyticsAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):
        Read = "READ"
        Execute = "EXECUTE"
        List = "LIST"

    class Scope:
        All = AllScope


@dataclass
class AnnotationsAcl(Capability):
    _capability_name = "annotationsAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"
        Suggest = "SUGGEST"
        Review = "REVIEW"

    class Scope:
        All = AllScope


@dataclass
class AssetsAcl(Capability):
    _capability_name = "assetsAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope


@dataclass
class DataSetsAcl(Capability):
    _capability_name = "datasetsAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"
        Owner = "OWNER"

    class Scope:
        All = AllScope
        DataSet = DataSetScope


@dataclass
class DigitalTwinAcl(Capability):
    _capability_name = "digitalTwinAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"


@dataclass
class EntityMatchingAcl(Capability):
    _capability_name = "entitymatchingAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class EventsAcl(Capability):
    _capability_name = "eventsAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope


@dataclass
class ExtractionPipelinesAcl(Capability):
    _capability_name = "extractionPipelinesAcl"
    actions: Sequence[Action]
    scope: AllScope | IDScope | DataSetScope

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        ID = IDScope
        DataSet = DataSetScope


@dataclass
class ExtractionsRunAcl(Capability):
    _capability_name = "extractionRunsAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope | ExtractionPipelineScope

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope
        ExtractionPipeline = ExtractionPipelineScope


@dataclass
class ExtractionConfigsAcl(Capability):
    _capability_name = "extractionConfigsAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope | ExtractionPipelineScope

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope
        ExtractionPipeline = ExtractionPipelineScope


@dataclass
class FilesAcl(Capability):
    _capability_name = "filesAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope


@dataclass
class FunctionsAcl(Capability):
    _capability_name = "functionsAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class GeospatialAcl(Capability):
    _capability_name = "geospatialAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class GeospatialCrsAcl(Capability):
    _capability_name = "geospatialCrsAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class GroupsAcl(Capability):
    _capability_name = "groupsAcl"
    actions: Sequence[Action]
    scope: AllScope | CurrentUserScope

    class Action(Capability.Action):
        Create = "CREATE"
        Delete = "DELETE"
        Read = "READ"
        List = "LIST"
        Update = "UPDATE"

    class Scope:
        All = AllScope
        CurrentUser = CurrentUserScope


@dataclass
class LabelsAcl(Capability):
    _capability_name = "labelsAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope


@dataclass
class ProjectsAcl(Capability):
    _capability_name = "projectsAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"
        List = "LIST"
        Update = "UPDATE"

    class Scope:
        All = AllScope


@dataclass
class RawAcl(Capability):
    _capability_name = "rawAcl"
    actions: Sequence[Action]
    scope: AllScope | TableScope

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"
        List = "LIST"

    class Scope:
        All = AllScope
        Table = TableScope


@dataclass
class RelationshipsAcl(Capability):
    _capability_name = "relationshipsAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope


@dataclass
class RoboticsAcl(Capability):
    _capability_name = "roboticsAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope

    class Action(Capability.Action):
        Read = "READ"
        Create = "CREATE"
        Update = "UPDATE"
        Delete = "DELETE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope


@dataclass
class SecurityCategoriesAcl(Capability):
    _capability_name = "securityCategoriesAcl"
    actions: Sequence[Action]
    scope: AllScope | IDScope

    class Action(Capability.Action):
        MemberOf = "MEMBEROF"
        List = "LIST"
        Create = "CREATE"
        Update = "UPDATE"
        Delete = "DELETE"

    class Scope:
        All = AllScope
        ID = IDScope


@dataclass
class SeismicAcl(Capability):
    _capability_name = "seismicAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class SequencesAcl(Capability):
    _capability_name = "sequencesAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope


@dataclass
class SessionsAcl(Capability):
    _capability_name = "sessionsAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):
        List = "LIST"
        Create = "CREATE"
        Delete = "DELETE"

    class Scope:
        All = AllScope


@dataclass
class ThreeDAcl(Capability):
    _capability_name = "threedAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope

    class Action(Capability.Action):
        Read = "READ"
        Create = "CREATE"
        Update = "UPDATE"
        Delete = "DELETE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope


@dataclass
class TimeSeriesAcl(Capability):
    _capability_name = "timeSeriesAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope | IDScope | AssetRootIDScope

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope
        ID = IDScope
        AssetRootID = AssetRootIDScope


@dataclass
class TimeSeriesSubscriptionsAcl(Capability):
    _capability_name = "timeSeriesSubscriptionsAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class TransformationsAcl(Capability):
    _capability_name = "transformationsAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope


@dataclass
class TypesAcl(Capability):
    _capability_name = "typesAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class WellsAcl(Capability):
    _capability_name = "wellsAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class ExperimentsAcl(Capability):
    _capability_name = "experimentAcl"
    actions: Sequence[Action]

    class Action(Capability.Action):
        Use = "USE"

    class Scope:
        ...


@dataclass
class TemplateGroupsAcl(Capability):
    _capability_name = "templateGroupsAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class TemplateInstancesAcl(Capability):
    _capability_name = "templateInstancesAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class DataModelInstancesAcl(Capability):
    _capability_name = "dataModelInstancesAcl"
    actions: Sequence[Action]
    scope: AllScope | SpaceIDScope

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        SpaceID = SpaceIDScope


@dataclass
class DataModelsAcl(Capability):
    _capability_name = "dataModelsAcl"
    actions: Sequence[Action]
    scope: AllScope | SpaceIDScope

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        SpaceID = SpaceIDScope


@dataclass
class PipelinesAcl(Capability):
    _capability_name = "pipelinesAcl"
    actions: Sequence[Action]
    scope: AllScope

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class DocumentPipelinesAcl(Capability):
    _capability_name = "documentPipelinesAcl"
    actions: Sequence[Action]
    scope: AllScope

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class FilePipelinesAcl(Capability):
    _capability_name = "filePipelinesAcl"
    actions: Sequence[Action]
    scope: AllScope

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class NotificationsAcl(Capability):
    _capability_name = "notificationsAcl"
    actions: Sequence[Action]
    scope: AllScope

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class ScheduledCalculationsAcl(Capability):
    _capability_name = "scheduledCalculationsAcl"
    actions: Sequence[Action]
    scope: AllScope

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class MonitoringTasksAcl(Capability):
    _capability_name = "monitoringTasksAcl"
    actions: Sequence[Action]
    scope: AllScope

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class HostedExtractorsAcl(Capability):
    _capability_name = "hostedExtractorsAcl"
    actions: Sequence[Action]
    scope: AllScope

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class VisionModelAcl(Capability):
    _capability_name = "visionModelAcl"
    actions: Sequence[Action]
    scope: AllScope

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class DocumentFeedbackAcl(Capability):
    _capability_name = "documentFeedbackAcl"
    actions: Sequence[Action]
    scope: AllScope

    class Action(Capability.Action):
        Create = "CREATE"
        Read = "READ"
        Delete = "DELETE"

    class Scope:
        All = AllScope


@dataclass
class WorkflowOrchestrationAcl(Capability):
    _capability_name = "workflowOrchestrationAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


_CAPABILITY_CLASS_BY_NAME: dict[str, type[Capability]] = {
    c._capability_name: c for c in Capability.__subclasses__() if not issubclass(c, UnknownAcl)
}
# Give all Actions a better error message (instead of implementing __missing__ for all):
for acl in _CAPABILITY_CLASS_BY_NAME.values():
    if acl.Action.__members__:
        _cls = type(next(iter(acl.Action.__members__.values())))
        _cls.__name__ = f"{acl.__name__} {_cls.__name__}"
