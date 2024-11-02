from __future__ import annotations

import difflib
import enum
import inspect
import itertools
import logging
import warnings
from abc import ABC
from collections.abc import Iterable, Sequence
from dataclasses import InitVar, asdict, dataclass, field
from itertools import product
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NamedTuple, NoReturn, cast

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


class CapabilityTuple(NamedTuple):
    acl_name: str
    action: str
    scope_name: str
    scope_id: int | str | None = None
    scope_extra: str | None = None

    @property
    def database(self) -> str:
        assert self.acl_name == RawAcl._capability_name and isinstance(self.scope_id, str)
        return self.scope_id

    @property
    def table(self) -> str:
        assert self.acl_name == RawAcl._capability_name and isinstance(self.scope_extra, str)
        return self.scope_extra


@dataclass
class Capability(ABC):
    _capability_name: ClassVar[str]
    actions: Sequence[Action]
    scope: Scope
    allow_unknown: InitVar[bool] = field(default=False, kw_only=True)

    def __post_init__(self, allow_unknown: bool) -> None:
        try:
            # There are so many things that may fail validation; non-enum passed, not iterable etc.
            # We always want to show the example usage to the user.
            if not allow_unknown:
                self._validate()
        except Exception as err:
            acl_name = "ACL" if (cls := type(self)) is UnknownAcl else cls.__name__
            raise ValueError(f"Could not instantiate {acl_name} due to: {err}. " + self.show_example_usage()) from err

    def _validate(self) -> None:
        if (capability_cls := type(self)) is UnknownAcl:
            raise ValueError(
                f"{self.capability_name!r} is not a known ACL. If it should be, "  # type: ignore [attr-defined]
                "please create an issue on: https://github.com/cognitedata/cognite-sdk-python"
            )
        acl = (capability_cls := type(self)).__name__
        if bad_actions := [a for a in self.actions if a not in capability_cls.Action]:
            full_action_examples = ", ".join(f"{acl}.Action.{action.name}" for action in capability_cls.Action)
            raise ValueError(
                f"{acl} got unknown action(s): {bad_actions}, expected a subset of: [{full_action_examples}]"
            )
        allowed_scopes, scope_names = _VALID_SCOPES_BY_CAPABILITY[capability_cls]
        if type(self.scope) in allowed_scopes or not allowed_scopes:
            return  # Acl-supported scope was passed (or acl doesn't use scopes?)

        if self.scope in allowed_scopes and self.scope in (AllScope, CurrentUserScope):
            # Easy mistake as these look like enums:
            self.scope = self.scope()  # type: ignore [operator]
        else:
            full_scope_examples = ", ".join(f"{acl}.Scope.{name}" for name in scope_names)
            raise ValueError(
                f"{acl} got an unknown scope: {self.scope}, expected an instance of one of: [{full_scope_examples}]"
            )

    @classmethod
    def show_example_usage(cls) -> str:
        acl, any_action = cls.__name__, next(iter(cls.Action))
        return f"Example usage: {acl}(actions=[{acl}.Action.{any_action.name}], scope={acl}.Scope.All())"

    class Action(enum.Enum):
        ...

        @classmethod
        def load(cls, action: str, allow_unknown: bool = False) -> Self:
            # Pythonistas, don't judge me, _missing_ does an isinstance check...
            try:
                return cls(action)
            except (ValueError, TypeError):
                if allow_unknown:
                    Action = enum.Enum("Action", {action.title(): action}, type=Capability.Action)  # type: ignore [misc]
                    return Action(action)  # type: ignore [return-value]

                # Note: Doesn't enum raise this for us?! Not as of >=3.11 (new check on empty is done first..)
                raise ValueError(f"{action!r} is not a valid {cls.__qualname__}")

    @dataclass(frozen=True)
    class Scope(ABC):
        _scope_name: ClassVar[str]

        @classmethod
        def load(cls, resource: dict | str, allow_unknown: bool = False) -> Self:
            resource = resource if isinstance(resource, dict) else load_yaml_or_json(resource)
            known_scopes = set(resource).intersection(_SCOPE_CLASS_BY_NAME)
            if len(known_scopes) == 1:
                (name,) = known_scopes
                scope_cls = _SCOPE_CLASS_BY_NAME[name]
                data = convert_all_keys_to_snake_case(resource[name])
                try:
                    return cast(Self, scope_cls(**data))
                except TypeError:
                    not_supported = set(data).difference(inspect.signature(scope_cls).parameters)
                    logger.warning(
                        f"For '{scope_cls.__name__}', the following unknown fields were ignored: {not_supported}. "
                        "Try updating to the latest SDK version, or create an issue on Github!"
                    )
                    return cast(Self, scope_cls(**rename_and_exclude_keys(data, exclude=not_supported)))

            # We infer this as an unknown scope not yet added to the SDK:
            if len(resource) == 1:
                ((name, data),) = resource.items()
                if allow_unknown:
                    return cast(Self, UnknownScope(name=name, data=data))

                raise ValueError(
                    f"Unable to parse Scope, {name!r} is not a known scope. Pass `allow_unknown=True` to force "
                    f"loading it as an unknown scope. List of known: {sorted(_SCOPE_CLASS_BY_NAME)}."
                )
            raise ValueError(
                f"Unable to parse Scope, none of the top-level keys in the input, {sorted(resource)}, "
                f"matched known Scopes, - or - multiple was found. List of known: {sorted(_SCOPE_CLASS_BY_NAME)}."
            )

        def dump(self, camel_case: bool = True) -> dict[str, Any]:
            if isinstance(self, UnknownScope):
                return {self.name: self.data}

            data = asdict(self)
            if camel_case:
                data = convert_all_keys_to_camel_case(data)
            return {self._scope_name: data}

        def as_tuples(
            self,
        ) -> set[tuple[str]] | set[tuple[str, str]] | set[tuple[str, str, str]] | set[tuple[str, int]]:
            # Basic implementation for all simple Scopes (e.g. all or currentuser)
            return {(self._scope_name,)}

    @classmethod
    def from_tuple(cls, tpl: CapabilityTuple) -> Self:
        capability_cls = _CAPABILITY_CLASS_BY_NAME[tpl.acl_name]
        scope_cls = _SCOPE_CLASS_BY_NAME[tpl.scope_name]

        if tpl.scope_id is None:
            scope = scope_cls()
        elif tpl.scope_extra is None:
            scope = scope_cls([tpl.scope_id])  # type: ignore [call-arg]
        elif scope_cls is TableScope:
            scope = scope_cls({tpl.database: [tpl.table] if tpl.table else []})  # type: ignore [call-arg]
        else:
            raise ValueError(f"CapabilityTuple not understood: {tpl}")

        return cast(Self, capability_cls(actions=[capability_cls.Action(tpl.action)], scope=scope, allow_unknown=False))

    @classmethod
    def load(cls, resource: dict | str, allow_unknown: bool = False) -> Self:
        resource = resource if isinstance(resource, dict) else load_yaml_or_json(resource)
        known_acls = set(resource).intersection(_CAPABILITY_CLASS_BY_NAME)
        if len(known_acls) == 1:
            (name,) = known_acls
            capability_cls = _CAPABILITY_CLASS_BY_NAME[name]
            # TODO: We ignore extra keys that might be present like 'projectUrlNames' - are these needed?
            try:
                scopes = Capability.Scope.load(resource[name]["scope"], allow_unknown)
            except Exception as err:
                raise ValueError(f"Could not instantiate {capability_cls.__name__} due to: {err}")

            return cast(
                Self,
                capability_cls(
                    actions=[capability_cls.Action.load(act, allow_unknown) for act in resource[name]["actions"]],
                    scope=scopes,
                    allow_unknown=allow_unknown,
                ),
            )
        # We infer this as an unknown acl not yet added to the SDK. All API-responses are neccessarily
        # loaded with 'allow_unknown=True' (to future-proof):
        if allow_unknown:
            if len(resource) != 1:
                unknown_acl = UnknownAcl(actions=None, scope=None, raw_data=resource, allow_unknown=True)  # type: ignore [arg-type]
            else:
                ((name, data),) = resource.items()
                unknown_acl = UnknownAcl(
                    capability_name=name,
                    actions=[UnknownAcl.Action.load(act, allow_unknown=True) for act in data["actions"]],
                    scope=Capability.Scope.load(data["scope"], allow_unknown=True),
                    raw_data=resource,
                    allow_unknown=True,
                )
            return cast(Self, unknown_acl)

        top_lvl_keys = sorted(resource)
        err_msg = (
            f"Unable to parse Capability, none of the top-level keys in the input, {top_lvl_keys}, "
            f"matched known ACLs, - or - multiple was found. Pass `allow_unknown=True` to force loading it "
            f"as an unknown capability."
        )
        if matches := [match for key in top_lvl_keys for match in difflib.get_close_matches(key, ALL_CAPABILITIES)]:
            err_msg += f" Did you mean one of: {matches}?"
        err_msg += " List of all ACLs: from cognite.client.data_classes.capabilities import ALL_CAPABILITIES"
        raise ValueError(err_msg)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        if isinstance(self, UnknownAcl):
            return self.raw_data
        data = {
            "actions": [action.value for action in self.actions],
            "scope": self.scope.dump(camel_case=camel_case),
        }
        capability_name = self._capability_name
        return {to_camel_case(capability_name) if camel_case else to_snake_case(capability_name): data}

    def as_tuples(self) -> set[CapabilityTuple]:
        return set(
            CapabilityTuple(acl, action, *scope_tpl)  # type: ignore [arg-type]
            for acl, action, scope_tpl in product(
                [self._capability_name], [action.value for action in self.actions], self.scope.as_tuples()
            )
        )


@dataclass
class LegacyCapability(Capability, ABC):
    """This is a base class for capabilities that are no longer in use by the API."""

    ...


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
class AllProjectsScope(ProjectScope): ...


@dataclass(frozen=True)
class ProjectsScope(ProjectScope):
    projects: list[str]


@dataclass
class ProjectCapability(CogniteResource):
    """This represents an capability with additional info about which project(s) it is for."""

    capability: Capability
    project_scope: AllProjectsScope | ProjectsScope

    class Scope:
        All = AllProjectsScope
        Projects = ProjectsScope

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None, allow_unknown: bool = False) -> Self:
        (keys := set(resource)).remove(ProjectScope.name)
        project_scope_dct = {ProjectScope.name: resource[ProjectScope.name]}
        return cls(
            capability=Capability.load({k: resource[k] for k in keys}, allow_unknown),
            project_scope=ProjectScope.load(project_scope_dct),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped = self.capability.dump(camel_case=camel_case)
        dumped.update(self.project_scope.dump(camel_case=camel_case))
        return dumped


class ProjectCapabilityList(CogniteResourceList[ProjectCapability]):
    _RESOURCE = ProjectCapability

    @classmethod
    def _load(
        cls,
        resource_list: Iterable[dict[str, Any]],
        cognite_client: CogniteClient | None = None,
        allow_unknown: bool = False,
    ) -> Self:
        return cls(
            [cls._RESOURCE._load(res, cognite_client, allow_unknown) for res in resource_list],
            cognite_client=cognite_client,
        )

    def _infer_project(self, project: str | None = None) -> str:
        if project is None:
            return self._cognite_client.config.project
        return project

    def as_tuples(self, project: str | None = None) -> set[CapabilityTuple]:
        project = self._infer_project(project)

        output: set[CapabilityTuple] = set()
        for proj_cap in self:
            cap = proj_cap.capability
            if isinstance(cap, UnknownAcl):
                warnings.warn(f"Unknown capability {cap.capability_name!r} will be ignored in comparison")
                continue
            if isinstance(cap, LegacyCapability):
                # Legacy capabilities are no longer in use, so they are safe to skip.
                continue
            if (
                isinstance(proj_cap.project_scope, AllProjectsScope)
                or isinstance(proj_cap.project_scope, ProjectsScope)
                and project in proj_cap.project_scope.projects
            ):
                output |= cap.as_tuples()
        return output


@dataclass(frozen=True)
class AllScope(Capability.Scope):
    _scope_name = "all"


@dataclass(frozen=True)
class CurrentUserScope(Capability.Scope):
    _scope_name = "currentuserscope"


@dataclass(frozen=True)
class IDScope(Capability.Scope):
    _scope_name = "idScope"
    ids: list[int]

    def __post_init__(self) -> None:
        object.__setattr__(self, "ids", [int(i) for i in self.ids])

    def as_tuples(self) -> set[tuple[str, int]]:
        return {(self._scope_name, i) for i in self.ids}


@dataclass(frozen=True)
class IDScopeLowerCase(Capability.Scope):
    """Necessary due to lack of API standardisation on scope name: 'idScope' VS 'idscope'"""

    _scope_name = "idscope"
    ids: list[int]

    def __post_init__(self) -> None:
        object.__setattr__(self, "ids", [int(i) for i in self.ids])

    def as_tuples(self) -> set[tuple[str, int]]:
        return {(self._scope_name, i) for i in self.ids}


@dataclass(frozen=True)
class ExtractionPipelineScope(Capability.Scope):
    _scope_name = "extractionPipelineScope"
    ids: list[int]

    def __post_init__(self) -> None:
        object.__setattr__(self, "ids", [int(i) for i in self.ids])

    def as_tuples(self) -> set[tuple[str, int]]:
        return {(self._scope_name, i) for i in self.ids}


@dataclass(frozen=True)
class DataSetScope(Capability.Scope):
    _scope_name = "datasetScope"
    ids: list[int]

    def __post_init__(self) -> None:
        object.__setattr__(self, "ids", [int(i) for i in self.ids])

    def as_tuples(self) -> set[tuple[str, int]]:
        return {(self._scope_name, i) for i in self.ids}


@dataclass(frozen=True)
class TableScope(Capability.Scope):
    _scope_name = "tableScope"
    dbs_to_tables: dict[str, list[str]]

    def __post_init__(self) -> None:
        if not self.dbs_to_tables:
            return

        loaded = self.dbs_to_tables.copy()
        for db, obj in self.dbs_to_tables.items():
            if isinstance(obj, dict):
                loaded[db] = obj.get("tables", [])
        # TableScope is frozen, so a bit awkward to set attribute:
        object.__setattr__(self, "dbs_to_tables", loaded)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        key = "dbsToTables" if camel_case else "dbs_to_tables"
        return {self._scope_name: {key: {k: {"tables": v} for k, v in self.dbs_to_tables.items()}}}

    def as_tuples(self) -> set[tuple[str, str, str]]:
        # When the scope contains no tables, it means all tables... since database name must be at least 1
        # character, we represent this internally with the empty string:
        return {(self._scope_name, db, tbl) for db, tables in self.dbs_to_tables.items() for tbl in tables or [""]}


@dataclass(frozen=True)
class AssetRootIDScope(Capability.Scope):
    _scope_name = "assetRootIdScope"
    root_ids: list[int]

    def __post_init__(self) -> None:
        object.__setattr__(self, "root_ids", [int(i) for i in self.root_ids])

    def as_tuples(self) -> set[tuple[str, int]]:
        return {(self._scope_name, i) for i in self.root_ids}


@dataclass(frozen=True)
class ExperimentsScope(Capability.Scope):
    _scope_name = "experimentscope"
    experiments: list[str]

    def as_tuples(self) -> set[tuple[str, str]]:
        return {(self._scope_name, s) for s in self.experiments}


@dataclass(frozen=True)
class SpaceIDScope(Capability.Scope):
    _scope_name = "spaceIdScope"
    space_ids: list[str]

    def as_tuples(self) -> set[tuple[str, str]]:
        return {(self._scope_name, s) for s in self.space_ids}


@dataclass(frozen=True)
class PartitionScope(Capability.Scope):
    _scope_name = "partition"
    partition_ids: list[int]

    def __post_init__(self) -> None:
        object.__setattr__(self, "partition_ids", [int(i) for i in self.partition_ids])

    def as_tuples(self) -> set[tuple[str, int]]:
        return {(self._scope_name, i) for i in self.partition_ids}


@dataclass(frozen=True)
class LegacySpaceScope(Capability.Scope):
    _scope_name = "spaceScope"
    external_ids: list[str]

    def as_tuples(self) -> set[tuple[str, str]]:
        return {(self._scope_name, s) for s in self.external_ids}


@dataclass(frozen=True)
class LegacyDataModelScope(Capability.Scope):
    _scope_name = "dataModelScope"
    external_ids: list[str]

    def as_tuples(self) -> set[tuple[str, str]]:
        return {(self._scope_name, s) for s in self.external_ids}


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

    def as_tuples(self) -> NoReturn:
        raise NotImplementedError("Unknown scope cannot be converted to tuples (needed for comparisons)")


_SCOPE_CLASS_BY_NAME: MappingProxyType[str, type[Capability.Scope]] = MappingProxyType(
    {c._scope_name: c for c in Capability.Scope.__subclasses__() if not issubclass(c, UnknownScope)}
)


@dataclass
class UnknownAcl(Capability):
    """
    This class is used for capabilities that are not implemented in this version of the SDK.
    Typically, experimental capabilities or new capabilities that have recently been added to the API.
    """

    capability_name: str = "unknownAcl"
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def show_example_usage(cls) -> Literal[""]:
        return ""


@dataclass
class AnalyticsAcl(Capability):
    _capability_name = "analyticsAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):  # type: ignore [misc]
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

    class Action(Capability.Action):  # type: ignore [misc]
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

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope


@dataclass
class DataSetsAcl(Capability):
    _capability_name = "datasetsAcl"
    actions: Sequence[Action]
    scope: AllScope | IDScope

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"
        Owner = "OWNER"

    class Scope:
        All = AllScope
        ID = IDScope


@dataclass
class DigitalTwinAcl(Capability):
    _capability_name = "digitalTwinAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class EntityMatchingAcl(Capability):
    _capability_name = "entitymatchingAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class EventsAcl(Capability):
    _capability_name = "eventsAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope

    class Action(Capability.Action):  # type: ignore [misc]
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

    class Action(Capability.Action):  # type: ignore [misc]
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

    class Action(Capability.Action):  # type: ignore [misc]
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

    class Action(Capability.Action):  # type: ignore [misc]
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

    class Action(Capability.Action):  # type: ignore [misc]
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

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class GeospatialAcl(Capability):
    _capability_name = "geospatialAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class GeospatialCrsAcl(Capability):
    _capability_name = "geospatialCrsAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class GroupsAcl(Capability):
    _capability_name = "groupsAcl"
    actions: Sequence[Action]
    scope: AllScope | CurrentUserScope

    class Action(Capability.Action):  # type: ignore [misc]
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

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope


@dataclass
class LocationFiltersAcl(Capability):
    _capability_name = "locationFiltersAcl"
    actions: Sequence[Action]
    scope: AllScope | IDScope

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        ID = IDScope


@dataclass
class ProjectsAcl(Capability):
    _capability_name = "projectsAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Create = "CREATE"
        List = "LIST"
        Update = "UPDATE"
        Delete = "DELETE"

    class Scope:
        All = AllScope


@dataclass
class RawAcl(Capability):
    _capability_name = "rawAcl"
    actions: Sequence[Action]
    scope: AllScope | TableScope

    class Action(Capability.Action):  # type: ignore [misc]
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

    class Action(Capability.Action):  # type: ignore [misc]
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

    class Action(Capability.Action):  # type: ignore [misc]
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
    scope: AllScope | IDScopeLowerCase

    class Action(Capability.Action):  # type: ignore [misc]
        MemberOf = "MEMBEROF"
        List = "LIST"
        Create = "CREATE"
        Update = "UPDATE"
        Delete = "DELETE"

    class Scope:
        All = AllScope
        ID = IDScopeLowerCase


@dataclass
class SeismicAcl(Capability):
    _capability_name = "seismicAcl"
    actions: Sequence[Action]
    scope: AllScope | PartitionScope

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        Partition = PartitionScope


@dataclass
class SequencesAcl(Capability):
    _capability_name = "sequencesAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope

    class Action(Capability.Action):  # type: ignore [misc]
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

    class Action(Capability.Action):  # type: ignore [misc]
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

    class Action(Capability.Action):  # type: ignore [misc]
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
    scope: AllScope | DataSetScope | IDScopeLowerCase | AssetRootIDScope

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope
        ID = IDScopeLowerCase
        AssetRootID = AssetRootIDScope


@dataclass
class TimeSeriesSubscriptionsAcl(Capability):
    _capability_name = "timeSeriesSubscriptionsAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope = field(default_factory=AllScope)

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope


@dataclass
class TransformationsAcl(Capability):
    _capability_name = "transformationsAcl"
    actions: Sequence[Action]
    scope: AllScope | DataSetScope

    class Action(Capability.Action):  # type: ignore [misc]
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

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class WellsAcl(Capability):
    _capability_name = "wellsAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class ExperimentsAcl(Capability):
    _capability_name = "experimentAcl"
    actions: Sequence[Action]
    scope: ExperimentsScope

    class Action(Capability.Action):  # type: ignore [misc]
        Use = "USE"

    class Scope:
        Experiments = ExperimentsScope

    @classmethod
    def show_example_usage(cls) -> str:
        return (
            "Example usage: ExperimentsAcl(actions=[ExperimentsAcl.Action.Use], "
            "scope=ExperimentsAcl.Scope.Experiments(['some-experimental-api']))"
        )


@dataclass
class TemplateGroupsAcl(Capability):
    _capability_name = "templateGroupsAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope


@dataclass
class TemplateInstancesAcl(Capability):
    _capability_name = "templateInstancesAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        DataSet = DataSetScope


@dataclass
class DataModelInstancesAcl(Capability):
    _capability_name = "dataModelInstancesAcl"
    actions: Sequence[Action]
    scope: AllScope | SpaceIDScope

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"
        Write_Properties = "WRITE_PROPERTIES"

    class Scope:
        All = AllScope
        SpaceID = SpaceIDScope
        LegacySpace = LegacySpaceScope


@dataclass
class DataModelsAcl(Capability):
    _capability_name = "dataModelsAcl"
    actions: Sequence[Action]
    scope: AllScope | SpaceIDScope

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        SpaceID = SpaceIDScope
        LegacyDataModel = LegacyDataModelScope


@dataclass
class PipelinesAcl(Capability):
    _capability_name = "pipelinesAcl"
    actions: Sequence[Action]
    scope: AllScope

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class DocumentPipelinesAcl(Capability):
    _capability_name = "documentPipelinesAcl"
    actions: Sequence[Action]
    scope: AllScope

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class FilePipelinesAcl(Capability):
    _capability_name = "filePipelinesAcl"
    actions: Sequence[Action]
    scope: AllScope

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class NotificationsAcl(Capability):
    _capability_name = "notificationsAcl"
    actions: Sequence[Action]
    scope: AllScope

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class ScheduledCalculationsAcl(Capability):
    _capability_name = "scheduledCalculationsAcl"
    actions: Sequence[Action]
    scope: AllScope

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class MonitoringTasksAcl(Capability):
    _capability_name = "monitoringTasksAcl"
    actions: Sequence[Action]
    scope: AllScope

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class HostedExtractorsAcl(Capability):
    _capability_name = "hostedExtractorsAcl"
    actions: Sequence[Action]
    scope: AllScope

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class VisionModelAcl(Capability):
    _capability_name = "visionModelAcl"
    actions: Sequence[Action]
    scope: AllScope

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class DocumentFeedbackAcl(Capability):
    _capability_name = "documentFeedbackAcl"
    actions: Sequence[Action]
    scope: AllScope

    class Action(Capability.Action):  # type: ignore [misc]
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

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class PostgresGatewayAcl(Capability):
    _capability_name = "postgresGatewayAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class UserProfilesAcl(Capability):
    _capability_name = "userProfilesAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"

    class Scope:
        All = AllScope


@dataclass
class AuditlogAcl(Capability):
    _capability_name = "auditlogAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"

    class Scope:
        All = AllScope


@dataclass
class LegacyModelHostingAcl(LegacyCapability):
    _capability_name = "modelHostingAcl"
    actions: Sequence[Action]
    scope: AllScope = field(default_factory=AllScope)

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


@dataclass
class LegacyGenericsAcl(LegacyCapability):
    _capability_name = "genericsAcl"
    actions: Sequence[Action]
    scope: AllScope

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope


_CAPABILITY_CLASS_BY_NAME: MappingProxyType[str, type[Capability]] = MappingProxyType(
    {
        c._capability_name: c
        for c in itertools.chain(Capability.__subclasses__(), LegacyCapability.__subclasses__())
        if c not in (UnknownAcl, LegacyCapability)
    }
)
ALL_CAPABILITIES = sorted(_CAPABILITY_CLASS_BY_NAME)

# Give all Actions a better error message (instead of implementing __missing__ for all):
for acl in _CAPABILITY_CLASS_BY_NAME.values():
    if acl.Action.__members__:
        _cls = type(next(iter(acl.Action.__members__.values())))
        _cls.__name__ = f"{acl.__name__} {_cls.__name__}"

# Add lookup that knows which acls support which scopes (+ attribute lookup name):
_VALID_SCOPES_BY_CAPABILITY: MappingProxyType[
    type[Capability], tuple[tuple[type[Capability.Scope], ...], tuple[str, ...]]
] = MappingProxyType(
    {
        acl: tuple(zip(*[(v, k) for k, v in vars(acl.Scope).items() if inspect.isclass(v)]))
        for acl in _CAPABILITY_CLASS_BY_NAME.values()
    }
)
