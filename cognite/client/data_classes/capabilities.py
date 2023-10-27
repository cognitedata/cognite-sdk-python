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
    Suggest = enum.auto()
    Review = enum.auto()
    Execute = enum.auto()


@dataclass(frozen=True)
class Scope(ABC):
    @classmethod
    def load(cls, resource: dict | str) -> Self | dict[str, Any]:
        resource = resource if isinstance(resource, dict) else json.loads(resource)
        (name,) = resource.keys()
        if name.casefold() not in _SCOPE_CLASS_BY_NAME:
            # The scope is not implemented in this version of the SDK, just
            # return the raw resource
            return resource
        scope_cls = _SCOPE_CLASS_BY_NAME[name.casefold()]
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
    ...


@dataclass(frozen=True)
class DataSetScope(Scope):
    data_set_ids: list[int]


@dataclass(frozen=True)
class IDScope(Scope):
    ids: list[int]


_SCOPE_CLASS_BY_NAME: dict[str, type[Scope]] = {c.__name__.casefold(): c for c in Scope.__subclasses__()}


@dataclass
class Capability(ABC):
    _valid_scopes: ClassVar[frozenset[type[Scope]]]
    _valid_actions: ClassVar[frozenset[Action]]
    actions: list[Action | str]
    scope: Scope | dict[str, Any]

    @property
    def available_actions(self) -> frozenset[Action]:
        return self._valid_actions

    @property
    def available_scopes(self) -> frozenset[type[Scope]]:
        return self._valid_scopes

    @classmethod
    def load(cls, resource: dict | str) -> Self | dict[str, Any]:
        resource = resource if isinstance(resource, dict) else json.loads(resource)
        (name,) = resource.keys()
        if name.casefold() not in _CAPABILITY_CLASS_BY_NAME:
            # The capability is not implemented in this version of the SDK, just
            # return the raw resource
            return resource
        capability_cls = _CAPABILITY_CLASS_BY_NAME[name.casefold()]
        data = resource[name]
        return cast(
            Self,
            capability_cls(actions=[Action[action] for action in data["actions"]], scope=Scope.load(data["scope"])),
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        data = {
            "actions": [action.name if isinstance(action, Action) else action for action in self.actions],
            "scope": self.scope.dump(camel_case=camel_case) if isinstance(self.scope, Scope) else self.scope,
        }
        class_name = type(self).__name__
        return {to_camel_case(class_name) if camel_case else to_snake_case(class_name): data}


@dataclass
class AnalyticsAcl(Capability):
    _valid_scopes = frozenset({AllScope})
    _valid_actions = frozenset({Action.Read, Action.Execute, Action.List})
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class AnnotationsAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write, Action.Suggest, Action.Review})
    _valid_scopes = frozenset({AllScope})
    scope: AllScope = field(default_factory=AllScope)


@dataclass
class AssetsAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write})
    _valid_scopes = frozenset({AllScope, DataSetScope})


@dataclass
class DataSetsAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write, Action.Owner})
    _valid_scopes = frozenset({AllScope, DataSetScope})
    scope: AllScope | DataSetScope


@dataclass
class DigitalTwinAcl(Capability):
    _valid_actions = frozenset({Action.Read, Action.Write})
    _valid_scopes = frozenset({AllScope})
    scope: AllScope = field(default_factory=AllScope)


_CAPABILITY_CLASS_BY_NAME: dict[str, type[Capability]] = {c.__name__.casefold(): c for c in Capability.__subclasses__()}
