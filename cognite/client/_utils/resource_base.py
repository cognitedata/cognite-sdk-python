import json
from typing import Any, Dict, List, Union

from cognite.client._utils.utils import to_camel_case, to_snake_case


class CogniteResponse:
    def __str__(self):
        return json.dumps(self.dump(), indent=4, sort_keys=True)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return type(other) == type(self) and other.dump() == self.dump()

    def dump(self, camel_case: bool = False):
        dumped = {key: value for key, value in self.__dict__.items() if value is not None}
        if camel_case:
            dumped = {to_camel_case(key): value for key, value in dumped.items()}
        return dumped

    @classmethod
    def _load(cls, api_response):
        raise NotImplementedError

    def to_pandas(self):
        raise NotImplementedError


class CogniteResourceList:
    _RESOURCE = None

    def __init__(self, resources: List[Any]):
        for resource in resources:
            if not isinstance(resource, self._RESOURCE):
                raise TypeError("All resources must be of type {}".format(self._RESOURCE.__name__))
        self._resources = resources

    def __getitem__(self, index):
        return self._resources[index]

    def __eq__(self, other):
        if not type(self) == type(other):
            return False
        if not len(self) == len(other):
            return False
        for i in range(len(self)):
            if self[i] != other[i]:
                return False
        return True

    def __len__(self):
        return len(self._resources)

    def __iter__(self):
        for resource in self._resources:
            yield resource

    def __str__(self):
        return json.dumps(self.dump(), default=lambda x: x.__dict__, indent=4, sort_keys=True)

    def __repr__(self):
        return self.__str__()

    def dump(self, camel_case: bool = False):
        return [resource.dump(camel_case) for resource in self._resources]

    def to_pandas(self):
        raise NotImplementedError

    @classmethod
    def _load(cls, resource_list: Union[List, str]):
        if isinstance(resource_list, str):
            return cls._load(json.loads(resource_list))
        elif isinstance(resource_list, List):
            resources = [cls._RESOURCE._load(resource) for resource in resource_list]
            return cls(resources)


class CogniteResource:
    def __eq__(self, other):
        return type(self) == type(other) and self.dump() == other.dump()

    def __str__(self):
        return json.dumps(self.dump(), default=lambda x: x.__dict__, indent=4, sort_keys=True)

    def __repr__(self):
        return self.__str__()

    def dump(self, camel_case: bool = False):
        dumped = {key: value for key, value in self.__dict__.items() if value is not None}
        if camel_case:
            dumped = {to_camel_case(key): value for key, value in dumped.items()}
        return dumped

    @classmethod
    def _load(cls, resource: Union[Dict, str]):
        if isinstance(resource, str):
            return cls._load(json.loads(resource))
        elif isinstance(resource, Dict):
            instance = cls()
            for key, value in resource.items():
                snake_case_key = to_snake_case(key)
                if not hasattr(instance, snake_case_key):
                    raise AttributeError("Attribute '{}' does not exist on '{}'".format(snake_case_key, cls.__name__))
                setattr(instance, snake_case_key, value)
            return instance
        raise TypeError("Resource must be json str or Dict, not {}".format(type(resource)))

    def to_pandas(self):
        raise NotImplementedError


class CogniteUpdate:
    id = None
    external_id = None
    _update_object = None

    def __eq__(self, other):
        return type(self) == type(other) and self.dump() == other.dump()

    def __str__(self):
        return json.dumps(self.dump(), indent=4, sort_keys=True)

    def __repr__(self):
        return self.__str__()

    def dump(self):
        dumped = {"update": self._update_object}
        if self.external_id is not None:
            dumped["externalId"] = self.external_id
        if self.id is not None:
            dumped["id"] = self.id
        return dumped


class CogniteFilter:
    def __eq__(self, other):
        return type(self) == type(other) and self.dump() == other.dump()

    def __str__(self):
        return json.dumps(self.dump(), default=lambda x: x.__dict__, indent=4, sort_keys=True)

    def __repr__(self):
        return self.__str__()

    def dump(self, camel_case: bool = False):
        dumped = {key: value for key, value in self.__dict__.items() if value is not None}
        if camel_case:
            dumped = {to_camel_case(key): value for key, value in dumped.items()}
        return dumped
