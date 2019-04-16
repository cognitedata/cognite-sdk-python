import json
from collections import UserList
from typing import *

from cognite.client._utils import utils
from cognite.client._utils.utils import global_client, to_camel_case, to_snake_case


def instantiate_with_global_client(cls, super, *args, **kwargs):
    obj = super.__new__(cls)
    obj._client = global_client.get()
    if "cognite_client" in kwargs:
        obj._client = kwargs["cognite_client"]
    return obj


EXCLUDE_VALUE = [None]
EXCLUDE_KEY = ["_client"]


class CogniteResponse:
    def __new__(cls, *args, **kwargs):
        return instantiate_with_global_client(cls, super(), *args, **kwargs)

    def __str__(self):
        return json.dumps(self.dump(), indent=4)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return type(other) == type(self) and other.dump() == self.dump()

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        dumped = {
            key: value for key, value in self.__dict__.items() if value not in EXCLUDE_VALUE and key not in EXCLUDE_KEY
        }
        if camel_case:
            dumped = {to_camel_case(key): value for key, value in dumped.items()}
        return dumped

    @classmethod
    def _load(cls, api_response):
        raise NotImplementedError

    def to_pandas(self):
        raise NotImplementedError


class CogniteResourceList(UserList):
    _RESOURCE = None
    _UPDATE = None
    _ASSERT_CLASSES = True

    def __new__(cls, *args, **kwargs):
        return instantiate_with_global_client(cls, super(), *args, **kwargs)

    def __init__(self, resources: List[Any], **kwargs):
        if self._ASSERT_CLASSES:
            assert self._RESOURCE is not None, "{} does not have _RESOURCE set".format(self.__class__.__name__)
            assert self._UPDATE is not None, "{} does not have _UPDATE set".format(self.__class__.__name__)
        for resource in resources:
            if not isinstance(resource, self._RESOURCE):
                raise TypeError(
                    "All resources for class '{}' must be of type '{}', not '{}'.".format(
                        self.__class__.__name__, self._RESOURCE.__name__, type(resource)
                    )
                )
        super().__init__(resources)

    def __str__(self):
        return json.dumps(self.dump(), default=lambda x: x.__dict__, indent=4)

    def __repr__(self):
        return self.__str__()

    def dump(self, camel_case: bool = False) -> List[Dict[str, Any]]:
        return [resource.dump(camel_case) for resource in self.data]

    def to_pandas(self):
        pd = utils.local_import("pandas")
        return pd.DataFrame(self.dump(camel_case=True))

    @classmethod
    def _load(cls, resource_list: Union[List, str], cognite_client=None):
        if isinstance(resource_list, str):
            return cls._load(json.loads(resource_list), cognite_client=cognite_client)
        elif isinstance(resource_list, List):
            resources = [cls._RESOURCE._load(resource, cognite_client=cognite_client) for resource in resource_list]
            return cls(resources, cognite_client=cognite_client)


class CogniteResource:
    def __new__(cls, *args, **kwargs):
        return instantiate_with_global_client(cls, super(), *args, **kwargs)

    def __eq__(self, other):
        return type(self) == type(other) and self.dump() == other.dump()

    def __str__(self):
        return json.dumps(self.dump(), default=lambda x: x.__dict__, indent=4)

    def __repr__(self):
        return self.__str__()

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        dumped = {
            key: value for key, value in self.__dict__.items() if value not in EXCLUDE_VALUE and key not in EXCLUDE_KEY
        }
        if camel_case:
            dumped = {to_camel_case(key): value for key, value in dumped.items()}
        return dumped

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        if isinstance(resource, str):
            return cls._load(json.loads(resource), cognite_client=cognite_client)
        elif isinstance(resource, Dict):
            instance = cls(cognite_client=cognite_client)
            for key, value in resource.items():
                snake_case_key = to_snake_case(key)
                if not hasattr(instance, snake_case_key):
                    raise AttributeError("Attribute '{}' does not exist on '{}'".format(snake_case_key, cls.__name__))
                setattr(instance, snake_case_key, value)
            return instance
        raise TypeError("Resource must be json str or Dict, not {}".format(type(resource)))

    def to_pandas(self, expand: List[str] = None, ignore: List[str] = None):
        expand = ["metadata"] if expand is None else expand
        ignore = [] if ignore is None else ignore
        pd = utils.local_import("pandas")
        dumped = self.dump(camel_case=True)

        for element in ignore:
            del dumped[element]
        for key in expand:
            if key in dumped and isinstance(dumped[key], dict):
                dumped.update(dumped.pop(key))
            else:
                raise AssertionError("Could not expand attribute '{}'".format(key))

        df = pd.DataFrame(columns=["value"])
        for name, value in dumped.items():
            df.loc[name] = [value]
        return df


class CogniteUpdate:
    def __init__(self, id: int = None, external_id: str = None):
        self._id = id
        self._external_id = external_id
        self._update_object = {}

    def __eq__(self, other):
        return type(self) == type(other) and self.dump() == other.dump()

    def __str__(self):
        return json.dumps(self.dump(), indent=4)

    def __repr__(self):
        return self.__str__()

    def _set(self, name, value):
        self._update_object[name] = {"set": value}

    def _set_null(self, name):
        self._update_object[name] = {"setNull": True}

    def _add(self, name, value):
        self._update_object[name] = {"add": value}

    def _remove(self, name, value):
        self._update_object[name] = {"remove": value}

    def dump(self):
        dumped = {"update": self._update_object}
        if self._id is not None:
            dumped["id"] = self._id
        elif self._external_id is not None:
            dumped["externalId"] = self._external_id
        return dumped

    @classmethod
    def _get_update_properties(cls):
        return [key for key in cls.__dict__.keys() if not key.startswith("_")]


class CognitePrimitiveUpdate:
    def __init__(self, update_object, name: str):
        self._update_object = update_object
        self._name = name

    def _set(self, value: Union[None, str, int, bool]):
        if value is None:
            self._update_object._set_null(self._name)
        else:
            self._update_object._set(self._name, value)
        return self._update_object


class CogniteObjectUpdate:
    def __init__(self, update_object, name: str):
        self._update_object = update_object
        self._name = name

    def _set(self, value: Dict):
        self._update_object._set(self._name, value)
        return self._update_object

    def _add(self, value: Dict):
        self._update_object._add(self._name, value)
        return self._update_object

    def _remove(self, value: List):
        self._update_object._remove(self._name, value)
        return self._update_object


class CogniteListUpdate:
    def __init__(self, update_object, name: str):
        self._update_object = update_object
        self._name = name

    def _set(self, value: List):
        self._update_object._set(self._name, value)
        return self._update_object

    def _add(self, value: List):
        self._update_object._add(self._name, value)
        return self._update_object

    def _remove(self, value: List):
        self._update_object._remove(self._name, value)
        return self._update_object


class CogniteFilter:
    def __eq__(self, other):
        return type(self) == type(other) and self.dump() == other.dump()

    def __str__(self):
        return json.dumps(self.dump(), default=lambda x: x.__dict__, indent=4)

    def __repr__(self):
        return self.__str__()

    def dump(self, camel_case: bool = False):
        dumped = {key: value for key, value in self.__dict__.items() if value is not None}
        if camel_case:
            dumped = {to_camel_case(key): value for key, value in dumped.items()}
        return dumped
