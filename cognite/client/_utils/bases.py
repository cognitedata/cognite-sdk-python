import json
import re
from typing import Any, Dict, List, Union


class CogniteResponse:
    """Cognite Response class

    All responses inherit from this class.

    Examples:
        All responses are pretty-printable::

            from cognite.client import CogniteClient

            client = CogniteClient()
            res = client.assets.get_assets(limit=1)

            print(res)

        All endpoints which support paging have an ``autopaging`` flag which may be set to true in order to sequentially
        fetch all resources. If for some reason, you want to do this manually, you may use the next_cursor() method on
        the response object. Here is an example of that::

            from cognite.client import CogniteClient

            client = CogniteClient()

            asset_list = []

            cursor = None
            while True:
                res = client.assets.get_assets(cursor=cursor)
                asset_list.extend(res.to_json())
                cursor = res.next_cursor()
                if cursor is None:
                    break

            print(asset_list)
    """

    def __init__(self, internal_representation):
        self.internal_representation = internal_representation

    def __str__(self):
        return json.dumps(self.to_json(), indent=4, sort_keys=True)

    def to_json(self):
        """Returns data as a json object"""
        return self.internal_representation["data"]["items"][0]

    def next_cursor(self):
        """Returns next cursor to use for paging through results. Returns ``None`` if there are no more results."""
        if self.internal_representation.get("data"):
            return self.internal_representation.get("data").get("nextCursor")

    def previous_cursor(self):
        """Returns previous cursor to use for paging through results. Returns ``None`` if there are no more results."""
        if self.internal_representation.get("data"):
            return self.internal_representation.get("data").get("previousCursor")


class CogniteResourceList:
    """Cognite Collection Response class

    All collection responses inherit from this class. Collection responses are subscriptable and iterable.
    """

    _RESOURCE = None

    def __init__(self, resources: List[Any]):
        for resource in resources:
            if not isinstance(resource, self._RESOURCE):
                raise TypeError("All resources must be of type {}".format(self._RESOURCE.__name__))
        self._resources = resources

    def dump(self):
        return [resource.dump() for resource in self._resources]

    @classmethod
    def _load(cls, resource_list: Union[List, str]):
        if isinstance(resource_list, str):
            return cls._load(json.loads(resource_list))
        elif isinstance(resource_list, List):
            resources = [cls._RESOURCE._load(resource) for resource in resource_list]
            return cls(resources)

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


class CogniteResource:
    @staticmethod
    def _to_camel_case(snake_case_string: str):
        components = snake_case_string.split("_")
        return components[0] + "".join(x.title() for x in components[1:])

    @staticmethod
    def _to_snake_case(camel_case_string: str):
        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", camel_case_string)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

    def dump(self, camel_case: bool = False):
        dumped = {key: value for key, value in self.__dict__.items() if value is not None}
        if camel_case:
            dumped = {self._to_camel_case(key): value for key, value in dumped.items()}
        return dumped

    @classmethod
    def _load(cls, resource: [Dict, str]):
        if isinstance(resource, str):
            return cls._load(json.loads(resource))
        elif isinstance(resource, Dict):
            instance = cls()
            for key, value in resource.items():
                snake_case_key = cls._to_snake_case(key)
                if not hasattr(instance, snake_case_key):
                    raise AttributeError(
                        "Attribute '{}' does not exist on '{}'".format(snake_case_key, instance.__class__.__name__)
                    )
                setattr(instance, snake_case_key, value)
            return instance
        raise TypeError("Resource must be json str or Dict")

    def to_pandas(self):
        raise NotImplementedError

    def __eq__(self, other):
        return type(self) == type(other) and self.dump() == other.dump()

    def __str__(self):
        return json.dumps(self.dump(), default=lambda x: x.__dict__, indent=4, sort_keys=True)

    def __repr__(self):
        return self.__str__()
