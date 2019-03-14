import json


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


class CogniteCollectionResponse(CogniteResponse):
    """Cognite Collection Response class

    All collection responses inherit from this class. Collection responses are subscriptable and iterable.
    """

    _RESPONSE_CLASS = None

    def to_json(self):
        """Returns data as a json object"""
        return self.internal_representation["data"]["items"]

    def __getitem__(self, index):
        if isinstance(index, slice):
            return self.__class__({"data": {"items": self.to_json()[index]}})
        return self._RESPONSE_CLASS({"data": {"items": [self.to_json()[index]]}})

    def __len__(self):
        return len(self.to_json())

    def __iter__(self):
        self.counter = 0
        return self

    def __next__(self):
        if self.counter > len(self.to_json()) - 1:
            raise StopIteration
        else:
            self.counter += 1
            return self._RESPONSE_CLASS({"data": {"items": [self.to_json()[self.counter - 1]]}})


class CogniteResource:
    @staticmethod
    def _to_camel_case(snake_case_string: str):
        components = snake_case_string.split("_")
        return components[0] + "".join(x.title() for x in components[1:])

    def camel_case_dict(self):
        new_d = {}
        for key in self.__dict__:
            new_d[self._to_camel_case(key)] = self.__dict__[key]
        return new_d

    def to_json(self):
        return json.loads(json.dumps(self, default=lambda x: x.__dict__))

    def __eq__(self, other):
        return type(self) == type(other) and self.to_json() == other.to_json()

    def __str__(self):
        return json.dumps(self.__dict__, default=lambda x: x.__dict__, indent=4, sort_keys=True)
