import json
from collections import UserList
from typing import *

from cognite.client import utils
from cognite.client.exceptions import CogniteMissingClientError

EXCLUDE_VALUE = [None]


class CogniteResponse:
    def __str__(self):
        item = utils._time.convert_time_attributes_to_datetime(self.dump())
        return json.dumps(item, default=utils._auxiliary.json_dump_default, indent=4)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return type(other) == type(self) and other.dump() == self.dump()

    def __getattribute__(self, item):
        attr = super().__getattribute__(item)
        if item == "_cognite_client":
            if attr is None:
                raise CogniteMissingClientError
        return attr

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        """Dump the instance into a json serializable python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            Dict[str, Any]: A dictionary representation of the instance.
        """
        dumped = {
            key: value for key, value in self.__dict__.items() if value not in EXCLUDE_VALUE and not key.startswith("_")
        }
        if camel_case:
            dumped = {utils._auxiliary.to_camel_case(key): value for key, value in dumped.items()}
        return dumped

    @classmethod
    def _load(cls, api_response):
        raise NotImplementedError

    def to_pandas(self):
        raise NotImplementedError


class CogniteResource:
    def __new__(cls, *args, **kwargs):
        obj = super().__new__(cls)
        obj._cognite_client = None
        if "cognite_client" in kwargs:
            obj._cognite_client = kwargs["cognite_client"]
        return obj

    def __eq__(self, other):
        return type(self) == type(other) and self.dump() == other.dump()

    def __str__(self):
        item = utils._time.convert_time_attributes_to_datetime(self.dump())
        return json.dumps(item, default=utils._auxiliary.json_dump_default, indent=4)

    def __getattribute__(self, item):
        attr = super().__getattribute__(item)
        if item == "_cognite_client":
            if attr is None:
                raise CogniteMissingClientError
        return attr

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            Dict[str, Any]: A dictionary representation of the instance.
        """
        if camel_case:
            return {
                utils._auxiliary.to_camel_case(key): value
                for key, value in self.__dict__.items()
                if value not in EXCLUDE_VALUE and not key.startswith("_")
            }
        return {
            key: value for key, value in self.__dict__.items() if value not in EXCLUDE_VALUE and not key.startswith("_")
        }

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        if isinstance(resource, str):
            return cls._load(json.loads(resource), cognite_client=cognite_client)
        elif isinstance(resource, Dict):
            instance = cls(cognite_client=cognite_client)
            for key, value in resource.items():
                snake_case_key = utils._auxiliary.to_snake_case(key)
                if hasattr(instance, snake_case_key):
                    setattr(instance, snake_case_key, value)
            return instance
        raise TypeError("Resource must be json str or Dict, not {}".format(type(resource)))

    def to_pandas(self, expand: List[str] = ("metadata",), ignore: List[str] = None, camel_case: bool = True):
        """Convert the instance into a pandas DataFrame.

        Args:
            expand (List[str]): List of row keys to expand, only works if the value is a Dict.
                Will expand metadata by default.
            ignore (List[str]): List of row keys to not include when converting to a data frame.
            camel_case (bool): Convert column names to camel case (e.g. `externalId` instead of `external_id`)

        Returns:
            pandas.DataFrame: The dataframe.
        """
        ignore = [] if ignore is None else ignore
        pd = utils._auxiliary.local_import("pandas")
        dumped = self.dump(camel_case=camel_case)

        for element in ignore:
            del dumped[element]
        for key in expand:
            if key in dumped:
                if isinstance(dumped[key], dict):
                    dumped.update(dumped.pop(key))
                else:
                    raise AssertionError("Could not expand attribute '{}'".format(key))

        df = pd.DataFrame(columns=["value"])
        for name, value in dumped.items():
            df.loc[name] = [value]
        return df

    def _repr_html_(self):
        return self.to_pandas(camel_case=False)._repr_html_()


class CognitePropertyClassUtil:
    @staticmethod
    def declare_property(schema_name):
        return (
            property(lambda s: s[schema_name] if schema_name in s else None)
            .setter(lambda s, v: CognitePropertyClassUtil._property_setter(s, schema_name, v))
            .deleter(lambda s: s.pop(schema_name, None))
        )

    @staticmethod
    def _property_setter(self, schema_name, value):
        if value is None:
            self.pop(schema_name, None)
        else:
            self[schema_name] = value


class CogniteResourceList(UserList):
    _RESOURCE = None
    _UPDATE = None
    _ASSERT_CLASSES = True

    def __init__(self, resources: List[Any], cognite_client=None):
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
        self._cognite_client = cognite_client
        super().__init__(resources)
        if self.data:
            if hasattr(self.data[0], "external_id"):
                self._external_id_to_item = {
                    item.external_id: item for item in self.data if item.external_id is not None
                }
            if hasattr(self.data[0], "id"):
                self._id_to_item = {item.id: item for item in self.data if item.id is not None}

    def __getattribute__(self, item):
        attr = super().__getattribute__(item)
        if item == "_cognite_client" and attr is None:
            raise CogniteMissingClientError
        return attr

    def __getitem__(self, item):
        value = super().__getitem__(item)
        if isinstance(item, slice):
            c = None
            if super().__getattribute__("_cognite_client") is not None:
                c = self._cognite_client
            return self.__class__(value, cognite_client=c)
        return value

    def __str__(self):
        item = utils._time.convert_time_attributes_to_datetime(self.dump())
        return json.dumps(item, default=utils._auxiliary.json_dump_default, indent=4)

    def dump(self, camel_case: bool = False) -> List[Dict[str, Any]]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            List[Dict[str, Any]]: A list of dicts representing the instance.
        """
        return [resource.dump(camel_case) for resource in self.data]

    def get(self, id: int = None, external_id: str = None) -> Optional[CogniteResource]:
        """Get an item from this list by id or exernal_id.

        Args:
            id (int): The id of the item to get.
            external_id (str): The external_id of the item to get.

        Returns:
            Optional[CogniteResource]: The requested item
        """
        utils._auxiliary.assert_exactly_one_of_id_or_external_id(id, external_id)
        if id:
            return self._id_to_item.get(id)
        return self._external_id_to_item.get(external_id)

    def to_pandas(self, camel_case=True) -> "pandas.DataFrame":
        """Convert the instance into a pandas DataFrame.

        Returns:
            pandas.DataFrame: The dataframe.
        """
        pd = utils._auxiliary.local_import("pandas")
        df = pd.DataFrame(self.dump(camel_case=camel_case))
        nullable_int_fields = ["startTime", "endTime", "assetId", "parentId", "dataSetId"]
        if not camel_case:
            nullable_int_fields = [utils._auxiliary.to_snake_case(f) for f in nullable_int_fields]
        try:
            for field in nullable_int_fields:
                if field in df:
                    df[field] = df[field].astype("Int64")
        except ValueError:
            pass
        return df

    def _repr_html_(self):
        return self.to_pandas(camel_case=False)._repr_html_()

    @classmethod
    def _load(cls, resource_list: Union[List, str], cognite_client=None):
        if isinstance(resource_list, str):
            return cls._load(json.loads(resource_list), cognite_client=cognite_client)
        elif isinstance(resource_list, List):
            resources = [cls._RESOURCE._load(resource, cognite_client=cognite_client) for resource in resource_list]
            return cls(resources, cognite_client=cognite_client)


class CogniteUpdate:
    def __init__(self, id: int = None, external_id: str = None):
        self._id = id
        self._external_id = external_id
        self._update_object = {}

    def __eq__(self, other):
        return type(self) == type(other) and self.dump() == other.dump()

    def __str__(self):
        return json.dumps(self.dump(), default=utils._auxiliary.json_dump_default, indent=4)

    def __repr__(self):
        return self.__str__()

    def _set(self, name, value):
        update_obj = self._update_object.get(name, {})
        assert (
            "remove" not in update_obj and "add" not in update_obj
        ), "Can not call set after adding or removing fields from an update object."
        self._update_object[name] = {"set": value}

    def _set_null(self, name):
        self._update_object[name] = {"setNull": True}

    def _add(self, name, value):
        update_obj = self._update_object.get(name, {})
        assert "set" not in update_obj, "Can not call remove or add fields after calling set on an update object."
        assert (
            "add" not in update_obj
        ), "Can not call add twice on the same object, please combine your objects and pass them to add in one call."
        update_obj["add"] = value
        self._update_object[name] = update_obj

    def _remove(self, name, value):
        update_obj = self._update_object.get(name, {})
        assert "set" not in update_obj, "Can not call remove or add fields after calling set on an update object."
        assert (
            "remove" not in update_obj
        ), "Can not call remove twice on the same object, please combine your items and pass them to remove in one call."
        update_obj["remove"] = value
        self._update_object[name] = update_obj

    def dump(self):
        """Dump the instance into a json serializable Python data type.

        Returns:
            Dict[str, Any]: A dictionary representation of the instance.
        """
        dumped = {"update": self._update_object}
        if self._id is not None:
            dumped["id"] = self._id
        elif self._external_id is not None:
            dumped["externalId"] = self._external_id
        return dumped

    @classmethod
    def _get_update_properties(cls):
        return [key for key in cls.__dict__.keys() if (not key.startswith("_")) and (not key == "labels")]


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


class CogniteLabelUpdate:
    def __init__(self, update_object, name: str):
        self._update_object = update_object
        self._name = name

    def _add(self, external_ids: Union[str, List[str]]):
        self._update_object._add(self._name, self._wrap_ids(self._wrap_in_list(external_ids)))
        return self._update_object

    def _remove(self, external_ids: Union[str, List[str]]):
        self._update_object._remove(self._name, self._wrap_ids(self._wrap_in_list(external_ids)))
        return self._update_object

    def _wrap_ids(self, external_ids: List[str]):
        return [{"externalId": external_id} for external_id in external_ids]

    def _wrap_in_list(self, external_ids: Union[str, List[str]]):
        return external_ids if isinstance(external_ids, list) else [external_ids]


class CogniteFilter:
    def __eq__(self, other):
        return type(self) == type(other) and self.dump() == other.dump()

    def __str__(self):
        item = utils._time.convert_time_attributes_to_datetime(self.dump())
        return json.dumps(item, default=utils._auxiliary.json_dump_default, indent=4)

    def __repr__(self):
        return self.__str__()

    def __getattribute__(self, item):
        attr = super().__getattribute__(item)
        if item == "_cognite_client":
            if attr is None:
                raise CogniteMissingClientError
        return attr

    def dump(self, camel_case: bool = False):
        """Dump the instance into a json serializable Python data type.

        Returns:
            Dict[str, Any]: A dictionary representation of the instance.
        """

        dump_key = lambda key: key if not camel_case else utils._auxiliary.to_camel_case(key)
        return {
            dump_key(key): value
            for key, value in self.__dict__.items()
            if value not in EXCLUDE_VALUE and not key.startswith("_")
        }
