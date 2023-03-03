import json
from collections import UserList
from typing import Dict, Generic, List, TypeVar

from cognite.client import utils
from cognite.client.exceptions import CogniteMissingClientError
from cognite.client.utils._auxiliary import convert_all_keys_to_camel_case, to_camel_case, to_snake_case
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._pandas_helpers import notebook_display_with_fallback
from cognite.client.utils._time import convert_time_attributes_to_datetime

EXCLUDE_VALUE = [None]


def basic_instance_dump(obj, camel_case):
    dumped = {k: v for (k, v) in vars(obj).items() if ((v not in EXCLUDE_VALUE) and (not k.startswith("_")))}
    if camel_case:
        return convert_all_keys_to_camel_case(dumped)
    return dumped


class CogniteResponse:
    def __str__(self):
        item = convert_time_attributes_to_datetime(self.dump())
        return json.dumps(item, default=utils._auxiliary.json_dump_default, indent=4)

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return (type(other) == type(self)) and (other.dump() == self.dump())

    def __getattribute__(self, item):
        attr = super().__getattribute__(item)
        if item == "_cognite_client":
            if attr is None:
                raise CogniteMissingClientError
        return attr

    def dump(self, camel_case=False):
        return basic_instance_dump(self, camel_case=camel_case)

    @classmethod
    def _load(cls, api_response):
        raise NotImplementedError

    def to_pandas(self):
        raise NotImplementedError


T_CogniteResponse = TypeVar("T_CogniteResponse", bound=CogniteResponse)


class CogniteResource:
    def __new__(cls, *args, **kwargs):
        obj = super().__new__(cls)
        obj._cognite_client = None
        if "cognite_client" in kwargs:
            obj._cognite_client = kwargs["cognite_client"]
        return obj

    def __eq__(self, other):
        return (type(self) == type(other)) and (self.dump() == other.dump())

    def __str__(self):
        item = convert_time_attributes_to_datetime(self.dump())
        return json.dumps(item, default=utils._auxiliary.json_dump_default, indent=4)

    def __getattribute__(self, item):
        attr = super().__getattribute__(item)
        if item == "_cognite_client":
            if attr is None:
                raise CogniteMissingClientError
        return attr

    def dump(self, camel_case=False):
        return basic_instance_dump(self, camel_case=camel_case)

    @classmethod
    def _load(cls, resource, cognite_client=None):
        if isinstance(resource, str):
            return cls._load(json.loads(resource), cognite_client=cognite_client)
        elif isinstance(resource, Dict):
            instance = cls(cognite_client=cognite_client)
            for (key, value) in resource.items():
                snake_case_key = to_snake_case(key)
                if hasattr(instance, snake_case_key):
                    setattr(instance, snake_case_key, value)
            return instance
        raise TypeError(f"Resource must be json str or dict, not {type(resource)}")

    def to_pandas(self, expand=("metadata",), ignore=None, camel_case=False):
        ignore = [] if (ignore is None) else ignore
        pd = utils._auxiliary.local_import("pandas")
        dumped = self.dump(camel_case=camel_case)
        for element in ignore:
            del dumped[element]
        for key in expand:
            if key in dumped:
                if isinstance(dumped[key], dict):
                    dumped.update(dumped.pop(key))
                else:
                    raise AssertionError(f"Could not expand attribute '{key}'")
        df = pd.DataFrame(columns=["value"])
        for (name, value) in dumped.items():
            df.loc[name] = [value]
        return df

    def _repr_html_(self):
        return notebook_display_with_fallback(self)


T_CogniteResource = TypeVar("T_CogniteResource", bound=CogniteResource)


class CognitePropertyClassUtil:
    @staticmethod
    def declare_property(schema_name):
        return (
            property(lambda s: (s[schema_name] if (schema_name in s) else None))
            .setter((lambda s, v: CognitePropertyClassUtil._property_setter(s, schema_name, v)))
            .deleter(lambda s: s.pop(schema_name, None))
        )

    @staticmethod
    def _property_setter(self, schema_name, value):
        if value is None:
            self.pop(schema_name, None)
        else:
            self[schema_name] = value


class CogniteResourceList(UserList):
    def __init__(self, resources, cognite_client=None):
        for resource in resources:
            if not isinstance(resource, self._RESOURCE):
                raise TypeError(
                    f"All resources for class '{self.__class__.__name__}' must be of type '{self._RESOURCE.__name__}', not '{type(resource)}'."
                )
        self._cognite_client = cognite_client
        super().__init__(resources)
        (self._id_to_item, self._external_id_to_item) = ({}, {})
        if self.data:
            if hasattr(self.data[0], "external_id"):
                self._external_id_to_item = {
                    item.external_id: item for item in self.data if (item.external_id is not None)
                }
            if hasattr(self.data[0], "id"):
                self._id_to_item = {item.id: item for item in self.data if (item.id is not None)}

    def __getattribute__(self, item):
        attr = super().__getattribute__(item)
        if (item == "_cognite_client") and (attr is None):
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
        item = convert_time_attributes_to_datetime(self.dump())
        return json.dumps(item, default=utils._auxiliary.json_dump_default, indent=4)

    def dump(self, camel_case=False):
        return [resource.dump(camel_case) for resource in self.data]

    def get(self, id=None, external_id=None):
        IdentifierSequence.load(id, external_id).assert_singleton()
        if id:
            return self._id_to_item.get(id)
        return self._external_id_to_item.get(external_id)

    def to_pandas(self, camel_case=False):
        pd = utils._auxiliary.local_import("pandas")
        df = pd.DataFrame(self.dump(camel_case=camel_case))
        nullable_int_cols = ["start_time", "end_time", "asset_id", "parent_id", "data_set_id"]
        if camel_case:
            nullable_int_cols = list(map(to_camel_case, nullable_int_cols))
        to_convert = df.columns.intersection(nullable_int_cols)
        df[to_convert] = df[to_convert].astype("Int64")
        return df

    def _repr_html_(self):
        return notebook_display_with_fallback(self)

    @classmethod
    def _load(cls, resource_list, cognite_client=None):
        if isinstance(resource_list, str):
            return cls._load(json.loads(resource_list), cognite_client=cognite_client)
        elif isinstance(resource_list, List):
            resources = [cls._RESOURCE._load(resource, cognite_client=cognite_client) for resource in resource_list]
            return cls(resources, cognite_client=cognite_client)


T_CogniteResourceList = TypeVar("T_CogniteResourceList", bound=CogniteResourceList)


class CogniteUpdate:
    def __init__(self, id=None, external_id=None):
        self._id = id
        self._external_id = external_id
        self._update_object = {}

    def __eq__(self, other):
        return (type(self) == type(other)) and (self.dump() == other.dump())

    def __str__(self):
        return json.dumps(self.dump(), default=utils._auxiliary.json_dump_default, indent=4)

    def __repr__(self):
        return str(self)

    def _set(self, name, value):
        update_obj = self._update_object.get(name, {})
        assert ("remove" not in update_obj) and (
            "add" not in update_obj
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

    def _modify(self, name, value):
        update_obj = self._update_object.get(name, {})
        assert "set" not in update_obj, "Can not call remove or add fields after calling set on an update object."
        assert (
            "modify" not in update_obj
        ), "Can not call modify twice on the same object, please combine your items and pass them to modify in one call."
        update_obj["modify"] = value
        self._update_object[name] = update_obj

    def dump(self):
        dumped = {"update": self._update_object}
        if self._id is not None:
            dumped["id"] = self._id
        elif self._external_id is not None:
            dumped["externalId"] = self._external_id
        return dumped

    @classmethod
    def _get_update_properties(cls):
        return [
            key for key in cls.__dict__.keys() if ((not key.startswith("_")) and (key not in ["labels", "columns"]))
        ]


T_CogniteUpdate = TypeVar("T_CogniteUpdate", bound=CogniteUpdate)


class CognitePrimitiveUpdate(Generic[T_CogniteUpdate]):
    def __init__(self, update_object, name):
        self._update_object = update_object
        self._name = name

    def _set(self, value):
        if value is None:
            self._update_object._set_null(self._name)
        else:
            self._update_object._set(self._name, value)
        return self._update_object


class CogniteObjectUpdate(Generic[T_CogniteUpdate]):
    def __init__(self, update_object, name):
        self._update_object = update_object
        self._name = name

    def _set(self, value):
        self._update_object._set(self._name, value)
        return self._update_object

    def _add(self, value):
        self._update_object._add(self._name, value)
        return self._update_object

    def _remove(self, value):
        self._update_object._remove(self._name, value)
        return self._update_object


class CogniteListUpdate(Generic[T_CogniteUpdate]):
    def __init__(self, update_object, name):
        self._update_object = update_object
        self._name = name

    def _set(self, value):
        self._update_object._set(self._name, value)
        return self._update_object

    def _add(self, value):
        self._update_object._add(self._name, value)
        return self._update_object

    def _remove(self, value):
        self._update_object._remove(self._name, value)
        return self._update_object

    def _modify(self, value):
        self._update_object._modify(self._name, value)
        return self._update_object


class CogniteLabelUpdate(Generic[T_CogniteUpdate]):
    def __init__(self, update_object, name):
        self._update_object = update_object
        self._name = name

    def _set(self, external_ids):
        self._update_object._set(self._name, self._wrap_ids(self._wrap_in_list(external_ids)))
        return self._update_object

    def _add(self, external_ids):
        self._update_object._add(self._name, self._wrap_ids(self._wrap_in_list(external_ids)))
        return self._update_object

    def _remove(self, external_ids):
        self._update_object._remove(self._name, self._wrap_ids(self._wrap_in_list(external_ids)))
        return self._update_object

    def _wrap_ids(self, external_ids):
        return [{"externalId": external_id} for external_id in external_ids]

    def _wrap_in_list(self, external_ids):
        return external_ids if isinstance(external_ids, list) else [external_ids]


class CogniteFilter:
    def __eq__(self, other):
        return (type(self) == type(other)) and (self.dump() == other.dump())

    def __str__(self):
        item = convert_time_attributes_to_datetime(self.dump())
        return json.dumps(item, default=utils._auxiliary.json_dump_default, indent=4)

    def __repr__(self):
        return str(self)

    def __getattribute__(self, item):
        attr = super().__getattribute__(item)
        if item == "_cognite_client":
            if attr is None:
                raise CogniteMissingClientError
        return attr

    @classmethod
    def _load(cls, resource):
        if isinstance(resource, str):
            return cls._load(json.loads(resource))
        elif isinstance(resource, Dict):
            instance = cls()
            for (key, value) in resource.items():
                snake_case_key = to_snake_case(key)
                if hasattr(instance, snake_case_key):
                    setattr(instance, snake_case_key, value)
            return instance
        raise TypeError(f"Resource must be json str or dict, not {type(resource)}")

    def dump(self, camel_case=False):
        return basic_instance_dump(self, camel_case=camel_case)


T_CogniteFilter = TypeVar("T_CogniteFilter", bound=CogniteFilter)
