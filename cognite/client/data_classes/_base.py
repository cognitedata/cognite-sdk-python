from __future__ import annotations

import contextlib
import json
from collections import UserList
from typing import TYPE_CHECKING, Any, Dict, Generic, List, Optional, Sequence, Type, TypeVar, Union, cast, overload

from cognite.client import utils
from cognite.client.exceptions import CogniteMissingClientError
from cognite.client.utils._auxiliary import fast_dict_load
from cognite.client.utils._identifier import Identifier
from cognite.client.utils._pandas_helpers import convert_nullable_int_cols, notebook_display_with_fallback
from cognite.client.utils._text import convert_all_keys_to_camel_case
from cognite.client.utils._time import convert_time_attributes_to_datetime

if TYPE_CHECKING:
    import pandas

    from cognite.client import CogniteClient


_T = TypeVar("_T")


# We want to reuse these functions as a property for both 'CogniteResource' and 'CogniteResourceList',
# so we define a getter and setter instead of using (incomprehensible) multiple inheritance:
def _cognite_client_getter(self: T_CogniteResource | CogniteResourceList) -> CogniteClient:
    with contextlib.suppress(AttributeError):
        if self.___cognite_client is not None:
            return self.___cognite_client
    raise CogniteMissingClientError(self)


def _cognite_client_setter(self: T_CogniteResource | CogniteResourceList, value: Optional[CogniteClient]) -> None:
    from cognite.client import CogniteClient

    if value is None or isinstance(value, CogniteClient):
        self.___cognite_client = value
    else:
        raise AttributeError(
            "Can't set the CogniteClient reference to anything else than a CogniteClient instance or None"
        )


class CogniteBase:
    def __init__(self) -> None:
        raise NotImplementedError

    def __str__(self) -> str:
        item = convert_time_attributes_to_datetime(self.dump())
        return json.dumps(item, default=utils._auxiliary.json_dump_default, indent=4)

    def __repr__(self) -> str:
        return str(self)

    def __eq__(self, other: Any) -> bool:
        return type(self) is type(other) and self.dump() == other.dump()

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            Dict[str, Any]: A dictionary representation of the instance.
        """
        dumped = {k: v for k, v in vars(self).items() if v is not None and not k.startswith("_")}
        if camel_case:
            return convert_all_keys_to_camel_case(dumped)
        return dumped


T_CogniteBase = TypeVar("T_CogniteBase", bound=CogniteBase)


class CogniteResponse(CogniteBase):
    def to_pandas(self) -> pandas.DataFrame:
        raise NotImplementedError

    @classmethod
    def _load(cls: Type[T_CogniteResponse], item: Dict[str, Any]) -> T_CogniteResponse:
        raise NotImplementedError


T_CogniteResponse = TypeVar("T_CogniteResponse", bound=CogniteResponse)


class CogniteFilter(CogniteBase):
    ...


T_CogniteFilter = TypeVar("T_CogniteFilter", bound=CogniteFilter)


class CogniteResource(CogniteBase):
    """A CogniteResource, as opposed to CogniteResponse, is any resource that needs access to an
    instantiated Cognite client in order to implement helper methods, e.g. `Asset.parent()`
    """

    ___cognite_client: Optional[CogniteClient]
    _cognite_client = property(_cognite_client_getter, _cognite_client_setter)

    def __init__(self, cognite_client: Optional[CogniteClient] = None) -> None:
        raise NotImplementedError

    @classmethod
    def _load(
        cls: Type[T_CogniteResource], item: Dict[str, Any], cognite_client: Optional[CogniteClient]
    ) -> T_CogniteResource:
        if isinstance(item, Dict):
            return fast_dict_load(cls, item, cognite_client=cognite_client)
        raise TypeError(f"Item to load must be a mapping, not {type(item)}")

    def to_pandas(
        self, expand: Sequence[str] = ("metadata",), ignore: List[str] = None, camel_case: bool = False
    ) -> pandas.DataFrame:
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
        pd = cast(Any, utils._auxiliary.local_import("pandas"))
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
        for name, value in dumped.items():
            df.loc[name] = [value]
        return df

    def _repr_html_(self) -> str:
        return notebook_display_with_fallback(self)


T_CogniteResource = TypeVar("T_CogniteResource", bound=CogniteResource)


class CogniteResourceList(Generic[T_CogniteResource], UserList):
    _RESOURCE: Type[T_CogniteResource]
    ___cognite_client: Optional[CogniteClient]

    _cognite_client = property(_cognite_client_getter, _cognite_client_setter)

    def __init__(self, items: List[T_CogniteResource], cognite_client: Optional[CogniteClient]):
        self._verify_items(items)
        super().__init__(items)
        self._init_lookup()
        self._cognite_client = cognite_client

    def _verify_items(self, items: List[T_CogniteBase]) -> None:
        for item in items:
            if not isinstance(item, self._RESOURCE):
                raise TypeError(
                    f"All resources for class '{type(self).__name__}' must be of type "
                    f"'{self._RESOURCE}', not '{type(item)}'."
                )

    def _init_lookup(self) -> None:
        self._id_to_item, self._external_id_to_item = {}, {}
        if not self.data:
            return
        if hasattr(self.data[0], "external_id"):
            self._external_id_to_item = {item.external_id: item for item in self.data if item.external_id is not None}  # type: ignore [attr-defined]
        if hasattr(self.data[0], "id"):
            self._id_to_item = {item.id: item for item in self.data if item.id is not None}  # type: ignore [attr-defined]

    @classmethod
    def _load(
        cls: Type[T_CogniteResourceList], items: List[Dict[str, Any]], cognite_client: Optional[CogniteClient]
    ) -> T_CogniteResourceList:
        if isinstance(items, list):
            resources = [cls._RESOURCE._load(res, cognite_client=cognite_client) for res in items]
            return cls(resources, cognite_client=cognite_client)
        raise TypeError(f"The items to load must be a list (of dicts), not {type(items)}")

    @overload  # type: ignore [override]
    # Generic[T] + UserList does not like this overload:
    def __getitem__(self: T_CogniteResourceList, item: int) -> T_CogniteResource:
        ...

    @overload
    def __getitem__(self: T_CogniteResourceList, item: slice) -> T_CogniteResourceList:
        ...

    def __getitem__(
        self: T_CogniteResourceList, item: Union[int, slice]
    ) -> Union[T_CogniteResource, T_CogniteResourceList]:
        value = self.data[item]
        if isinstance(item, slice):
            return type(self)(value, cognite_client=self._get_cognite_client())
        return cast(T_CogniteResource, value)

    def _get_cognite_client(self) -> Optional[CogniteClient]:
        # Get client reference without raising (when missing)
        return self.___cognite_client

    def __str__(self) -> str:
        item = convert_time_attributes_to_datetime(self.dump())
        return json.dumps(item, default=utils._auxiliary.json_dump_default, indent=4)

    def __repr__(self) -> str:
        return str(self)

    def __eq__(self, other: Any) -> bool:
        return type(self) is type(other) and self.dump() == other.dump()

    # TODO: We inherit a lot from UserList that we don't actually support...
    def extend(self, other: List[T_CogniteResource]) -> None:  # type: ignore [override]
        other_res_list = type(self)(other, cognite_client=None)  # See if we can accept the types
        if set(self._id_to_item).isdisjoint(other_res_list._id_to_item):
            super().extend(other)
            self._external_id_to_item.update(other_res_list._external_id_to_item)
            self._id_to_item.update(other_res_list._id_to_item)
        else:
            raise ValueError("Unable to extend as this would introduce duplicates")

    def dump(self, camel_case: bool = False) -> List[Dict[str, Any]]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            List[Dict[str, Any]]: A list of dicts representing the instance.
        """
        return [resource.dump(camel_case) for resource in self.data]

    def get(self, id: int = None, external_id: str = None) -> Optional[T_CogniteResource]:
        """Get an item from this list by id or exernal_id. Specify either, but not both.

        Args:
            id (int): The id of the item to get.
            external_id (str): The external_id of the item to get.

        Returns:
            Optional[CogniteResource]: The requested item
        """
        Identifier.of_either(id, external_id)
        if id:
            return self._id_to_item.get(id)
        return self._external_id_to_item.get(external_id)

    def to_pandas(self, camel_case: bool = False) -> pandas.DataFrame:
        """Convert the instance into a pandas DataFrame.

        Args:
            camel_case (bool): Decide if the columns names should be camelCased. Defaults to False.

        Returns:
            pandas.DataFrame: The dataframe.
        """
        pd = cast(Any, utils._auxiliary.local_import("pandas"))
        df = pd.DataFrame(self.dump(camel_case=camel_case))
        return convert_nullable_int_cols(df, camel_case)

    def _repr_html_(self) -> str:
        return notebook_display_with_fallback(self)


T_CogniteResourceList = TypeVar("T_CogniteResourceList", bound=CogniteResourceList)


class CogniteUpdate:
    def __init__(self, id: int = None, external_id: str = None) -> None:
        self._id = id
        self._external_id = external_id
        self._update_object: Dict[str, Any] = {}

    def __eq__(self, other: Any) -> bool:
        return type(self) is type(other) and self.dump() == other.dump()

    def __str__(self) -> str:
        return json.dumps(self.dump(), default=utils._auxiliary.json_dump_default, indent=4)

    def __repr__(self) -> str:
        return str(self)

    def _set(self, name: str, value: Any) -> None:
        update_obj = self._update_object.get(name, {})
        assert (
            "remove" not in update_obj and "add" not in update_obj
        ), "Can not call set after adding or removing fields from an update object."
        self._update_object[name] = {"set": value}

    def _set_null(self, name: str) -> None:
        self._update_object[name] = {"setNull": True}

    def _add(self, name: str, value: Any) -> None:
        update_obj = self._update_object.get(name, {})
        assert "set" not in update_obj, "Can not call remove or add fields after calling set on an update object."
        assert (
            "add" not in update_obj
        ), "Can not call add twice on the same object, please combine your objects and pass them to add in one call."
        update_obj["add"] = value
        self._update_object[name] = update_obj

    def _remove(self, name: str, value: Any) -> None:
        update_obj = self._update_object.get(name, {})
        assert "set" not in update_obj, "Can not call remove or add fields after calling set on an update object."
        assert (
            "remove" not in update_obj
        ), "Can not call remove twice on the same object, please combine your items and pass them to remove in one call."
        update_obj["remove"] = value
        self._update_object[name] = update_obj

    def _modify(self, name: str, value: Any) -> None:
        update_obj = self._update_object.get(name, {})
        assert "set" not in update_obj, "Can not call remove or add fields after calling set on an update object."
        assert (
            "modify" not in update_obj
        ), "Can not call modify twice on the same object, please combine your items and pass them to modify in one call."
        update_obj["modify"] = value
        self._update_object[name] = update_obj

    def dump(self, camel_case: bool = True) -> Dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Returns:
            Dict[str, Any]: A dictionary representation of the instance.
        """
        assert camel_case is True, "snake_case is currently unsupported"  # TODO maybe (when unifying classes)
        dumped: Dict[str, Any] = {"update": self._update_object}
        if self._id is not None:
            dumped["id"] = self._id
        elif self._external_id is not None:
            dumped["externalId"] = self._external_id
        return dumped

    @classmethod
    def _get_update_properties(cls) -> List[str]:
        return [key for key in cls.__dict__ if not (key.startswith("_") or key == "columns")]


T_CogniteUpdate = TypeVar("T_CogniteUpdate", bound=CogniteUpdate)


class CognitePrimitiveUpdate(Generic[T_CogniteUpdate]):
    def __init__(self, update_object: T_CogniteUpdate, name: str):
        self._update_object = update_object
        self._name = name

    def _set(self, value: Union[None, str, int, bool]) -> T_CogniteUpdate:
        if value is None:
            self._update_object._set_null(self._name)
        else:
            self._update_object._set(self._name, value)
        return self._update_object


class CogniteObjectUpdate(Generic[T_CogniteUpdate]):
    def __init__(self, update_object: T_CogniteUpdate, name: str) -> None:
        self._update_object = update_object
        self._name = name

    def _set(self, value: Dict) -> T_CogniteUpdate:
        self._update_object._set(self._name, value)
        return self._update_object

    def _add(self, value: Dict) -> T_CogniteUpdate:
        self._update_object._add(self._name, value)
        return self._update_object

    def _remove(self, value: List) -> T_CogniteUpdate:
        self._update_object._remove(self._name, value)
        return self._update_object


class CogniteListUpdate(Generic[T_CogniteUpdate]):
    def __init__(self, update_object: T_CogniteUpdate, name: str):
        self._update_object = update_object
        self._name = name

    def _set(self, value: List) -> T_CogniteUpdate:
        self._update_object._set(self._name, value)
        return self._update_object

    def _add(self, value: List) -> T_CogniteUpdate:
        self._update_object._add(self._name, value)
        return self._update_object

    def _remove(self, value: List) -> T_CogniteUpdate:
        self._update_object._remove(self._name, value)
        return self._update_object

    def _modify(self, value: List) -> T_CogniteUpdate:
        self._update_object._modify(self._name, value)
        return self._update_object


class CogniteLabelUpdate(Generic[T_CogniteUpdate]):
    def __init__(self, update_object: T_CogniteUpdate, name: str) -> None:
        self._update_object = update_object
        self._name = name

    def _set(self, external_ids: Union[str, List[str]]) -> T_CogniteUpdate:
        self._update_object._set(self._name, self._wrap_ids(self._wrap_in_list(external_ids)))
        return self._update_object

    def _add(self, external_ids: Union[str, List[str]]) -> T_CogniteUpdate:
        self._update_object._add(self._name, self._wrap_ids(self._wrap_in_list(external_ids)))
        return self._update_object

    def _remove(self, external_ids: Union[str, List[str]]) -> T_CogniteUpdate:
        self._update_object._remove(self._name, self._wrap_ids(self._wrap_in_list(external_ids)))
        return self._update_object

    def _wrap_ids(self, external_ids: List[str]) -> List[Dict[str, str]]:
        return [{"externalId": external_id} for external_id in external_ids]

    def _wrap_in_list(self, external_ids: Union[str, List[str]]) -> List[str]:
        return external_ids if isinstance(external_ids, list) else [external_ids]


class CognitePropertyClassUtil:
    @staticmethod
    def declare_property(schema_name: str) -> property:
        return (
            property(lambda s: s[schema_name] if schema_name in s else None)
            .setter(lambda s, v: CognitePropertyClassUtil._property_setter(s, schema_name, v))
            .deleter(lambda s: s.pop(schema_name, None))
        )

    @staticmethod
    def _property_setter(self: Any, schema_name: str, value: Any) -> None:
        if value is None:
            self.pop(schema_name, None)
        else:
            self[schema_name] = value
