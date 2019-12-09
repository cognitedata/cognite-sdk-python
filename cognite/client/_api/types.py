from typing import *

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import ParentTypeDefinitionFilter, Type, TypeFilter, TypeList


class TypesAPI(APIClient):
    _RESOURCE_PATH = "/types"
    _LIST_CLASS = TypeList

    def __iter__(self) -> Generator[Type, None, None]:
        """Iterate over Types

        Fetches Types as they are iterated over, so you keep a limited number of Types in memory.

        Yields:
            Type: yields Types one by one.
        """
        return self.__call__()

    def __call__(
        self,
        name: str = None,
        external_id_prefix: str = None,
        type_subtree: Union[Dict[str, Any], ParentTypeDefinitionFilter] = None,
        limit: int = None,
    ):
        filter = TypeFilter(name=name, external_id_prefix=external_id_prefix, type_subtree=type_subtree).dump(
            camel_case=True
        )
        return self._list_generator(method="POST", limit=limit, filter=filter)

    def list(
        self,
        name: str = None,
        external_id_prefix: str = None,
        type_subtree: Union[Dict[str, Any], ParentTypeDefinitionFilter] = None,
        limit: int = 25,
    ) -> TypeList:
        """`List Types <https://docs.cognite.com/api/playground/#operation/listTypes>`_

        Args:
            name (str): returns the type definitions matching that name
            external_id_prefix (str): filter external ids starting with the prefix specified
            root_parent (Union[Dict[str, Any], ParentTypeDefinitionFilter]): filter for type definitions that belong to the subtree defined by the root parent type specified

        Returns:
            TypeList: List of requested Types

        Examples:

            List Types and filter on name::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> file_list = c.types.list(limit=5, name="name")

            Iterate over type definitions::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> for type in c.types:
                ...     type # do something with the  type definition

            Iterate over chunks of type definitions to reduce memory load::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> for type_list in c.types(chunk_size=2500):
                ...     type_list # do something with the type definitions
        """
        filter = TypeFilter(name=name, external_id_prefix=external_id_prefix, type_subtree=type_subtree).dump(
            camel_case=True
        )
        return self._list(method="POST", limit=limit, filter=filter)

    def retrieve(self, id: Optional[int] = None, external_id: Optional[str] = None) -> Optional[Type]:
        """`Retrieve a single type definition by id. <https://docs.cognite.com/api/playground/#operation/getTypes>`_

        Args:
            id (int, optional): ID
            external_id (str, optional): External ID

        Returns:
            Optional[Type]: Requested Type or None if it does not exist.

        Examples:

            Get Type by id::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.types.retrieve(id=1)

            Get Type by external id::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.types.retrieve(external_id="1")
        """
        utils._auxiliary.assert_exactly_one_of_id_or_external_id(id, external_id)
        return self._retrieve_multiple(ids=id, external_ids=external_id, wrap_ids=True)

    def retrieve_multiple(self, ids: Optional[List[int]] = None, external_ids: Optional[List[str]] = None) -> TypeList:
        """`Retrieve multiple type definitions by id. <https://docs.cognite.com/api/playground/#operation/getTypes>`_

        Args:
            ids (List[int], optional): IDs
            external_ids (List[str], optional): External IDs

        Returns:
            TypeList: The requested Types.

        Examples:

            Get Types by id::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.types.retrieve_multiple(ids=[1, 2, 3])

            Get Types by external id::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.types.retrieve_multiple(external_ids=["abc", "def"])
        """
        utils._auxiliary.assert_type(ids, "id", [List], allow_none=True)
        utils._auxiliary.assert_type(external_ids, "external_id", [List], allow_none=True)
        return self._retrieve_multiple(ids=ids, external_ids=external_ids, wrap_ids=True)

    def create(self, Type: Union[Type, List[Type]]) -> Union[Type, TypeList]:
        """`Create one or more Types. <https://docs.cognite.com/api/playground/#operation/createTypeDefinitions>`_

        Args:
            Type (Union[Type, List[Type]]): type definition or list of type definitions to create.
            Note the properties array for a Type consists of a list with items of the form {'propertyId':'abc','name':'...','description::'...','type':'string'}

        Returns:
            Union[Type, TypeList]: Created type definition(s)

        Examples:

            Create new type definitions::

                >>> from cognite.client.experimental import CogniteClient
                >>> from cognite.client.data_classes import Type
                >>> c = CogniteClient()
                >>> Types = [Type(external_id="valve"), Type(external_id="pipe",parent_type={"externalId":"parent","version":123})]
                >>> res = c.types.create(Types)
        """
        return self._create_multiple(items=Type)

    def delete(
        self,
        id: Union[int, List[int]] = None,
        external_id: Union[str, List[str]] = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete one or more type definitions <https://docs.cognite.com/api/playground/#operation/deleteTypes>`_

        Args:
            id (Union[int, List[int]): Id or list of ids
            external_id (Union[str, List[str]]): External ID or list of external ids
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found

        Returns:
            None
        Examples:

            Delete type definitions by id or external id::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> c.types.delete(id=[1,2,3], external_id="3",soft=False)
        """
        self._delete_multiple(
            ids=id, external_ids=external_id, wrap_ids=True
        )  # , extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids}

    def update(self, version: int, new_version: Type, id: int = None, external_id: str = None) -> Union[Type, TypeList]:
        """`Update a type definition <https://docs.cognite.com/api/playground/#operation/updateTypeDefinitions>`_

        Args:
            id: id of the type
            external_id: external id of the type. If both external_id and id are not passed, the external_id of the `new_version` will be used.
            version: version of the type
            new_version: new type specification

        Returns:
            Type: Updated Type"""
        external_id = external_id or new_version.external_id
        assert external_id == new_version.external_id
        utils._auxiliary.assert_at_least_one_of_id_or_external_id(id, external_id)
        json = {"id": id, "external_id": external_id, "version": version, "set": new_version.dump(camel_case=True)}
        return Type._load(self._post(url_path=self._RESOURCE_PATH + "/update", json=json).json()["items"][0])
