from typing import *

from cognite.client.data_classes._base import *
from cognite.client.data_classes.shared import TimestampRange


class TypeDefinitionReference(dict):
    pass


# GenPropertyClass: ParentTypeDefinitionFilter
class ParentTypeDefinitionFilter(dict):
    """filter for type definitions that belong to the subtree defined by the root parent type specified

    Args:
        id (int): Javascript friendly internal ID given to the object.
        external_id (str): External Id provided by client. Should be unique within the project.
        version (int): A JavaScript-friendly version for the object.
    """

    def __init__(self, id: int = None, external_id: str = None, version: int = None, **kwargs):
        self.id = id
        self.external_id = external_id
        self.version = version
        self.update(kwargs)

    id = CognitePropertyClassUtil.declare_property("id")
    external_id = CognitePropertyClassUtil.declare_property("externalId")
    version = CognitePropertyClassUtil.declare_property("version")

    # GenStop


# GenClass: TypeDefinitionSpec
class Type(CogniteResource):
    """No description.

    Args:
        external_id (str): External Id provided by client. Should be unique within the project.
        name (str): No description.
        description (str): No description.
        properties (List[Dict[str, Any]]): No description.
        parent_type (Union[Dict[str, Any], TypeDefinitionReference]): No description.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str = None,
        name: str = None,
        description: str = None,
        properties: List[Dict[str, Any]] = None,
        parent_type: Union[Dict[str, Any], TypeDefinitionReference] = None,
        cognite_client=None,
    ):
        self.external_id = external_id
        self.name = name
        self.description = description
        self.properties = properties
        self.parent_type = parent_type
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        instance = super(Type, cls)._load(resource, cognite_client)
        if isinstance(resource, Dict):
            if instance.parent_type is not None:
                instance.parent_type = TypeDefinitionReference(**instance.parent_type)
        return instance

    # GenStop


# GenClass: TypeDefinitionFilter.filter
class TypeFilter(CogniteFilter):
    """Filter on types with strict matching.

    Args:
        name (str): returns the type definitions matching that name
        external_id_prefix (str): filter external ids starting with the prefix specified
        root_parent (Union[Dict[str, Any], ParentTypeDefinitionFilter]): filter for type definitions that belong to the subtree defined by the root parent type specified
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        name: str = None,
        external_id_prefix: str = None,
        root_parent: Union[Dict[str, Any], ParentTypeDefinitionFilter] = None,
        cognite_client=None,
    ):
        self.name = name
        self.external_id_prefix = external_id_prefix
        self.root_parent = root_parent
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        instance = super(TypeFilter, cls)._load(resource, cognite_client)
        if isinstance(resource, Dict):
            if instance.root_parent is not None:
                instance.root_parent = ParentTypeDefinitionFilter(**instance.root_parent)
        return instance

    # GenStop


# GenClass: TypeDefinitionUpdate
class TypeUpdate(CogniteUpdate):
    """No description.

    Args:
        ref (Union[Dict[str, Any], TypeDefinitionReference]): No description.
        update (Union[Dict[str, Any], Type]): No description.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        ref: Union[Dict[str, Any], TypeDefinitionReference] = None,
        update: Union[Dict[str, Any], Type] = None,
        cognite_client=None,
    ):
        self.ref = ref
        self.update = update
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        instance = super(TypeUpdate, cls)._load(resource, cognite_client)
        if isinstance(resource, Dict):
            if instance.ref is not None:
                instance.ref = TypeDefinitionReference(**instance.ref)
            if instance.update is not None:
                instance.update = Type(**instance.update)
        return instance

    # GenStop


class TypeList(CogniteResourceList):
    _RESOURCE = Type
    _UPDATE = TypeUpdate
