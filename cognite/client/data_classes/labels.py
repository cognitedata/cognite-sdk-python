from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union, cast

from cognite.client import utils
from cognite.client.data_classes._base import (
    CogniteFilter,
    CognitePropertyClassUtil,
    CogniteResource,
    CogniteResourceList,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class LabelDefinition(CogniteResource):
    """A label definition is a globally defined label that can later be attached to resources (e.g., assets). For example, can you define a "Pump" label definition and attach that label to your pump assets.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): Name of the label.
        description (str): Description of the label.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        data_set_id (int): The id of the dataset this label belongs to.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str = None,
        name: str = None,
        description: str = None,
        created_time: int = None,
        data_set_id: int = None,
        cognite_client: "CogniteClient" = None,
    ):
        self.external_id = external_id
        self.name = name
        self.description = description
        self.created_time = created_time
        self.data_set_id = data_set_id
        self._cognite_client = cast("CogniteClient", cognite_client)


class LabelDefinitionFilter(CogniteFilter):
    """Filter on labels definitions with strict matching.

    Args:
        name (str): Returns the label definitions matching that name.
        external_id_prefix (str): filter label definitions with external ids starting with the prefix specified
        data_set_ids (List[Dict[str, Any]]): Only include labels that belong to these datasets.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        name: str = None,
        external_id_prefix: str = None,
        data_set_ids: List[Dict[str, Any]] = None,
        cognite_client: "CogniteClient" = None,
    ):
        self.name = name
        self.external_id_prefix = external_id_prefix
        self.data_set_ids = data_set_ids
        self._cognite_client = cast("CogniteClient", cognite_client)


class LabelDefinitionList(CogniteResourceList):
    _RESOURCE = LabelDefinition


class Label(dict):
    """A label assigned to a resource.

    Args:
        external_id (str): The external id to the attached label.
    """

    def __init__(self, external_id: str = None, **kwargs: Any):
        self.external_id = external_id
        self.update(kwargs)

    external_id = CognitePropertyClassUtil.declare_property("externalId")

    @classmethod
    def _load_list(
        cls, labels: Optional[Sequence[Union[str, dict, LabelDefinition, "Label"]]]
    ) -> Optional[List["Label"]]:
        def convert_label(label: Union[Label, str, LabelDefinition, dict]) -> Label:
            if isinstance(label, Label):
                return label
            elif isinstance(label, str):
                return Label(label)
            elif isinstance(label, LabelDefinition):
                return Label(label.external_id)
            elif isinstance(label, dict):
                if "externalId" in label:
                    return Label(label["externalId"])
            raise ValueError("Could not parse label: {}".format(label))

        if labels is None:
            return None
        return [convert_label(label) for label in labels]

    @classmethod
    def _load(self, raw_label: Dict[str, Any]) -> "Label":
        return Label(external_id=raw_label["externalId"])

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        dump_key = lambda key: key if not camel_case else utils._auxiliary.to_camel_case(key)
        return {dump_key(key): value for key, value in self.items()}


class LabelFilter(dict, CogniteFilter):
    """Return only the resource matching the specified label constraints.

    Args:
        contains_any (List[Label]): The resource item contains at least one of the listed labels. The labels are defined by a list of external ids.
        contains_all (List[Label]): The resource item contains at least all the listed labels. The labels are defined by a list of external ids.
        cognite_client (CogniteClient): The client to associate with this object.

    Examples:

            List only resources marked as a PUMP and VERIFIED::

                >>> from cognite.client.data_classes import LabelFilter
                >>> my_label_filter = LabelFilter(contains_all=["PUMP", "VERIFIED"])

            List only resources marked as a PUMP or as a VALVE::

                >>> from cognite.client.data_classes import LabelFilter
                >>> my_label_filter = LabelFilter(contains_any=["PUMP", "VALVE"])
    """

    def __init__(
        self, contains_any: List[str] = None, contains_all: List[str] = None, cognite_client: "CogniteClient" = None
    ):
        self.contains_any = contains_any
        self.contains_all = contains_all
        self._cognite_client = cast("CogniteClient", cognite_client)

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        dump_key = lambda key: key if not camel_case else utils._auxiliary.to_camel_case(key)
        wrap = lambda values: None if values is None else [{"externalId": value} for value in values]
        return {dump_key(key): wrap(value) for key, value in self.items()}

    contains_any = CognitePropertyClassUtil.declare_property("containsAny")
    contains_all = CognitePropertyClassUtil.declare_property("containsAll")
