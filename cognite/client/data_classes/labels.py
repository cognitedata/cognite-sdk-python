from typing import List

from cognite.client.data_classes._base import *


class LabelDefinition(CogniteResource):
    """A label definition is a globally defined label that can later be attached to resources (e.g., assets). For example, can you define a "Pump" label definition and attach that label to your pump assets.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): Name of the label.
        description (str): Description of the label.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str = None,
        name: str = None,
        description: str = None,
        created_time: int = None,
        cognite_client=None,
    ):
        self.external_id = external_id
        self.name = name
        self.description = description
        self.created_time = created_time
        self._cognite_client = cognite_client


class LabelDefinitionFilter(CogniteFilter):
    """Filter on labels definitions with strict matching.

    Args:
        name (str): Returns the label definitions matching that name.
        external_id_prefix (str): filter label definitions with external ids starting with the prefix specified
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(self, name: str = None, external_id_prefix: str = None, cognite_client=None):
        self.name = name
        self.external_id_prefix = external_id_prefix
        self._cognite_client = cognite_client


class LabelDefinitionList(CogniteResourceList):
    _RESOURCE = LabelDefinition
    _UPDATE = None
    _ASSERT_CLASSES = False  # because no Update


class Label(dict):
    """A label assigned to a resource.

    Args:
        external_id (str): The external id to the attached label.
    """

    def __init__(self, external_id: str = None, **kwargs):
        self.external_id = external_id
        self.update(kwargs)

    external_id = CognitePropertyClassUtil.declare_property("externalId")

    @classmethod
    def _load_list(cls, labels: List[Union[str, dict, LabelDefinition]]):
        def convert_label(label):
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
        if not isinstance(labels, list):
            labels = [labels]
        return [convert_label(label) for label in labels]

    @classmethod
    def _load(self, raw_label: Dict[str, Any]):
        return Label(external_id=raw_label["externalId"])

    def dump(self, camel_case: bool = False):
        dump_key = lambda key: key if not camel_case else utils._auxiliary.to_camel_case(key)
        return {dump_key(key): value for key, value in self.items()}


class LabelFilter(dict):
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

    def __init__(self, contains_any: List[str] = None, contains_all: List[str] = None, cognite_client=None):
        self.contains_any = contains_any
        self.contains_all = contains_all
        self._cognite_client = cognite_client

    def dump(self, camel_case: bool = False):
        dump_key = lambda key: key if not camel_case else utils._auxiliary.to_camel_case(key)
        wrap = lambda values: None if values is None else [{"externalId": value} for value in values]
        return {dump_key(key): wrap(value) for key, value in self.items()}

    contains_any = CognitePropertyClassUtil.declare_property("containsAny")
    contains_all = CognitePropertyClassUtil.declare_property("containsAll")
