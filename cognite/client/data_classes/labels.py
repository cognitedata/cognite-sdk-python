from cognite.client.data_classes._base import (
    CogniteFilter,
    CognitePropertyClassUtil,
    CogniteResource,
    CogniteResourceList,
)
from cognite.client.utils._auxiliary import convert_all_keys_to_camel_case, to_camel_case


class LabelDefinition(CogniteResource):
    def __init__(
        self, external_id=None, name=None, description=None, created_time=None, data_set_id=None, cognite_client=None
    ):
        self.external_id = external_id
        self.name = name
        self.description = description
        self.created_time = created_time
        self.data_set_id = data_set_id
        self._cognite_client = cognite_client


class LabelDefinitionFilter(CogniteFilter):
    def __init__(self, name=None, external_id_prefix=None, data_set_ids=None, cognite_client=None):
        self.name = name
        self.external_id_prefix = external_id_prefix
        self.data_set_ids = data_set_ids
        self._cognite_client = cognite_client


class LabelDefinitionList(CogniteResourceList):
    _RESOURCE = LabelDefinition


class Label(dict):
    def __init__(self, external_id=None, **kwargs):
        self.external_id = external_id
        self.update(kwargs)

    external_id = CognitePropertyClassUtil.declare_property("externalId")

    @classmethod
    def _load_list(cls, labels):
        def convert_label(label: Union[(Label, str, LabelDefinition, dict)]) -> Label:
            if isinstance(label, Label):
                return label
            elif isinstance(label, str):
                return Label(label)
            elif isinstance(label, LabelDefinition):
                return Label(label.external_id)
            elif isinstance(label, dict):
                if "externalId" in label:
                    return Label(label["externalId"])
            raise ValueError(f"Could not parse label: {label}")

        if labels is None:
            return None
        return [convert_label(label) for label in labels]

    @classmethod
    def _load(cls, raw_label):
        return cls(external_id=raw_label["externalId"])

    def dump(self, camel_case=False):
        return convert_all_keys_to_camel_case(self) if camel_case else dict(self)


class LabelFilter(dict, CogniteFilter):
    def __init__(self, contains_any=None, contains_all=None, cognite_client=None):
        self.contains_any = contains_any
        self.contains_all = contains_all
        self._cognite_client = cognite_client

    @staticmethod
    def _wrap_labels(values):
        if values is None:
            return None
        return [{"externalId": v} for v in values]

    def dump(self, camel_case=False):
        keys = map(to_camel_case, self.keys()) if camel_case else self.keys()
        return dict(zip(keys, map(self._wrap_labels, self.values())))

    contains_any = CognitePropertyClassUtil.declare_property("containsAny")
    contains_all = CognitePropertyClassUtil.declare_property("containsAll")
