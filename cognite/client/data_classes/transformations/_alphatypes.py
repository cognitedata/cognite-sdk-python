from cognite.client.data_classes._base import *
from cognite.client.data_classes.transformations.common import TransformationDestination


class AlphaDataModelInstances(TransformationDestination):
    """To be used when the transformation is meant to produce data model instances.
        Flexible Data Models resource type is on `alpha` version currently and the API may change.

    Args:
        model_external_id (str): external_id of the flexible data model.

    Returns:
        TransformationDestination pointing to the target flexible data model.
    """

    def __init__(self, model_external_id: str = None):
        super().__init__(type="data_model_instances")
        self.model_external_id = model_external_id

    def __hash__(self):
        return hash((self.type, self.model_external_id))

    def __eq__(self, obj):
        return isinstance(obj, AlphaDataModelInstances) and hash(obj) == hash(self)

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        ret = {"model_external_id": self.model_external_id, "type": self.type}
        if camel_case:
            return {utils._auxiliary.to_camel_case(key): value for key, value in ret.items()}
        return ret
