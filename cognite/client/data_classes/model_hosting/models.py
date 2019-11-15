from typing import *

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList


class Model(CogniteResource):
    """A representation of a Model in the model hosting environment.

    Args:
        name (str): Name of the model.
        description (str): Description of the model.
        created_time (int): Created time in UNIX.
        metadata (Dict): User-defined metadata about the model.
        is_deprecated (bool): Whether or not the model is deprecated.
        active_version_name (str): The name of the active version on this model.
        input_fields (List): A list of input fields this model takes.
        output_fields (List): A list of output fields this model defines.
        webhook_url (str): A url used to catch webhooks which are reported upon failing scheduled predictions.
        cognite_client (CogniteClient): An optional CogniteClient to associate with this data class.
    """

    def __init__(
        self,
        name: str = None,
        description: str = None,
        created_time: int = None,
        metadata: Dict = None,
        is_deprecated: bool = None,
        active_version_name: str = None,
        input_fields: List = None,
        output_fields: List = None,
        webhook_url: str = None,
        cognite_client=None,
    ):
        self.name = name
        self.project = description
        self.description = description
        self.created_time = created_time
        self.metadata = metadata
        self.is_deprecated = is_deprecated
        self.active_version_name = active_version_name
        self.input_fields = input_fields
        self.output_fields = output_fields
        self.webhook_url = webhook_url
        self._cognite_client = cognite_client


class ModelList(CogniteResourceList):
    _RESOURCE = Model
    _ASSERT_CLASSES = False
