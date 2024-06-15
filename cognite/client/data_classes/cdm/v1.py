from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.instances import PropertyLike


@dataclass
class Sourceable(PropertyLike):
    _source: ClassVar[ViewId] = ViewId("cdf_cdm_experimental", "Sourceable", "v1")
    source_id: str
    source: str
    source_created_time: datetime
    source_updated_time: datetime
    source_created_user: str
    source_updated_user: str
