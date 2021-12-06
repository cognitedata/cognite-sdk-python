from typing import Any, Dict

from cognite.client import utils
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList


class FeatureType(CogniteResource):
    """A representation of a feature type in the geospatial api.
    """

    def __init__(
        self,
        external_id: str = None,
        created_time: int = None,
        last_updated_time: int = None,
        attributes: Dict[str, Any] = None,
        search_spec: Dict[str, Any] = None,
        cognite_client=None,
        cognite_domain=None,
    ):
        self.external_id = external_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.attributes = attributes
        self.search_spec = search_spec
        self._cognite_client = cognite_client
        self._cognite_domain = cognite_domain

    @classmethod
    def _load(cls, resource: Dict, cognite_client=None):
        cognite_domain = cognite_client.geospatial.get_current_cognite_domain()
        instance = cls(cognite_client=cognite_client, cognite_domain=cognite_domain)
        for key, value in resource.items():
            snake_case_key = utils._auxiliary.to_snake_case(key)
            setattr(instance, snake_case_key, value)
        return instance


class FeatureTypeList(CogniteResourceList):
    _RESOURCE = FeatureType
    _ASSERT_CLASSES = False


class AttributeAndSearchSpec:
    """A representation of a feature type attribute and search spec.
    """

    def __init__(
        self, attributes: Dict[str, Any] = None, search_spec: Dict[str, Any] = None,
    ):
        self.attributes = attributes
        self.search_spec = search_spec


class FeatureTypeUpdate:
    """A representation of a feature type update in the geospatial api.
    """

    def __init__(
        self, external_id: str = None, add: AttributeAndSearchSpec = None, cognite_client=None, cognite_domain=None,
    ):
        self.external_id = external_id
        self.add = add
        self._cognite_client = cognite_client
        self._cognite_domain = cognite_domain


class FeatureTypeUpdateList:
    _RESOURCE = FeatureTypeUpdate
    _ASSERT_CLASSES = False


class Feature(CogniteResource):
    """A representation of a feature in the geospatial api.
    """

    def __init__(
        self, external_id: str = None, cognite_client=None, **attributes,
    ):
        self.external_id = external_id
        for key in attributes:
            setattr(self, key, attributes[key])
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Dict, cognite_client=None):
        instance = cls(cognite_client=cognite_client)
        for key, value in resource.items():
            snake_case_key = utils._auxiliary.to_snake_case(key)
            setattr(instance, snake_case_key, value)
        return instance


def _is_geometry_type(attribute_type: str):
    return attribute_type in {"POINT"}


class FeatureList(CogniteResourceList):
    _RESOURCE = Feature
    _ASSERT_CLASSES = False

class CoordinateReferenceSystem(CogniteResource):
    """A representation of a feature in the geospatial api.
    """

    def __init__(self, srid: int = None, wkt: str = None, proj_string: str = None, cognite_client=None):
        self.srid = srid
        self.wkt = wkt
        self.proj_string = proj_string
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Dict, cognite_client=None):
        instance = cls(cognite_client=cognite_client)
        for key, value in resource.items():
            snake_case_key = utils._auxiliary.to_snake_case(key)
            setattr(instance, snake_case_key, value)
        return instance


class CoordinateReferenceSystemList(CogniteResourceList):
    _RESOURCE = CoordinateReferenceSystem
    _ASSERT_CLASSES = False
