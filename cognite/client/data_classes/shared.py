from cognite.client.data_classes._base import CognitePropertyClassUtil
from cognite.client.utils._auxiliary import convert_all_keys_to_camel_case, handle_deprecated_camel_case_argument


class TimestampRange(dict):
    def __init__(self, max=None, min=None, **kwargs):
        self.max = max
        self.min = min
        self.update(kwargs)

    max = CognitePropertyClassUtil.declare_property("max")
    min = CognitePropertyClassUtil.declare_property("min")


class AggregateResult(dict):
    def __init__(self, count=None, **kwargs):
        super().__init__(count=count, **kwargs)
        self.count = count


class AggregateUniqueValuesResult(AggregateResult):
    def __init__(self, count=None, value=None, **kwargs):
        super().__init__(count=count, value=value, **kwargs)
        self.value = value


class AggregateBucketResult(AggregateResult):
    def __init__(self, count=None, value=None, **kwargs):
        super().__init__(count=count, value=value, **kwargs)
        self.value = value


class Geometry(dict):
    _VALID_TYPES = frozenset({"Point", "MultiPoint", "LineString", "MultiLineString", "Polygon", "MultiPolygon"})

    def __init__(self, type, coordinates):
        if type not in self._VALID_TYPES:
            raise ValueError(f"type must be one of {self._VALID_TYPES}")
        self.type = type
        self.coordinates = coordinates

    type = CognitePropertyClassUtil.declare_property("type")
    coordinates = CognitePropertyClassUtil.declare_property("coordinates")

    @classmethod
    def _load(cls, raw_geometry):
        return cls(type=raw_geometry["type"], coordinates=raw_geometry["coordinates"])

    def dump(self, camel_case=False):
        return convert_all_keys_to_camel_case(self) if camel_case else dict(self)


class GeometryFilter(dict):
    _VALID_TYPES = frozenset({"Point", "LineString", "MultiLineString", "Polygon", "MultiPolygon"})

    def __init__(self, type, coordinates):
        if type not in self._VALID_TYPES:
            raise ValueError(f"type must be one of {self._VALID_TYPES}")
        self.type = type
        self.coordinates = coordinates

    type = CognitePropertyClassUtil.declare_property("type")
    coordinates = CognitePropertyClassUtil.declare_property("coordinates")


class GeoLocation(dict):
    _VALID_TYPES = frozenset({"Feature"})

    def __init__(self, type, geometry, properties=None):
        if type not in self._VALID_TYPES:
            raise ValueError("Only the 'Feature' type is supported.")
        self.type = type
        self.geometry = geometry
        self.properties = properties

    type = CognitePropertyClassUtil.declare_property("type")
    geometry = CognitePropertyClassUtil.declare_property("geometry")
    properties = CognitePropertyClassUtil.declare_property("properties")

    @classmethod
    def _load(cls, raw_geo_location=None, **kwargs):
        raw_geo_location = cast(
            Dict[(str, Any)],
            handle_deprecated_camel_case_argument(raw_geo_location, "raw_geoLocation", "_load", kwargs),
        )
        return cls(
            type=raw_geo_location.get("type", "Feature"),
            geometry=raw_geo_location["geometry"],
            properties=raw_geo_location.get("properties"),
        )

    def dump(self, camel_case=False):
        return convert_all_keys_to_camel_case(self) if camel_case else dict(self)


class GeoLocationFilter(dict):
    def __init__(self, relation, shape):
        self.relation = relation
        self.shape = shape

    relation = CognitePropertyClassUtil.declare_property("relation")
    shape = CognitePropertyClassUtil.declare_property("shape")

    @classmethod
    def _load(cls, raw_geo_location_filter=None, **kwargs):
        raw_geo_location_filter = cast(
            Dict[(str, Any)],
            handle_deprecated_camel_case_argument(raw_geo_location_filter, "raw_geoLocation_filter", "_load", kwargs),
        )
        return cls(relation=raw_geo_location_filter["relation"], shape=raw_geo_location_filter["shape"])

    def dump(self, camel_case=False):
        return convert_all_keys_to_camel_case(self) if camel_case else dict(self)
