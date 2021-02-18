from typing import Dict, List

from cognite.client.data_classes._base import *
from cognite.client.data_classes.labels import Label, LabelFilter
from cognite.client.data_classes.shared import TimestampRange


class Geometry(dict):
    """Represents the points, curves and surfaces in the coordinate space.

    Args:
        type (str): The geometry type. One of 'Point', 'MultiPoint', 'LineString', 'MultiLineString', 'Polygon', or 'MultiPolygon'.
        coordinates (List): An array of the coordinates of the geometry. The structure of the elements in this array is determined by the type of geometry.

            Point:
                Coordinates of a point in 2D space, described as an array of 2 numbers.

                Example: `[4.306640625, 60.205710352530346]`


            LineString:
                Coordinates of a line described by a list of two or more points.
                Each point is defined as a pair of two numbers in an array, representing coordinates of a point in 2D space.

                Example: `[[30, 10], [10, 30], [40, 40]]`


            Polygon:
                List of one or more linear rings representing a shape.
                A linear ring is the boundary of a surface or the boundary of a hole in a surface. It is defined as a list consisting of 4 or more Points, where the first and last Point is equivalent.
                Each Point is defined as an array of 2 numbers, representing coordinates of a point in 2D space.

                Example: `[[[35, 10], [45, 45], [15, 40], [10, 20], [35, 10]], [[20, 30], [35, 35], [30, 20], [20, 30]]]`
                type: array

            MultiPoint:
                List of Points. Each Point is defined as an array of 2 numbers, representing coordinates of a point in 2D space.

                Example: `[[35, 10], [45, 45]]`

            MultiLineString:
                    List of lines where each line (LineString) is defined as a list of two or more points.
                    Each point is defined as a pair of two numbers in an array, representing coordinates of a point in 2D space.

                    Example: `[[[30, 10], [10, 30]], [[35, 10], [10, 30], [40, 40]]]`

            MultiPolygon:
                List of multiple polygons.

                Each polygon is defined as a list of one or more linear rings representing a shape.

                A linear ring is the boundary of a surface or the boundary of a hole in a surface. It is defined as a list consisting of 4 or more Points, where the first and last Point is equivalent.

                Each Point is defined as an array of 2 numbers, representing coordinates of a point in 2D space.

                Example: `[[[[30, 20], [45, 40], [10, 40], [30, 20]]], [[[15, 5], [40, 10], [10, 20], [5, 10], [15, 5]]]]`



    """

    def __init__(self, type: str, coordinates: List):
        valid_types = ["Point", "MultiPoint", "LineString", "MultiLineString", "Polygon", "MultiPolygon"]
        if type not in valid_types:
            raise ValueError("type must be one of " + str(valid_types))
        self.type = type
        self.coordinates = coordinates

    type = CognitePropertyClassUtil.declare_property("type")
    coordinates = CognitePropertyClassUtil.declare_property("coordinates")

    @classmethod
    def _load(self, raw_geometry: Dict[str, Any]):
        return Geometry(type=raw_geometry["type"], coordinates=raw_geometry["coordinates"])

    def dump(self, camel_case: bool = False):
        dump_key = lambda key: key if not camel_case else utils._auxiliary.to_camel_case(key)
        return {dump_key(key): value for key, value in self.items()}


class GeometryFilter(dict):
    """Represents the points, curves and surfaces in the coordinate space.

    Args: type (str): The geometry type. One of 'Point', 'LineString', 'MultiLineString', 'Polygon', or 'MultiPolygon'.
          coordinates (List): An array of the coordinates of the geometry. The structure of the elements in this array is determined by the type of geometry.
    """

    def __init__(self, type: str, coordinates: List):
        valid_types = ["Point", "LineString", "MultiLineString", "Polygon", "MultiPolygon"]
        if type not in valid_types:
            raise ValueError("type must be one of " + str(valid_types))
        self.type = type
        self.coordinates = coordinates

    type = CognitePropertyClassUtil.declare_property("type")
    coordinates = CognitePropertyClassUtil.declare_property("coordinates")


class GeoLocation(dict):
    """A GeoLocation object conforming to the GeoJSON spec.

    Args: type (str): The GeoJSON type. Currently only 'Feature' is supported.
          geometry (object): The geometry type. One of 'Point', 'MultiPoint, 'LineString', 'MultiLineString', 'Polygon', or 'MultiPolygon'.
          properties (object): Optional additional properties in a String key -> Object value format.
    """

    def __init__(self, type: str, geometry: Geometry, properties: dict = None):
        if type != "Feature":
            raise ValueError("Only the 'Feature' type is supported.")
        self.type = type
        self.geometry = geometry
        self.properties = properties

    type = CognitePropertyClassUtil.declare_property("type")
    geometry = CognitePropertyClassUtil.declare_property("geometry")
    properties = CognitePropertyClassUtil.declare_property("properties")

    @classmethod
    def _load(self, raw_geoLocation: Dict[str, Any]):
        return GeoLocation(
            type=raw_geoLocation.get("type", "Feature"),
            geometry=raw_geoLocation["geometry"],
            properties=raw_geoLocation.get("properties"),
        )

    def dump(self, camel_case: bool = False):
        dump_key = lambda key: key if not camel_case else utils._auxiliary.to_camel_case(key)
        return {dump_key(key): value for key, value in self.items()}


class GeoLocationFilter(dict):
    """Return only the file matching the specified geographic relation.

    Args: relation (str): One of the following supported queries: INTERSECTS, DISJOINT, WITHIN.
          shape (GeometryFilter): Represents the points, curves and surfaces in the coordinate space.
    """

    def __init__(self, relation: str, shape: GeometryFilter):
        self.relation = relation
        self.shape = shape

    relation = CognitePropertyClassUtil.declare_property("relation")
    shape = CognitePropertyClassUtil.declare_property("shape")

    @classmethod
    def _load(self, raw_geoLocation_filter: Dict[str, Any]):
        return GeoLocationFilter(relation=raw_geoLocation_filter["relation"], shape=raw_geoLocation_filter["shape"])

    def dump(self, camel_case: bool = False):
        dump_key = lambda key: key if not camel_case else utils._auxiliary.to_camel_case(key)
        return {dump_key(key): value for key, value in self.items()}


class FileMetadata(CogniteResource):
    """No description.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): Name of the file.
        source (str): The source of the file.
        mime_type (str): File type. E.g. text/plain, application/pdf, ..
        metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        directory (str): Directory associated with the file. Must be an absolute, unix-style path.
        asset_ids (List[int]): No description.
        data_set_id (int): The dataSet Id for the item.
        labels (List[Label]): A list of the labels associated with this resource item.
        geo_location (GeoLocation): The geographic metadata of the file.
        source_created_time (int): The timestamp for when the file was originally created in the source system.
        source_modified_time (int): The timestamp for when the file was last modified in the source system.
        security_categories (List[int]): The security category IDs required to access this file.
        id (int): A server-generated ID for the object.
        uploaded (bool): Whether or not the actual file is uploaded.  This field is returned only by the API, it has no effect in a post body.
        uploaded_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str = None,
        name: str = None,
        source: str = None,
        mime_type: str = None,
        metadata: Dict[str, str] = None,
        directory: str = None,
        asset_ids: List[int] = None,
        data_set_id: int = None,
        labels: List[Label] = None,
        geo_location: GeoLocation = None,
        source_created_time: int = None,
        source_modified_time: int = None,
        security_categories: List[int] = None,
        id: int = None,
        uploaded: bool = None,
        uploaded_time: int = None,
        created_time: int = None,
        last_updated_time: int = None,
        cognite_client=None,
    ):
        if geo_location is not None and not isinstance(geo_location, GeoLocation):
            raise TypeError("FileMetadata.geo_location should be of type GeoLocation")
        self.external_id = external_id
        self.name = name
        self.directory = directory
        self.source = source
        self.mime_type = mime_type
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.data_set_id = data_set_id
        self.labels = Label._load_list(labels)
        self.geo_location = geo_location
        self.source_created_time = source_created_time
        self.source_modified_time = source_modified_time
        self.security_categories = security_categories
        self.id = id
        self.uploaded = uploaded
        self.uploaded_time = uploaded_time
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        instance = super(FileMetadata, cls)._load(resource, cognite_client)
        instance.labels = Label._load_list(instance.labels)
        if instance.geo_location is not None:
            instance.geo_location = GeoLocation._load(instance.geo_location)
        return instance


class FileMetadataFilter(CogniteFilter):
    """No description.

    Args:
        name (str): Name of the file.
        mime_type (str): File type. E.g. text/plain, application/pdf, ..
        metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        asset_ids (List[int]): Only include files that reference these specific asset IDs.
        asset_external_ids (List[str]): Only include files that reference these specific asset external IDs.
        root_asset_ids (List[Dict[str, Any]]): Only include files that have a related asset in a tree rooted at any of these root assetIds.
        data_set_ids (List[Dict[str, Any]]): Only include files that belong to these datasets.
        labels (LabelFilter): Return only the files matching the specified label(s).
        geo_location (GeoLocationFilter): Only include files matching the specified geographic relation.
        asset_subtree_ids (List[Dict[str, Any]]): Only include files that have a related asset in a subtree rooted at any of these assetIds (including the roots given). If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
        source (str): The source of this event.
        created_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        last_updated_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        uploaded_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        source_created_time (Dict[str, Any]): Filter for files where the sourceCreatedTime field has been set and is within the specified range.
        source_modified_time (Dict[str, Any]): Filter for files where the sourceModifiedTime field has been set and is within the specified range.
        external_id_prefix (str): Filter by this (case-sensitive) prefix for the external ID.
        directory_prefix (str): Filter by this (case-sensitive) prefix for the directory provided by the client.
        uploaded (bool): Whether or not the actual file is uploaded. This field is returned only by the API, it has no effect in a post body.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        name: str = None,
        mime_type: str = None,
        metadata: Dict[str, str] = None,
        asset_ids: List[int] = None,
        asset_external_ids: List[str] = None,
        root_asset_ids: List[Dict[str, Any]] = None,
        data_set_ids: List[Dict[str, Any]] = None,
        labels: LabelFilter = None,
        geo_location: GeoLocationFilter = None,
        asset_subtree_ids: List[Dict[str, Any]] = None,
        source: str = None,
        created_time: Union[Dict[str, Any], TimestampRange] = None,
        last_updated_time: Union[Dict[str, Any], TimestampRange] = None,
        uploaded_time: Union[Dict[str, Any], TimestampRange] = None,
        source_created_time: Dict[str, Any] = None,
        source_modified_time: Dict[str, Any] = None,
        external_id_prefix: str = None,
        directory_prefix: str = None,
        uploaded: bool = None,
        cognite_client=None,
    ):
        self.name = name
        self.mime_type = mime_type
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.asset_external_ids = asset_external_ids
        self.root_asset_ids = root_asset_ids
        self.data_set_ids = data_set_ids
        self.labels = labels
        self.geo_location = geo_location
        self.asset_subtree_ids = asset_subtree_ids
        self.source = source
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.uploaded_time = uploaded_time
        self.source_created_time = source_created_time
        self.source_modified_time = source_modified_time
        self.external_id_prefix = external_id_prefix
        self.directory_prefix = directory_prefix
        self.uploaded = uploaded
        self._cognite_client = cognite_client

        if labels is not None and not isinstance(labels, LabelFilter):
            raise TypeError("FileMetadataFilter.labels must be of type LabelFilter")
        if geo_location is not None and not isinstance(geo_location, GeoLocationFilter):
            raise TypeError("FileMetadata.geo_location should be of type GeoLocationFilter")

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        instance = super(FileMetadataFilter, cls)._load(resource, cognite_client)
        if isinstance(resource, Dict):
            if instance.created_time is not None:
                instance.created_time = TimestampRange(**instance.created_time)
            if instance.last_updated_time is not None:
                instance.last_updated_time = TimestampRange(**instance.last_updated_time)
            if instance.uploaded_time is not None:
                instance.uploaded_time = TimestampRange(**instance.uploaded_time)
            if instance.labels is not None:
                instance.labels = [Label._load(label) for label in instance.labels]
            if instance.geo_location is not None:
                instance.geo_location = GeoLocationFilter._load(**instance.geo_location)
        return instance

    def dump(self, camel_case: bool = False):
        result = super(FileMetadataFilter, self).dump(camel_case)
        if isinstance(self.labels, LabelFilter):
            result["labels"] = self.labels.dump(camel_case)
        if isinstance(self.geo_location, GeoLocationFilter):
            result["geoLocation"] = self.geo_location.dump(camel_case)
        return result


class FileMetadataUpdate(CogniteUpdate):
    """Changes will be applied to file.

    Args:
    """

    class _PrimitiveFileMetadataUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> "FileMetadataUpdate":
            return self._set(value)

    class _ObjectFileMetadataUpdate(CogniteObjectUpdate):
        def set(self, value: Dict) -> "FileMetadataUpdate":
            return self._set(value)

        def add(self, value: Dict) -> "FileMetadataUpdate":
            return self._add(value)

        def remove(self, value: List) -> "FileMetadataUpdate":
            return self._remove(value)

    class _ListFileMetadataUpdate(CogniteListUpdate):
        def set(self, value: List) -> "FileMetadataUpdate":
            return self._set(value)

        def add(self, value: List) -> "FileMetadataUpdate":
            return self._add(value)

        def remove(self, value: List) -> "FileMetadataUpdate":
            return self._remove(value)

    class _LabelFileMetadataUpdate(CogniteLabelUpdate):
        def add(self, value: Union[str, List[str]]) -> "FileMetadataUpdate":
            return self._add(value)

        def remove(self, value: Union[str, List[str]]) -> "FileMetadataUpdate":
            return self._remove(value)

    @property
    def external_id(self):
        return FileMetadataUpdate._PrimitiveFileMetadataUpdate(self, "externalId")

    @property
    def directory(self):
        return FileMetadataUpdate._PrimitiveFileMetadataUpdate(self, "directory")

    @property
    def source(self):
        return FileMetadataUpdate._PrimitiveFileMetadataUpdate(self, "source")

    @property
    def mime_type(self):
        return FileMetadataUpdate._PrimitiveFileMetadataUpdate(self, "mimeType")

    @property
    def metadata(self):
        return FileMetadataUpdate._ObjectFileMetadataUpdate(self, "metadata")

    @property
    def asset_ids(self):
        return FileMetadataUpdate._ListFileMetadataUpdate(self, "assetIds")

    @property
    def source_created_time(self):
        return FileMetadataUpdate._PrimitiveFileMetadataUpdate(self, "sourceCreatedTime")

    @property
    def source_modified_time(self):
        return FileMetadataUpdate._PrimitiveFileMetadataUpdate(self, "sourceModifiedTime")

    @property
    def data_set_id(self):
        return FileMetadataUpdate._PrimitiveFileMetadataUpdate(self, "dataSetId")

    @property
    def labels(self):
        return FileMetadataUpdate._LabelFileMetadataUpdate(self, "labels")

    @property
    def geoLocation(self):
        return FileMetadataUpdate._PrimitiveFileMetadataUpdate(self, "geoLocation")

    @property
    def security_categories(self):
        return FileMetadataUpdate._ListFileMetadataUpdate(self, "securityCategories")


class FileAggregate(dict):
    """Aggregation results for files

    Args:
        count (int): Number of filtered items included in aggregation
    """

    def __init__(self, count: int = None, **kwargs):
        self.count = count
        self.update(kwargs)

    count = CognitePropertyClassUtil.declare_property("count")


class FileMetadataList(CogniteResourceList):
    _RESOURCE = FileMetadata
    _UPDATE = FileMetadataUpdate
