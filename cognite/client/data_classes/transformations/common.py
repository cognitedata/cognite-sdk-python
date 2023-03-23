from __future__ import annotations

import warnings
from typing import Any, Dict, Optional, Union

from cognite.client.utils._auxiliary import basic_obj_dump
from cognite.client.utils._text import convert_all_keys_to_snake_case, iterable_to_case


class TransformationDestination:
    """TransformationDestination has static methods to define the target resource type of a transformation

    Args:
        type (str): Used as data type identifier on transformation creation/retrieval.
    """

    def __init__(self, type: str = None):
        self.type = type

    def __hash__(self) -> int:
        return hash(self.type)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and hash(other) == hash(self)

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        ret = basic_obj_dump(self, camel_case)

        needs_dump = set(iterable_to_case(("view", "edge_type"), camel_case))
        for k in needs_dump.intersection(ret):
            if ret[k] is not None:
                ret[k] = ret[k].dump(camel_case=camel_case)
        return ret

    @staticmethod
    def assets() -> TransformationDestination:
        """To be used when the transformation is meant to produce assets."""
        return TransformationDestination(type="assets")

    @staticmethod
    def timeseries() -> TransformationDestination:
        """To be used when the transformation is meant to produce time series."""
        return TransformationDestination(type="timeseries")

    @staticmethod
    def asset_hierarchy() -> TransformationDestination:
        """To be used when the transformation is meant to produce asset hierarchies."""
        return TransformationDestination(type="asset_hierarchy")

    @staticmethod
    def events() -> TransformationDestination:
        """To be used when the transformation is meant to produce events."""
        return TransformationDestination(type="events")

    @staticmethod
    def datapoints() -> TransformationDestination:
        """To be used when the transformation is meant to produce numeric data points."""
        return TransformationDestination(type="datapoints")

    @staticmethod
    def string_datapoints() -> TransformationDestination:
        """To be used when the transformation is meant to produce string data points."""
        return TransformationDestination(type="string_datapoints")

    @staticmethod
    def sequences() -> TransformationDestination:
        """To be used when the transformation is meant to produce sequences."""
        return TransformationDestination(type="sequences")

    @staticmethod
    def files() -> TransformationDestination:
        """To be used when the transformation is meant to produce files."""
        return TransformationDestination(type="files")

    @staticmethod
    def labels() -> TransformationDestination:
        """To be used when the transformation is meant to produce labels."""
        return TransformationDestination(type="labels")

    @staticmethod
    def relationships() -> TransformationDestination:
        """To be used when the transformation is meant to produce relationships."""
        return TransformationDestination(type="relationships")

    @staticmethod
    def data_sets() -> TransformationDestination:
        """To be used when the transformation is meant to produce data sets."""
        return TransformationDestination(type="data_sets")

    @staticmethod
    def raw(database: str = "", table: str = "") -> RawTable:
        """To be used when the transformation is meant to produce raw table rows.

        Args:
            database (str): database name of the target raw table.
            table (str): name of the target raw table

        Returns:
            TransformationDestination pointing to the target table
        """
        return RawTable(database=database, table=table)

    @staticmethod
    def sequence_rows(external_id: str = "") -> SequenceRows:
        """To be used when the transformation is meant to produce sequence rows.

        Args:
            external_id (str): Sequence external id.

        Returns:
            TransformationDestination pointing to the target sequence rows
        """
        return SequenceRows(external_id=external_id)

    @staticmethod
    def data_model_instances(
        model_external_id: str = "", space_external_id: str = "", instance_space_external_id: str = ""
    ) -> DataModelInstances:
        """To be used when the transformation is meant to produce data model instances.
            Flexible Data Models resource type is on `beta` version currently.

        Args:
            model_external_id (str): external_id of the flexible data model.
            space_external_id (str): space external_id of the flexible data model.
            instance_space_external_id (str): space external_id of the flexible data model instance.
        Returns:
            TransformationDestination pointing to the target flexible data model.
        """
        return DataModelInstances(
            model_external_id=model_external_id,
            space_external_id=space_external_id,
            instance_space_external_id=instance_space_external_id,
        )

    @staticmethod
    def instance_nodes(view: Optional[ViewInfo] = None, instance_space: Optional[str] = None) -> InstanceNodes:
        """To be used when the transformation is meant to produce node's instances.
            Flexible Data Models resource type is on `beta` version currently.

        Args:
            view (ViewInfo): information of the view.
            instance_space (str): space id of the instance.
        Returns:
            InstanceNodes: pointing to the target flexible data model.
        """
        return InstanceNodes(view=view, instance_space=instance_space)

    @staticmethod
    def instance_edges(
        view: Optional[ViewInfo] = None,
        instance_space: Optional[str] = None,
        edge_type: Optional[EdgeType] = None,
    ) -> InstanceEdges:
        """To be used when the transformation is meant to produce edge's instances.
            Flexible Data Models resource type is on `beta` version currently.

        Args:
            view (ViewInfo): information of the view.
            instance_space (str): space id of the instance.
            edge_type (EdgeType): information about the type of the edge
        Returns:
            InstanceEdges: pointing to the target flexible data model.
        """
        return InstanceEdges(view=view, instance_space=instance_space, edge_type=edge_type)


class RawTable(TransformationDestination):
    def __init__(self, database: str = None, table: str = None):
        super().__init__(type="raw")
        self.database = database
        self.table = table

    def __hash__(self) -> int:
        return hash((self.type, self.database, self.table))


class SequenceRows(TransformationDestination):
    def __init__(self, external_id: str = None):
        super().__init__(type="sequence_rows")
        self.external_id = external_id

    def __hash__(self) -> int:
        return hash((self.type, self.external_id))


class DataModelInstances(TransformationDestination):
    def __init__(
        self, model_external_id: str = None, space_external_id: str = None, instance_space_external_id: str = None
    ):
        warnings.warn(
            "Feature DataModelStorage is in beta and still in development. "
            "Breaking changes can happen in between patch versions.",
            stacklevel=2,
        )
        super().__init__(type="data_model_instances")
        self.model_external_id = model_external_id
        self.space_external_id = space_external_id
        self.instance_space_external_id = instance_space_external_id

    def __hash__(self) -> int:
        return hash((self.type, self.model_external_id, self.space_external_id, self.instance_space_external_id))


class ViewInfo:
    def __init__(self, space: str, external_id: str, version: str):
        self.space = space
        self.external_id = external_id
        self.version = version

    def __hash__(self) -> int:
        return hash((self.space, self.external_id, self.version))

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        return basic_obj_dump(self, camel_case)


class EdgeType:
    def __init__(self, space: str, external_id: str):
        self.space = space
        self.external_id = external_id

    def __hash__(self) -> int:
        return hash((self.space, self.external_id))

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        return basic_obj_dump(self, camel_case)


class InstanceNodes(TransformationDestination):
    def __init__(
        self,
        view: Optional[ViewInfo] = None,
        instance_space: Optional[str] = None,
    ):
        warnings.warn(
            "Feature DataModelStorage is in beta and still in development. "
            "Breaking changes can happen in between patch versions.",
            stacklevel=2,
        )
        super().__init__(type="nodes")
        self.view = view
        self.instance_space = instance_space

    @classmethod
    def _load(cls, resource: Dict[str, Any]) -> InstanceNodes:
        inst = cls(**resource)
        if isinstance(inst.view, dict):
            inst.view = ViewInfo(**convert_all_keys_to_snake_case(inst.view))
        return inst


class InstanceEdges(TransformationDestination):
    def __init__(
        self,
        view: Optional[ViewInfo] = None,
        instance_space: Optional[str] = None,
        edge_type: Optional[EdgeType] = None,
    ):
        warnings.warn(
            "Feature DataModelStorage is in beta and still in development. "
            "Breaking changes can happen in between patch versions.",
            stacklevel=2,
        )
        super().__init__(type="edges")
        self.view = view
        self.instance_space = instance_space
        self.edge_type = edge_type

    @classmethod
    def _load(cls, resource: Dict[str, Any]) -> InstanceEdges:
        inst = cls(**resource)
        if isinstance(inst.view, dict):
            inst.view = ViewInfo(**convert_all_keys_to_snake_case(inst.view))
        if isinstance(inst.edge_type, dict):
            inst.edge_type = EdgeType(**convert_all_keys_to_snake_case(inst.edge_type))
        return inst


class OidcCredentials:
    def __init__(
        self,
        client_id: str = None,
        client_secret: str = None,
        scopes: str = None,
        token_uri: str = None,
        audience: str = None,
        cdf_project_name: str = None,
    ):

        self.client_id = client_id
        self.client_secret = client_secret
        self.scopes = scopes
        self.token_uri = token_uri
        self.audience = audience
        self.cdf_project_name = cdf_project_name

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            Dict[str, Any]: A dictionary representation of the instance.
        """
        return basic_obj_dump(self, camel_case)


class NonceCredentials:
    def __init__(
        self,
        session_id: int,
        nonce: str,
        cdf_project_name: str,
    ):
        self.session_id = session_id
        self.nonce = nonce
        self.cdf_project_name = cdf_project_name

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            Dict[str, Any]: A dictionary representation of the instance.
        """
        return basic_obj_dump(self, camel_case)


class TransformationBlockedInfo:
    """Information about the reason why and when a transformation is blocked.

    Args:
        reason (str): Reason why the transformation is blocked.
        created_time (Optional[int]): Timestamp when the transformation was blocked.
    """

    def __init__(self, reason: str = None, created_time: Optional[int] = None):
        self.reason = reason
        self.created_time = created_time


def _load_destination_dct(
    dct: Dict[str, Any]
) -> Union[RawTable, DataModelInstances, InstanceNodes, InstanceEdges, SequenceRows, TransformationDestination]:
    """Helper function to load destination from dictionary"""
    snake_dict = convert_all_keys_to_snake_case(dct)
    destination_type = snake_dict.pop("type")
    simple = {
        "raw": RawTable,
        "data_model_instances": DataModelInstances,
        "sequence_rows": SequenceRows,
    }
    if destination_type in simple:
        return simple[destination_type](**snake_dict)

    nested: Dict[str, Union[type[InstanceNodes], type[InstanceEdges]]] = {
        "nodes": InstanceNodes,
        "edges": InstanceEdges,
    }
    if destination_type in nested:
        return nested[destination_type]._load(snake_dict)

    return TransformationDestination(destination_type)
