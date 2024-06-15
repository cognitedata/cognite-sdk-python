from __future__ import annotations

from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes._base import CogniteObject, UnknownCogniteObject
from cognite.client.data_classes.iam import ClientCredentials
from cognite.client.utils._auxiliary import basic_obj_dump
from cognite.client.utils._text import iterable_to_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class TransformationDestination(CogniteObject):
    """TransformationDestination has static methods to define the target resource type of a transformation

    Args:
        type (str | None): Used as data type identifier on transformation creation/retrieval.
    """

    def __init__(self, type: str | None = None) -> None:
        self.type = type

    def __hash__(self) -> int:
        return hash(self.type)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and hash(other) == hash(self)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        ret = basic_obj_dump(self, camel_case)

        needs_dump = set(iterable_to_case(("view", "edge_type", "data_model"), camel_case))
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
            RawTable: TransformationDestination pointing to the target table
        """
        return RawTable(database=database, table=table)

    @staticmethod
    def sequence_rows(external_id: str = "") -> SequenceRowsDestination:
        """To be used when the transformation is meant to produce sequence rows.

        Args:
            external_id (str): Sequence external id.

        Returns:
            SequenceRowsDestination: TransformationDestination pointing to the target sequence rows
        """
        return SequenceRowsDestination(external_id=external_id)

    @staticmethod
    def nodes(view: ViewInfo | None = None, instance_space: str | None = None) -> Nodes:
        """

        Args:
            view (ViewInfo | None): information of the view.
            instance_space (str | None): space id of the instance.
        Returns:
            Nodes: pointing to the target flexible data model.
        """
        return Nodes(view=view, instance_space=instance_space)

    @staticmethod
    def edges(
        view: ViewInfo | None = None,
        instance_space: str | None = None,
        edge_type: EdgeType | None = None,
    ) -> Edges:
        """

        Args:
            view (ViewInfo | None): information of the view.
            instance_space (str | None): space id of the instance.
            edge_type (EdgeType | None): information about the type of the edge
        Returns:
            Edges: pointing to the target flexible data model.
        """
        return Edges(view=view, instance_space=instance_space, edge_type=edge_type)

    @staticmethod
    def instances(data_model: DataModelInfo | None = None, instance_space: str | None = None) -> Instances:
        """
        Args:
            data_model (DataModelInfo | None): information of the Data Model.
            instance_space (str | None): space id of the instance.
        Returns:
            Instances: pointing to the target centric data model.
        """
        return Instances(data_model=data_model, instance_space=instance_space)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> TransformationDestination:
        type_ = resource.get("type")
        if type_ is None:
            return UnknownCogniteObject._load(resource)  # type: ignore[return-value]

        if type_ == "raw":
            return RawTable._load(resource)
        elif type_ == "sequence_rows":
            return SequenceRowsDestination._load(resource)
        elif type_ == "nodes":
            return Nodes._load(resource)
        elif type_ == "edges":
            return Edges._load(resource)
        elif type_ == "instances":
            return Instances._load(resource)
        return TransformationDestination(type_)


class RawTable(TransformationDestination):
    def __init__(self, database: str | None = None, table: str | None = None) -> None:
        super().__init__(type="raw")
        self.database = database
        self.table = table

    def __hash__(self) -> int:
        return hash((self.type, self.database, self.table))

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(database=resource["database"], table=resource["table"])


class SequenceRowsDestination(TransformationDestination):
    def __init__(self, external_id: str | None = None) -> None:
        super().__init__(type="sequence_rows")
        self.external_id = external_id

    def __hash__(self) -> int:
        return hash((self.type, self.external_id))

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(external_id=resource["externalId"])


class ViewInfo(CogniteObject):
    def __init__(self, space: str, external_id: str, version: str) -> None:
        self.space = space
        self.external_id = external_id
        self.version = version

    def __hash__(self) -> int:
        return hash((self.space, self.external_id, self.version))

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> ViewInfo:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            version=resource["version"],
        )


class EdgeType(CogniteObject):
    def __init__(self, space: str, external_id: str) -> None:
        self.space = space
        self.external_id = external_id

    def __hash__(self) -> int:
        return hash((self.space, self.external_id))

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> EdgeType:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return basic_obj_dump(self, camel_case)


class DataModelInfo(CogniteObject):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: str,
        destination_type: str,
        destination_relationship_from_type: str | None = None,
    ) -> None:
        self.space = space
        self.external_id = external_id
        self.version = version
        self.destination_type = destination_type
        self.destination_relationship_from_type = destination_relationship_from_type

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            version=resource["version"],
            destination_type=resource["destinationType"],
            destination_relationship_from_type=resource.get("destinationRelationshipFromType"),
        )


class Nodes(TransformationDestination):
    def __init__(
        self,
        view: ViewInfo | None = None,
        instance_space: str | None = None,
    ) -> None:
        super().__init__(type="nodes")
        self.view = view
        self.instance_space = instance_space

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            view=ViewInfo._load(resource["view"]) if resource.get("view") is not None else None,
            instance_space=resource.get("instanceSpace"),
        )


class Edges(TransformationDestination):
    def __init__(
        self,
        view: ViewInfo | None = None,
        instance_space: str | None = None,
        edge_type: EdgeType | None = None,
    ) -> None:
        super().__init__(type="edges")
        self.view = view
        self.instance_space = instance_space
        self.edge_type = edge_type

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            view=ViewInfo._load(resource["view"]) if resource.get("view") is not None else None,
            instance_space=resource.get("instanceSpace"),
            edge_type=EdgeType._load(resource["edgeType"]) if resource.get("edgeType") is not None else None,
        )


class Instances(TransformationDestination):
    def __init__(
        self,
        data_model: DataModelInfo | None = None,
        instance_space: str | None = None,
    ) -> None:
        super().__init__(type="instances")
        self.data_model = data_model
        self.instance_space = instance_space

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            data_model=DataModelInfo._load(resource["dataModel"]) if resource.get("dataModel") is not None else None,
            instance_space=resource.get("instanceSpace"),
        )


class OidcCredentials:
    """
    Class that represents OpenID Connect (OIDC) credentials used to authenticate towards Cognite Data Fusion (CDF).

    Note:
        Is currently only used to specify inputs to TransformationsAPI like source_oidc_credentials and
        destination_oidc_credentials.

    Args:
        client_id (str): Your application's client id.
        client_secret (str): Your application's client secret
        scopes (str | list[str]): A list of scopes or a comma-separated string (for backwards compatibility).
        token_uri (str): OAuth token url
        cdf_project_name (str): Name of CDF project
        audience (str | None): Audience (optional)
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        scopes: str | list[str],
        token_uri: str,
        cdf_project_name: str,
        audience: str | None = None,
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_uri = token_uri
        self.audience = audience
        self.cdf_project_name = cdf_project_name
        self.scopes = self._verify_scopes(scopes)

    @staticmethod
    def _verify_scopes(scopes: str | list[str]) -> str:
        if isinstance(scopes, str):
            return scopes
        elif isinstance(scopes, list):
            return ",".join(scopes)
        raise TypeError(f"scopes must be provided as a comma-separated string or list, not {type(scopes)}")

    def as_credential_provider(self) -> OAuthClientCredentials:
        return OAuthClientCredentials(
            token_url=self.token_uri,
            client_id=self.client_id,
            client_secret=self.client_secret,
            scopes=self.scopes.split(","),
            audience=self.audience,
        )

    def as_client_credentials(self) -> ClientCredentials:
        return ClientCredentials(client_id=self.client_id, client_secret=self.client_secret)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to True.

        Returns:
            dict[str, Any]: A dictionary representation of the instance.
        """
        return basic_obj_dump(self, camel_case)

    @classmethod
    def load(cls, data: dict[str, Any]) -> Self:
        """Load data into the instance.

        Args:
            data (dict[str, Any]): A dictionary representation of the instance.
        Returns:
            Self: No description.
        """
        return cls(
            client_id=data["clientId"],
            client_secret=data["clientSecret"],
            scopes=data["scopes"],
            token_uri=data["tokenUri"],
            cdf_project_name=data["cdfProjectName"],
            audience=data.get("audience"),
        )


class NonceCredentials:
    def __init__(
        self,
        session_id: int,
        nonce: str,
        cdf_project_name: str,
    ) -> None:
        self.session_id = session_id
        self.nonce = nonce
        self.cdf_project_name = cdf_project_name

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to True.

        Returns:
            dict[str, Any]: A dictionary representation of the instance.
        """
        return basic_obj_dump(self, camel_case)

    @classmethod
    def load(cls, data: dict[str, Any]) -> NonceCredentials:
        """Load data into the instance.

        Args:
            data (dict[str, Any]): A dictionary representation of the instance.
        Returns:
            NonceCredentials: No description.
        """
        return cls(
            session_id=data["sessionId"],
            nonce=data["nonce"],
            cdf_project_name=data["cdfProjectName"],
        )


class TransformationBlockedInfo:
    """Information about the reason why and when a transformation is blocked.

    Args:
        reason (str): Reason why the transformation is blocked.
        created_time (int): Timestamp when the transformation was blocked.
    """

    def __init__(self, reason: str, created_time: int) -> None:
        self.reason = reason
        self.created_time = created_time

    @classmethod
    def load(cls, resource: dict[str, Any]) -> TransformationBlockedInfo:
        return cls(reason=resource["reason"], created_time=resource["createdTime"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return basic_obj_dump(self, camel_case)
