from typing import Any, Dict, Optional

from cognite.client import utils


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
        return isinstance(other, TransformationDestination) and hash(other) == hash(self)

    @staticmethod
    def assets() -> "TransformationDestination":
        """To be used when the transformation is meant to produce assets."""
        return TransformationDestination(type="assets")

    @staticmethod
    def timeseries() -> "TransformationDestination":
        """To be used when the transformation is meant to produce time series."""
        return TransformationDestination(type="timeseries")

    @staticmethod
    def asset_hierarchy() -> "TransformationDestination":
        """To be used when the transformation is meant to produce asset hierarchies."""
        return TransformationDestination(type="asset_hierarchy")

    @staticmethod
    def events() -> "TransformationDestination":
        """To be used when the transformation is meant to produce events."""
        return TransformationDestination(type="events")

    @staticmethod
    def datapoints() -> "TransformationDestination":
        """To be used when the transformation is meant to produce numeric data points."""
        return TransformationDestination(type="datapoints")

    @staticmethod
    def string_datapoints() -> "TransformationDestination":
        """To be used when the transformation is meant to produce string data points."""
        return TransformationDestination(type="string_datapoints")

    @staticmethod
    def sequences() -> "TransformationDestination":
        """To be used when the transformation is meant to produce sequences."""
        return TransformationDestination(type="sequences")

    @staticmethod
    def files() -> "TransformationDestination":
        """To be used when the transformation is meant to produce files."""
        return TransformationDestination(type="files")

    @staticmethod
    def labels() -> "TransformationDestination":
        """To be used when the transformation is meant to produce labels."""
        return TransformationDestination(type="labels")

    @staticmethod
    def relationships() -> "TransformationDestination":
        """To be used when the transformation is meant to produce relationships."""
        return TransformationDestination(type="relationships")

    @staticmethod
    def data_sets() -> "TransformationDestination":
        """To be used when the transformation is meant to produce data sets."""
        return TransformationDestination(type="data_sets")

    @staticmethod
    def raw(database: str = "", table: str = "") -> "RawTable":
        """To be used when the transformation is meant to produce raw table rows.

        Args:
            database (str): database name of the target raw table.
            table (str): name of the target raw table

        Returns:
            TransformationDestination pointing to the target table
        """
        return RawTable(database=database, table=table)

    @staticmethod
    def sequence_rows(external_id: str = "") -> "SequenceRows":
        """To be used when the transformation is meant to produce sequence rows.

        Args:
            external_id (str): Sequence external id.

        Returns:
            TransformationDestination pointing to the target sequence rows
        """
        return SequenceRows(external_id=external_id)


class RawTable(TransformationDestination):
    def __init__(self, database: str = None, table: str = None):
        super().__init__(type="raw")
        self.database = database
        self.table = table

    def __hash__(self) -> int:
        return hash((self.type, self.database, self.table))

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, RawTable) and hash(other) == hash(self)


class SequenceRows(TransformationDestination):
    def __init__(self, external_id: str = None):
        super().__init__(type="sequence_rows")
        self.external_id = external_id

    def __hash__(self) -> int:
        return hash((self.type, self.external_id))

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, SequenceRows) and hash(other) == hash(self)

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        ret = {"external_id": self.external_id, "type": self.type}
        if camel_case:
            return {utils._auxiliary.to_camel_case(key): value for key, value in ret.items()}
        return ret


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
        ret = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scopes": self.scopes,
            "token_uri": self.token_uri,
            "audience": self.audience,
            "cdf_project_name": self.cdf_project_name,
        }
        if camel_case:
            return {utils._auxiliary.to_camel_case(key): value for key, value in ret.items()}
        return ret


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
        ret = self.__dict__

        if camel_case:
            return {utils._auxiliary.to_camel_case(key): value for key, value in ret.items()}
        return ret


class TransformationBlockedInfo:
    def __init__(self, reason: str = None, created_time: Optional[int] = None):
        self.reason = reason
        self.created_time = created_time
