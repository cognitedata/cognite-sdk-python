from cognite.client.data_classes._base import *


class TransformationDestination:
    """TransformationDestination has static methods to define the target resource type of a transformation

    Args:
        type (str): Used as data type identifier on transformation creation/retrieval.
    """

    def __init__(self, type: str = None):
        self.type = type

    def __hash__(self):
        return hash(self.type)

    def __eq__(self, obj):
        return isinstance(obj, TransformationDestination) and hash(obj) == hash(self)

    @staticmethod
    def assets():
        """To be used when the transformation is meant to produce assets."""
        return TransformationDestination(type="assets")

    @staticmethod
    def timeseries():
        """To be used when the transformation is meant to produce time series."""
        return TransformationDestination(type="timeseries")

    @staticmethod
    def asset_hierarchy():
        """To be used when the transformation is meant to produce asset hierarchies."""
        return TransformationDestination(type="asset_hierarchy")

    @staticmethod
    def events():
        """To be used when the transformation is meant to produce events."""
        return TransformationDestination(type="events")

    @staticmethod
    def datapoints():
        """To be used when the transformation is meant to produce numeric data points."""
        return TransformationDestination(type="datapoints")

    @staticmethod
    def string_datapoints():
        """To be used when the transformation is meant to produce string data points."""
        return TransformationDestination(type="string_datapoints")

    @staticmethod
    def sequences():
        """To be used when the transformation is meant to produce sequences."""
        return TransformationDestination(type="sequences")

    @staticmethod
    def files():
        """To be used when the transformation is meant to produce files."""
        return TransformationDestination(type="files")

    @staticmethod
    def labels():
        """To be used when the transformation is meant to produce labels."""
        return TransformationDestination(type="labels")

    @staticmethod
    def relationships():
        """To be used when the transformation is meant to produce relationships."""
        return TransformationDestination(type="relationships")

    @staticmethod
    def data_sets():
        """To be used when the transformation is meant to produce data sets."""
        return TransformationDestination(type="data_sets")

    @staticmethod
    def raw(database: str = "", table: str = ""):
        """To be used when the transformation is meant to produce raw table rows.

        Args:
            database (str): database name of the target raw table.
            table (str): name of the target raw table

        Returns:
            TransformationDestination pointing to the target table
        """
        return RawTable(database=database, table=table)


class RawTable(TransformationDestination):
    def __init__(self, database: str = None, table: str = None):
        super().__init__(type="raw")
        self.database = database
        self.table = table

    def __hash__(self):
        return hash((self.type, self.database, self.table))

    def __eq__(self, obj):
        return isinstance(obj, RawTable) and hash(obj) == hash(self)


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


class TransformationBlockedInfo:
    def __init__(self, reason: str = None, created_time: Optional[int] = None):
        self.reason = reason
        self.created_time = created_time
