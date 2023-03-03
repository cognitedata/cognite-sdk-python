import warnings

from cognite.client.utils._auxiliary import basic_obj_dump, convert_all_keys_to_snake_case


class TransformationDestination:
    def __init__(self, type=None):
        self.type = type

    def __hash__(self):
        return hash(self.type)

    def __eq__(self, other):
        return isinstance(other, type(self)) and (hash(other) == hash(self))

    def dump(self, camel_case=False):
        return basic_obj_dump(self, camel_case)

    @staticmethod
    def assets():
        return TransformationDestination(type="assets")

    @staticmethod
    def timeseries():
        return TransformationDestination(type="timeseries")

    @staticmethod
    def asset_hierarchy():
        return TransformationDestination(type="asset_hierarchy")

    @staticmethod
    def events():
        return TransformationDestination(type="events")

    @staticmethod
    def datapoints():
        return TransformationDestination(type="datapoints")

    @staticmethod
    def string_datapoints():
        return TransformationDestination(type="string_datapoints")

    @staticmethod
    def sequences():
        return TransformationDestination(type="sequences")

    @staticmethod
    def files():
        return TransformationDestination(type="files")

    @staticmethod
    def labels():
        return TransformationDestination(type="labels")

    @staticmethod
    def relationships():
        return TransformationDestination(type="relationships")

    @staticmethod
    def data_sets():
        return TransformationDestination(type="data_sets")

    @staticmethod
    def raw(database="", table=""):
        return RawTable(database=database, table=table)

    @staticmethod
    def sequence_rows(external_id=""):
        return SequenceRows(external_id=external_id)

    @staticmethod
    def data_model_instances(model_external_id="", space_external_id="", instance_space_external_id=""):
        return DataModelInstances(
            model_external_id=model_external_id,
            space_external_id=space_external_id,
            instance_space_external_id=instance_space_external_id,
        )

    @staticmethod
    def instances(view_external_id="", view_version="", view_space_external_id="", instance_space_external_id=""):
        return Instances(
            view_external_id=view_external_id,
            view_version=view_version,
            view_space_external_id=view_space_external_id,
            instance_space_external_id=instance_space_external_id,
        )


class RawTable(TransformationDestination):
    def __init__(self, database=None, table=None):
        super().__init__(type="raw")
        self.database = database
        self.table = table

    def __hash__(self):
        return hash((self.type, self.database, self.table))


class SequenceRows(TransformationDestination):
    def __init__(self, external_id=None):
        super().__init__(type="sequence_rows")
        self.external_id = external_id

    def __hash__(self):
        return hash((self.type, self.external_id))


class DataModelInstances(TransformationDestination):
    def __init__(self, model_external_id=None, space_external_id=None, instance_space_external_id=None):
        warnings.warn(
            "Feature DataModelStorage is in beta and still in development. Breaking changes can happen in between patch versions.",
            stacklevel=2,
        )
        super().__init__(type="data_model_instances")
        self.model_external_id = model_external_id
        self.space_external_id = space_external_id
        self.instance_space_external_id = instance_space_external_id

    def __hash__(self):
        return hash((self.type, self.model_external_id, self.space_external_id, self.instance_space_external_id))


class Instances(TransformationDestination):
    def __init__(
        self, view_external_id=None, view_version=None, view_space_external_id=None, instance_space_external_id=None
    ):
        warnings.warn(
            "Feature DataModelStorage is in beta and still in development. Breaking changes can happen in between patch versions.",
            stacklevel=2,
        )
        super().__init__(type="instances")
        self.view_external_id = view_external_id
        self.view_version = view_version
        self.view_space_external_id = view_space_external_id
        self.instance_space_external_id = instance_space_external_id

    def __hash__(self):
        return hash(
            (
                self.type,
                self.view_external_id,
                self.view_version,
                self.view_space_external_id,
                self.instance_space_external_id,
            )
        )


class OidcCredentials:
    def __init__(
        self, client_id=None, client_secret=None, scopes=None, token_uri=None, audience=None, cdf_project_name=None
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.scopes = scopes
        self.token_uri = token_uri
        self.audience = audience
        self.cdf_project_name = cdf_project_name

    def dump(self, camel_case=False):
        return basic_obj_dump(self, camel_case)


class NonceCredentials:
    def __init__(self, session_id, nonce, cdf_project_name):
        self.session_id = session_id
        self.nonce = nonce
        self.cdf_project_name = cdf_project_name

    def dump(self, camel_case=False):
        return basic_obj_dump(self, camel_case)


class TransformationBlockedInfo:
    def __init__(self, reason=None, created_time=None):
        self.reason = reason
        self.created_time = created_time


def _load_destination_dct(dct):
    snake_dict = convert_all_keys_to_snake_case(dct)
    destination_type = snake_dict.pop("type")
    try:
        dest_dct = {
            "raw": RawTable,
            "data_model_instances": DataModelInstances,
            "instances": Instances,
            "sequence_rows": SequenceRows,
        }
        return dest_dct[destination_type](**snake_dict)
    except KeyError:
        return TransformationDestination(destination_type)
