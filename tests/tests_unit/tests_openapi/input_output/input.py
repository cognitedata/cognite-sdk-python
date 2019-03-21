class CogniteResource:
    pass


# GenClass: GetFieldValuesDTO
class Field(CogniteResource):
    pass
    # GenStop


# GenClass: AssetV2
class Asset(CogniteResource):
    # GenStop
    def to_pandas(self):
        pass


class ApiClient:
    # GenMethod: getAssets -> Asset
    def get(self):
        pass
        # GenStop

    # GenMethod: postAssets -> Union[Asset, List[Asset]]
    def post(self, nana):
        pass
        # GenStop
