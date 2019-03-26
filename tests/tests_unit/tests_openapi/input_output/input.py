class CogniteResource:
    pass


class CogniteUpdate:
    pass


class CogniteFilter:
    pass


# GenClass: Asset, AssetReferences
class Asset(CogniteResource):
    # GenStop
    def to_pandas(self):
        pass


# GenUpdateClass: AssetChange
class AssetUpdate(CogniteUpdate):
    pass
    # GenStop


# GenClass: AssetFilter.filter
class AssetFilter(CogniteFilter):
    pass
    # GenStop
