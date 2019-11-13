class CogniteResource:
    pass


class CogniteUpdate:
    pass


class CogniteFilter:
    pass


class CognitePrimitiveUpdate:
    pass


class CogniteObjectUpdate:
    pass


class CogniteListUpdate:
    pass


class CognitePropertyClassUtil:
    @staticmethod
    def declare_property(tmp):
        return None


class TimestampRange:
    pass


# GenPropertyClass: AggregateResultItem
class AggregateResultItem(dict):
    pass
    # GenStop


# GenClass: Asset
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


# GenRWClass: r=GetTimeSeriesMetadataDTO w=PostTimeSeriesMetadataDTO
class TimeSeries(CogniteResource):
    pass
    # GenStop

    def after(self):
        return None
