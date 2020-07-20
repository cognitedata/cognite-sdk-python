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


class CogniteLabelUpdate:
    pass


class CognitePropertyClassUtil:
    @staticmethod
    def declare_property(tmp):
        return None


class TimestampRange:
    pass


class EndTimeFilter:
    pass


class LabelFilter:
    pass

# GenClass: Event
class Event(CogniteResource):
# GenStop

# GenClass: EventFilter
class EventFilter(CogniteFilter):
# GenStop

# GenPropertyClass: AggregateResultItem
class AggregateResultItem(dict):
    pass
    # GenStop


# GenUpdateClass: AssetChange
class AssetUpdate(CogniteUpdate):
    pass
    # GenStop
