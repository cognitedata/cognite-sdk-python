import pandas as pd
from abc import ABC, abstractmethod

# Author: Erlend Vollset
class CogniteDataObject(ABC):
    def __init__(self, internal_representation):
        self.internal_representation = internal_representation
    @abstractmethod
    def to_pandas(self):
        pass
    @abstractmethod
    def to_json(self):
        pass

    def to_ndarray(self):
        return self.to_pandas().values

# Author: Erlend Vollset
class TagMatchingObject(CogniteDataObject):
    def __init__(self, internal_representation):
        CogniteDataObject.__init__(self, internal_representation)

    def to_pandas(self):
        matches = []
        for tag in self.internal_representation['data']['items']:
            for match in tag['matches']:
                matches.append({
                    'tag': tag['tagId'],
                    'match': match['tagId'],
                    'score': match['score'],
                    'platform': match['platform']
                })
        if len(matches) > 0:
            return pd.DataFrame(matches)[['tag', 'match', 'platform', 'score']]
        else:
            return pd.DataFrame()

    def to_json(self):
        return self.internal_representation['data']['items']

    def to_list(self, first_matches_only=True):
        if self.to_pandas().empty:
            return []
        if first_matches_only:
            return self.to_pandas().sort_values(['score', 'match']).groupby(['tag']).first()['match'].tolist()
        return self.to_pandas().sort_values(['score', 'match'])['match'].tolist()


# Author: Erlend Vollset
class DatapointsObject(CogniteDataObject):
    def __init__(self, internal_representation):
        CogniteDataObject.__init__(self, internal_representation)

    def to_json(self):
        return self.internal_representation['data']['items'][0]

    def to_pandas(self):
        return pd.DataFrame(self.internal_representation['data']['items'][0]['datapoints'])


# Author: Erlend Vollset
class LatestDatapointObject(CogniteDataObject):
    def __init__(self, internal_representation):
        CogniteDataObject.__init__(self, internal_representation)

    def to_json(self):
        return self.internal_representation['data']['items'][0]

    def to_pandas(self):
        return pd.DataFrame([self.internal_representation['data']['items'][0]])

    def to_ndarray(self):
        return self.to_pandas().values[0]


# Author: Erlend Vollset
class SimilaritySearchObject(CogniteDataObject):
    def __init__(self, internal_representation):
        CogniteDataObject.__init__(self, internal_representation)

    def to_json(self):
        return self.internal_representation['data']['items']

    def to_pandas(self):
        if len(self.to_json()) > 1:
            return pd.DataFrame(self.internal_representation['data']['items'])
        return pd.DataFrame()



#
#   For now just a copy of SimilaritySearchObject
#   T.K
#
class AssetSearchObject(CogniteDataObject):
    def __init__(self, internal_representation):
        CogniteDataObject.__init__(self, internal_representation)

    def to_json(self):
        return self.internal_representation['data']['items']

    def to_pandas(self):
        if len(self.to_json()) > 1:
            return pd.DataFrame(self.internal_representation['data']['items'])
        return pd.DataFrame()