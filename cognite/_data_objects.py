import pandas as pd
from abc import ABC, abstractmethod

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

class DatapointsObject(CogniteDataObject):
    def __init__(self, internal_representation):
        CogniteDataObject.__init__(self, internal_representation)

    def to_json(self):
        return self.internal_representation['data']['items'][0]

    def to_pandas(self):
        return pd.DataFrame(self.internal_representation['data']['items'][0]['datapoints'])

class LatestDatapointObject(CogniteDataObject):
    def __init__(self, internal_representation):
        CogniteDataObject.__init__(self, internal_representation)

    def to_json(self):
        return self.internal_representation['data']['items'][0]

    def to_pandas(self):
        return pd.DataFrame([self.internal_representation['data']['items'][0]])

    def to_ndarray(self):
        return self.to_pandas().values[0]

class SimilaritySearchObject(CogniteDataObject):
    def __init__(self, internal_representation):
        CogniteDataObject.__init__(self, internal_representation)

    def to_json(self):
        return self.internal_representation['data']['items']

    def to_pandas(self):
        return pd.DataFrame([self.internal_representation['data']['items']])