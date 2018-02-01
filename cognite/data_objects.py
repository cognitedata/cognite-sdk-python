# -*- coding: utf-8 -*-
'''Data Objects

This module contains data objects used to represent the data returned from the API. These objects have at least
the following output formats:

    * to_pandas():    Returns pandas dataframe
    * to_ndarray():   Numpy array
    * to_json():      Json format
'''
from abc import ABC, abstractmethod
import pandas as pd
import json

# Author: Erlend Vollset
class CogniteDataObject(ABC):
    '''Abstract Cognite Data Object

    This abstract class provides a skeleton for all data objects in this module. All data objects should inherit
    this class.
    '''
    def __init__(self, internal_representation):
        self.internal_representation = internal_representation
    @abstractmethod
    def to_pandas(self):
        '''Returns data as a pandas dataframe'''
        pass
    @abstractmethod
    def to_json(self):
        '''Returns data as a json object'''
        pass

    def to_ndarray(self):
        '''Returns data as a numpy array'''
        return self.to_pandas().values


class RawRowDTO(object):
    """DTO for a row in a raw database.

    The Raw API is a simple key/value-store. Each row in a table in a raw database consists of a
    unique row key and a set of columns.

    Attributes:

        key (str):      Unique key for the row.
        columns (int):  A key/value-map consisting of the values in the row.

    """
    def __init__(self, key, columns):
        self.key = key
        self.columns = columns

    def __repr__(self):
        return json.dumps(self.repr_json())

    def repr_json(self):
        return self.__dict__


class RawObject(CogniteDataObject):
    """Raw Data Object."""
    def to_json(self):
        """Returns data as a json object"""
        return self.internal_representation['data']['items']

    def to_pandas(self):
        """Returns data as a pandas dataframe"""
        return pd.DataFrame(self.internal_representation['data']['items'])


# Author: Erlend Vollset
class TagMatchingObject(CogniteDataObject):
    '''Tag Matching Data Object.

    In addition to the standard output formats this data object also has a to_list() method which returns a list of
    names of the tag matches.
    '''
    def to_pandas(self):
        '''Returns data as a pandas dataframe'''
        matches = []
        for tag in self.internal_representation['data']['items']:
            for match in tag['matches']:
                matches.append({
                    'tag': tag['tagId'],
                    'match': match['tagId'],
                    'score': match['score'],
                    'platform': match['platform']
                })
        if matches:
            return pd.DataFrame(matches)[['tag', 'match', 'platform', 'score']]
        return pd.DataFrame()

    def to_json(self):
        '''Returns data as a json object'''
        return self.internal_representation['data']['items']

    def to_list(self, first_matches_only=True):
        '''Returns a list representation of the matches.

        Args:
            first_matches_only (bool):      Boolean determining whether or not to return only the top match for each
                                            tag.

        Returns:
            list: list of matched tags.
        '''
        if self.to_pandas().empty:
            return []
        if first_matches_only:
            return self.to_pandas().sort_values(['score', 'match']).groupby(['tag']).first()['match'].tolist()
        return self.to_pandas().sort_values(['score', 'match'])['match'].tolist()


# Author: Erlend Vollset
class DatapointsObject(CogniteDataObject):
    '''Datapoints Object.'''
    def to_json(self):
        '''Returns data as a json object'''
        return self.internal_representation['data']['items'][0]

    def to_pandas(self):
        '''Returns data as a pandas dataframe'''
        return pd.DataFrame(self.internal_representation['data']['items'][0]['datapoints'])


# Author: Erlend Vollset
class LatestDatapointObject(CogniteDataObject):
    '''Latest Datapoint Object.'''
    def to_json(self):
        '''Returns data as a json object'''
        return self.internal_representation['data']['items'][0]

    def to_pandas(self):
        '''Returns data as a pandas dataframe'''
        return pd.DataFrame([self.internal_representation['data']['items'][0]])

    def to_ndarray(self):
        '''Returns data as a numpy array'''
        return self.to_pandas().values[0]


# Author: Erlend Vollset
class SimilaritySearchObject(CogniteDataObject):
    '''Similarity Search Data Object.'''
    def to_json(self):
        '''Returns data as a json object'''
        return self.internal_representation['data']['items']

    def to_pandas(self):
        '''Returns data as a pandas dataframe'''
        if len(self.to_json()) > 1:
            return pd.DataFrame(self.internal_representation['data']['items'])
        return pd.DataFrame()


# Author: T.K
class AssetSearchObject(CogniteDataObject):
    '''Assets Search Data Object

    TODO:
        * For now just a copy of SimilaritySearchObject. Implement correct formatting for this data.
    '''
    def to_json(self):
        '''Returns data as a json object'''
        return self.internal_representation['data']['items']

    def to_pandas(self):
        '''Returns data as a pandas dataframe'''
        if len(self.to_json()) > 1:
            return pd.DataFrame(self.internal_representation['data']['items'])
        return pd.DataFrame()
