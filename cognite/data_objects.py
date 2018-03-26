# -*- coding: utf-8 -*-
'''Data Objects

This module contains data objects used to represent the data returned from the API. These objects have at least
the following output formats:

    * to_pandas():    Returns pandas dataframe
    * to_ndarray():   Numpy array
    * to_json():      Json format
'''
import abc
import json

import pandas as pd
import six

from cognite import _utils


@six.add_metaclass(abc.ABCMeta)
class CogniteDataObject():
    '''Abstract Cognite Data Object

    This abstract class provides a skeleton for all data objects in this module. All data objects should inherit
    this class.
    '''

    def __init__(self, internal_representation):
        self.internal_representation = internal_representation

    @abc.abstractmethod
    def to_pandas(self):
        '''Returns data as a pandas dataframe'''
        pass

    @abc.abstractmethod
    def to_json(self):
        '''Returns data as a json object'''
        pass

    def to_ndarray(self):
        '''Returns data as a numpy array'''
        return self.to_pandas().values

    def next_cursor(self):
        '''Returns next cursor to use for paging through results'''
        if self.internal_representation.get('data'):
            return self.internal_representation.get('data').get('nextCursor')

    def previous_cursor(self):
        '''Returns previous cursor'''
        if self.internal_representation.get('data'):
            return self.internal_representation.get('data').get('previousCursor')


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


class DatapointsObject(CogniteDataObject):
    '''Datapoints Object.'''

    def to_json(self):
        '''Returns data as a json object'''
        return self.internal_representation['data']['items'][0]

    def to_pandas(self):
        '''Returns data as a pandas dataframe'''
        return pd.DataFrame(self.internal_representation['data']['items'][0]['datapoints'])


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


class TimeseriesObject(CogniteDataObject):
    '''Timeseries Object'''

    def to_json(self):
        '''Returns data as a json object'''
        return self.internal_representation

    def to_pandas(self):
        '''Returns data as a pandas dataframe'''
        if self.internal_representation[0].get('metadata') is None:
            return pd.DataFrame(self.internal_representation)
        [d.update(d.pop('metadata')) for d in self.internal_representation]
        return pd.DataFrame(self.internal_representation)


class TimeSeriesDTO(object):
    """Data Transfer Object for a timeseries.

    Attributes:
        tag_id (str):       Unique ID of time series.
        is_string (bool):    Whether the time series is string valued or not.
        metadata (dict):    Metadata.
        unit (str):         Physical unit of the time series.
        asset_id (str):     Asset that this time series belongs to.
        description (str):  Description of the time series.
        securityCategories (list(int)): Security categories required in order to access this time series.
        step (bool):        Whether or not the time series is a step series.

    """

    def __init__(self, tag_id, is_string=False, metadata=None, unit=None, asset_id=None, description=None,
                 security_categories=None, step=None):
        self.tagId = tag_id
        self.isString = is_string
        self.metadata = metadata
        self.unit = unit
        self.assetId = asset_id
        self.description = description
        self.security_categories = security_categories
        self.step = step


class DatapointDTO(object):
    '''Data transfer object for datapoints.

    Attributes:
        timestamp (int, datetime):    The data timestamp in milliseconds since the epoch (Jan 1, 1970) or as a
        datetime object.

        value (string):     The data value, Can be string or numeric depending on the metric.
    '''

    def __init__(self, timestamp, value):
        self.timestamp = timestamp if isinstance(timestamp, int) else _utils.datetime_to_ms(timestamp)
        self.value = value


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


class AssetSearchObject(CogniteDataObject):
    '''Assets Search Data Object'''

    def to_json(self):
        '''Returns data as a json object'''
        return self.internal_representation['data']['items']

    def to_pandas(self):
        '''Returns data as a pandas dataframe'''
        if len(self.to_json()) > 1:
            return pd.DataFrame(self.internal_representation['data']['items'])
        return pd.DataFrame()


class AssetDTO(object):
    '''Data transfer object for assets.

    Attributes:
        name (str):                 Name of asset. Often referred to as tag.
        parent_id (int):            ID of parent asset, if any.
        description (str):          Description of asset.
        metadata (dict):            Custom , application specific metadata. String key -> String Value.
        ref_id (str):               Reference ID used only in post request to disambiguate references to duplicate
                                    names.
        parent_name (str):          Name of parent, this parent must exist in the same POST request.
        parent_ref_id (list(int)):  Reference ID of parent, to disambiguate if multiple nodes have the same name.
    '''

    def __init__(self, name, parent_id=None, description=None, metadata=None, ref_id=None, parent_name=None,
                 parent_ref_id=None):
        self.name = name
        self.parentId = parent_id
        self.description = description
        self.metadata = metadata
        self.refId = ref_id
        self.parentName = parent_name
        self.parentRefId = parent_ref_id
