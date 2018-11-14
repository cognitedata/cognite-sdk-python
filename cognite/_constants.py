# -*- coding: utf-8 -*-
"""Constants

This module contains constants used in the Cognite Python SDK.

This module is protected and should not used by end-users.

Attributes:
    BASE_URL (str):    Base url for Cognite API. Should have correct version number.
    LIMIT (int):       Limit on how many datapoints should be returned from the API when fetching data using the
                        timeseries module.
    RETRY_LIMIT (int): Number of retries to perform if a request to the API should fail.
"""
import os

# GLOBAL CONSTANTS
BASE_URL = "https://api.cognitedata.com"
LIMIT = 100000
LIMIT_AGG = 10000
RETRY_LIMIT = 3
NUM_OF_WORKERS = 10

# DATA TRANSFER SERVICE
TIMESERIES = "timeSeries"
AGGREGATES = "aggregates"
GRANULARITY = "granularity"
START = "start"
END = "end"
MISSING_DATA_STRATEGY = "missingDataStrategy"
LABEL = "label"
