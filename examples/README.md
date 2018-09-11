<a href="https://cognite.com/">
    <img src="https://github.com/cognitedata/cognite-sdk-python/blob/master/cognite_logo.png" alt="Cognite logo" title="Cognite" align="right" height="80" />
</a>

Cognite Python SDK Examples
===========================

Samples on using the Python SDK to access the Cognite Data Platform.

## Prerequisites
In order to start using these examples, you need
- Python3 and pip
- Install the Python SDK
- An API key in the COGNITE_API_KEY environment variable.


## Examples

### [basics/Learn to use cdp](basics/Learn%20to%20use%20CDP.ipynb.py)

Simple Jupyter notebook which introduces some of the main concepts of the Cognite Data Platform. The notebook fetches, visualizes, and navigates some data from the [Open Industrial Data](https://openindustrialdata.com/) collection. An API key can be obtained from the Open Industrial Data web site.


### [openindustrialdata.py](openindustrialdata.py)

This example requires `matplotlib` for plotting the time series.
```bash
$ pip install matplotlib
```

Simple script to get and plot one year of daily aggregates (min, max, avg) from a time series in the [Open Industrial Data](https://openindustrialdata.com/) collection. An API key can be obtained from the Open Industrial Data web site.


### [analytics/PatternSearch example](analytics/PatternSearch%20example.ipynb)

A Jupyter notebook which uses the Python requests module to make an API call to the Cognite public API and demonstrates the PatternSearch endpoint. PatternSearch lets you search through select intevals of timeseries for a particular timeseries pattern.


## Learn more
Public examples will come in this folder, including examples for:
- Retrieval of timeseries data, using aggregates, granularity, etc.
- Different methods for retrieving data and navigating the data set
- How to get events and why events are useful
- How to use functionality which is in the public API, but not yet incorporated into the SDK, using requests

## License
Apache 2.0
