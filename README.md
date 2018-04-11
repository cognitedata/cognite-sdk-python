<a href="https://cognite.com/">
    <img src="https://github.com/cognitedata/cognite-sdk-python/blob/readme/cognite_logo.png" alt="Cognite logo" title="Cognite" align="right" height="80" />
</a>

Cognite API Python SDK
==========================
Python SDK to ensure excellent CDP user experience for developers and data scientists.

[![build](https://webhooks.dev.cognite.ai/build/buildStatus/icon?job=github-builds/cognite-sdk-python/master)](https://build.dev.cognite.ai/job/github-builds/job/cognite-sdk-python/job/master/)
[![codecov](https://codecov.io/gh/cognitedata/cognite-sdk-python/branch/master/graph/badge.svg)](https://codecov.io/gh/cognitedata/cognite-sdk-python)
[![Documentation Status](https://readthedocs.org/projects/cognite-sdk-python/badge/?version=latest)](http://cognite-sdk-python.readthedocs.io/en/latest/?badge=latest)

## Installation
```bash
$ pip install cognite-sdk
```

## Usage
Simple script to download and plot one year of hourly aggregates.
```python
import os
from cognite.config import configure_session
from cognite.timeseries import get_datapoints
import matplotlib.pyplot as plt

configure_session(api_key=os.environ.get('COGNITE_API_KEY'), project='akerbp')

tag_id = 'a_tag'
datapoints = get_datapoints(tag_id, start='52w-ago', aggregates=['avg'], granularity='1h')

dataframe = datapoints.to_pandas()

dataframe.plot(x='timestamp')
plt.show()
```
