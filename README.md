<a href="https://cognite.com/">
    <img src="https://github.com/cognitedata/cognite-sdk-python/blob/master/cognite_logo.png" alt="Cognite logo" title="Cognite" align="right" height="80" />
</a>

Cognite Python SDK
==========================
Python SDK to ensure excellent user experience for developers and data scientists working with the Cognite Data Platform.

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
from cognite.v05.timeseries import get_datapoints
import matplotlib.pyplot as plt

# Set API key and project for current session
configure_session(api_key=os.getenv('COGNITE_API_KEY'), project='akerbp')

# Retrieve one year of hourly aggreagets for timeseries 'equipment_x'
ts = 'equipment_x'
datapoints = get_datapoints(ts, start='52w-ago', aggregates=['avg'], granularity='1h')

# Convert to pandas dataframe
dataframe = datapoints.to_pandas()

# Plot the dataframe
dataframe.plot(x='timestamp')
plt.show()
```

## Contributing
Want to contribute? Check out [CONTRIBUTING](https://github.com/cognitedata/cognite-sdk-python/blob/master/CONTRIBUTING.md).

## Documentation
* [SDK Documentation](http://cognite-sdk-python.readthedocs.io/en/latest/)
* [API Documentation](https://doc.cognitedata.com/)


## License
Apache 2.0