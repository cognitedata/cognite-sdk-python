<a href="https://cognite.com/">
    <img src="https://github.com/cognitedata/cognite-python-docs/blob/master/img/cognite_logo.png" alt="Cognite logo" title="Cognite" align="right" height="80" />
</a>

Cognite Python SDK
==========================
[![build](https://github.com/cognitedata/cognite-sdk-python/workflows/release/badge.svg)](https://github.com/cognitedata/cognite-sdk-python/actions?query=workflow:release)
[![codecov](https://codecov.io/gh/cognitedata/cognite-sdk-python/branch/master/graph/badge.svg)](https://codecov.io/gh/cognitedata/cognite-sdk-python)
[![Documentation Status](https://readthedocs.com/projects/cognite-sdk-python/badge/?version=latest)](https://cognite-docs.readthedocs-hosted.com/en/latest/)
[![PyPI version](https://badge.fury.io/py/cognite-sdk.svg)](https://pypi.org/project/cognite-sdk/)
[![tox](https://img.shields.io/badge/tox-3.5%2B-blue.svg)](https://www.python.org/downloads/release/python-350/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

This is the Cognite Python SDK for developers and data scientists working with Cognite Data Fusion (CDF). 
The package is tightly integrated with pandas, and helps you work easily and efficiently with data in Cognite Data 
Fusion (CDF).

## Documentation
* [SDK Documentation](https://cognite-docs.readthedocs-hosted.com/en/latest/)
* [API Documentation](https://doc.cognitedata.com/)
* [Cognite Developer Documentation](https://docs.cognite.com/dev/)

## Prerequisites
In order to start using the Python SDK, you need
- Python3 (>= 3.5) and pip
- An API key. Never include the API key directly in the code or upload the key to github. Instead, set the API key as an environment variable. See the usage example for how to authenticate with the API key.

This is how you set the API key as an environment variable on Mac OS and Linux:
```bash
$ export COGNITE_API_KEY=<your API key>
```

On Windows, you can follows [these instructions](https://www.computerhope.com/issues/ch000549.htm) to set the API key as an environment variable.

## Installation
To install this package:
```bash
$ pip install cognite-sdk
```

To install this package without the pandas and NumPy support:
```bash
$ pip install cognite-sdk-core
```

To install with pandas, geopandas and shapely support (equivalent to installing `cognite-sdk`).
However, this gives you the option to only have pandas (and NumPy) support without geopandas.
```bash
$ pip install cognite-sdk-core[pandas, geo]
```

On Windows, it is recommended to install `geopandas` and its dependencies using `conda` package manager, see [geopandas installation page](https://geopandas.org/en/stable/getting_started/install.html#installation).
The following commands create a new environment, install `geopandas` and `cognite-sdk`.
```bash
conda create -n geo_env
conda activate geo_env
conda install --channel conda-forge geopandas
pip install cognite-sdk
```
## Examples
For a collection of scripts and Jupyter Notebooks that explain how to perform various tasks in Cognite Data Fusion (CDF) 
using Python, see the GitHub repository [here](https://github.com/cognitedata/cognite-python-docs)

## Changelog
Wondering about upcoming or previous changes to the SDK? Take a look at the [CHANGELOG](https://github.com/cognitedata/cognite-sdk-python/blob/master/CHANGELOG.md).

## Contributing
Want to contribute? Check out [CONTRIBUTING](https://github.com/cognitedata/cognite-sdk-python/blob/master/CONTRIBUTING.md).
