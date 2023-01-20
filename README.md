<a href="https://cognite.com/">
    <img src="https://github.com/cognitedata/cognite-python-docs/blob/master/img/cognite_logo.png" alt="Cognite logo" title="Cognite" align="right" height="80" />
</a>

Cognite Python SDK
==========================
[![build](https://github.com/cognitedata/cognite-sdk-python/workflows/release/badge.svg)](https://github.com/cognitedata/cognite-sdk-python/actions?query=workflow:release)
[![Downloads](https://img.shields.io/pypi/dm/cognite-sdk)](https://pypistats.org/packages/cognite-sdk)
[![GitHub](https://img.shields.io/github/license/cognitedata/cognite-sdk-python)](https://github.com/cognitedata/cognite-sdk-python/blob/master/LICENSE)
[![codecov](https://codecov.io/gh/cognitedata/cognite-sdk-python/branch/master/graph/badge.svg)](https://codecov.io/gh/cognitedata/cognite-sdk-python)
[![Documentation Status](https://readthedocs.com/projects/cognite-sdk-python/badge/?version=latest)](https://cognite-sdk-python.readthedocs-hosted.com/en/latest/)
[![PyPI version](https://badge.fury.io/py/cognite-sdk.svg)](https://pypi.org/project/cognite-sdk/)
[![mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

This is the Cognite Python SDK for developers and data scientists working with Cognite Data Fusion (CDF).
The package is tightly integrated with pandas, and helps you work easily and efficiently with data in Cognite Data Fusion (CDF).

## Refererence documentation
* [SDK Documentation](https://cognite-sdk-python.readthedocs-hosted.com/en/latest/)
* [CDF API Documentation](https://doc.cognitedata.com/)
* [Cognite Developer Documentation](https://docs.cognite.com/dev/)

## Installation

### Without any optional dependencies

To install this package without pandas and geopandas support:
```bash
$ pip install cognite-sdk
```

### With optional dependencies
A number of optional dependencies may be specified in order to support a wider set of features.
The available extras (along with the libraries they include) are:
- numpy `[numpy]`
- pandas `[pandas]`
- geo `[geopandas, shapely]`
- sympy `[sympy]`
- functions `[pip]`
- all `[numpy, pandas, geopandas, shapely, sympy, pip]`

To include optional dependencies, specify them like this with pip:

```bash
$ pip install "cognite-sdk[pandas, geo]"
```

or like this if you are using poetry:
```bash
$ poetry add cognite-sdk -E pandas -E geo
```

### Performance notes
If you regularly need to fetch large amounts of datapoints, consider installing with `numpy`
(or with `pandas`, as it depends on `numpy`) for best performance, then use the `retrieve_arrays` (or `retrieve_dataframe`) endpoint(s). This avoids building large pure Python data structures, and instead reads data directly into memory-efficient `numpy.ndarrays`.

### Windows specific

On Windows, it is recommended to install `geopandas` and its dependencies using `conda` package manager, see [geopandas installation page](https://geopandas.org/en/stable/getting_started/install.html#installation).
The following commands create a new environment, install `geopandas` and `cognite-sdk`.

```bash
conda create -n geo_env
conda activate geo_env
conda install --channel conda-forge geopandas
pip install cognite-sdk
```

## Changelog
Wondering about upcoming or previous changes to the SDK? Take a look at the [CHANGELOG](https://github.com/cognitedata/cognite-sdk-python/blob/master/CHANGELOG.md).

## Migration Guide
To help you upgrade your code(base) quickly and safely to a newer major version of the SDK, check out our migration guide. It is a more focused guide based on the detailed change log. [MIGRATION GUIDE](https://github.com/cognitedata/cognite-sdk-python/blob/master/MIGRATION_GUIDE.md).

## Contributing
Want to contribute? Check out [CONTRIBUTING](https://github.com/cognitedata/cognite-sdk-python/blob/master/CONTRIBUTING.md).
