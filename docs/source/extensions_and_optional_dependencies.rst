Extensions and optional dependencies
====================================
Pandas integration
------------------
The SDK is tightly integrated with the `pandas <https://pandas.pydata.org/pandas-docs/stable/>`_ library.
You can use the :code:`.to_pandas()` method on pretty much any object and get a pandas data frame describing the data.

This is particularly useful when you are working with time series data and with tabular data from the Raw API.

How to install extra dependencies
---------------------------------
If your application requires the functionality from e.g. the :code:`pandas`, :code:`sympy`, or :code:`geopandas` dependencies,
you should install the SDK along with its optional dependencies. The available extras are:

- numpy: numpy
- pandas: pandas
- geo: geopanda, shapely
- sympy: sympy
- functions: pip
- all (will install dependencies for all the above)

These can be installed with the following command:

pip

.. code:: bash

    $ pip install "cognite-sdk[pandas, geo]"

poetry

.. code:: bash

    $ poetry add cognite-sdk -E pandas -E geo
