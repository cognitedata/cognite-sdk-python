.. cognite-sdk documentation master file, created by
   sphinx-quickstart on Thu Jan 11 15:57:44 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Cognite Python SDK Documentation
================================

This is the Cognite Python SDK for developers and data scientists working with Cognite Data Fusion (CDF).
The package is tightly integrated with pandas, and helps you work easily and efficiently with data in
Cognite Data Fusion (CDF).

.. contents::
   :local:

Installation
^^^^^^^^^^^^
To install or upgrade this package:

**pip:**

.. code-block:: bash

   pip install --upgrade cognite-sdk

**poetry:**

.. code-block:: bash

   poetry add cognite-sdk

**uv:**

.. code-block:: bash

   uv add cognite-sdk

To install with optional dependencies:

.. code-block:: bash

   pip install "cognite-sdk[pandas, geo]"

Available extras:

- **numpy**: numpy
- **pandas**: pandas
- **geo**: geopandas, shapely
- **sympy**: sympy
- **all**: *all of the above*


What's new in v8
^^^^^^^^^^^^^^^^
The SDK v8 introduces **full async support** with the new ``AsyncCogniteClient``. This enables:

* Native ``async/await`` patterns for all API operations
* Non-blocking concurrent operations directly in Notebooks (including browser-based via Pyodide) and UI frameworks like Streamlit
* Significantly faster file uploads on Windows (new underlying HTTP client, ``httpx``)
* Async variants for all helper/utility methods on data classes (e.g., ``Function`` now has ``call_async``)

Other improvements:

* Simpler client instantiation: pass either ``cluster`` or ``base_url``
* Typed instance apply classes (e.g., ``CogniteAssetApply``) now work correctly with patch updates (``replace=False``)
* Better pandas DataFrame columns for datapoints: now uses ``MultiIndex`` for identifiers, aggregates, units, etc.
* Read data classes now have correct types (no longer ``Optional`` on required fields)
* More specific exceptions are now always raised when appropriate (``CogniteNotFoundError``, ``CogniteDuplicatedError``)

For a complete list of changes, see the `Migration Guide <https://github.com/cognitedata/cognite-sdk-python/blob/master/MIGRATION_GUIDE.md>`_.

The synchronous ``CogniteClient`` remains fully supported and now wraps the async client internally.

.. code-block:: python

   # Async client (new in v8!)
   from cognite.client import AsyncCogniteClient

   async def main():
       client = AsyncCogniteClient()
       tss = await client.time_series.list()

   # Sync client (still supported)
   from cognite.client import CogniteClient

   client = CogniteClient()
   tss = client.time_series.list()

See the :doc:`quickstart` for configuration examples.

Contents
^^^^^^^^
.. toctree::
   quickstart
   settings
   credential_providers
   cognite_client
   extensions_and_optional_dependencies
   identity_and_access_management
   data_modeling
   agents
   ai
   limits
   assets
   events
   files
   time_series
   sequences
   geospatial
   3d
   contextualization
   documents
   data_ingestion
   hosted_extractors
   postgres_gateway
   data_organization
   transformations
   functions
   simulators
   data_workflows
   unit_catalog
   filters
   base_data_classes
   exceptions
   utils
   testing
   appendix
