.. cognite-sdk documentation master file, created by
   sphinx-quickstart on Thu Jan 11 15:57:44 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Cognite Python SDK Documentation
================================

This is the Cognite Python SDK for developers and data scientists working with Cognite Data Fusion (CDF). The package is tightly integrated with pandas, and helps you work easily and efficiently with data in Cognite Data Fusion (CDF).


Installation
^^^^^^^^^^^^
This package supports Python |PythonVersion|. To install this package:

.. code-block:: bash

   pip install cognite-sdk

To upgrade the version of this package:

.. code-block:: bash

   pip install cognite-sdk --upgrade

To install optional dependencies:

.. code-block:: bash

   pip install "cognite-sdk[pandas, geo]"


Contents
^^^^^^^^
.. toctree::
   :caption: Getting started
   :maxdepth: 2

   quickstart
   settings
   extensions_and_optional_dependencies

.. toctree::
   :caption: Connecting to CDF
   :maxdepth: 2

   credential_providers
   cognite_client

.. toctree::
   :caption: Interacting with CDF
   :maxdepth: 2

   identity_and_access_management
   core_data_model
   3d
   contextualization
   data_ingestion
   data_organization
   transformations
   functions
   data_modeling

.. toctree::
   :caption: Miscellaneous
   :maxdepth: 2

   deprecated
   base_data_classes
   exceptions
   utils
   testing
   appendix
