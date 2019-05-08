.. cognite-sdk documentation master file, created by
   sphinx-quickstart on Thu Jan 11 15:57:44 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Cognite Python SDK Documentation
================================

This is the Cognite Python SDK for developers and data scientists working with Cognite Data Fusion (CDF). The package is tightly integrated with pandas, and helps you work easily and efficiently with data in Cognite Data Fusion (CDF). 

.. contents::
   :local:

Installation
^^^^^^^^^^^^
.. note::

   The SDK version 1.0.0 is currently in alpha so you need to include the :code:`--pre` flag in order to install it.
   If you already have the the SDK installed in your environment, include the :code:`-U` flag to upgrade aswell.

To install this package:

.. code-block:: bash

   pip install --pre cognite-sdk

To install this package without the pandas and NumPy support:

.. code-block:: bash

   pip install --pre cognite-sdk-core

Contents
^^^^^^^^
.. toctree::
   cognite

Examples
^^^^^^^^
For a collection of scripts and Jupyter Notebooks that explain how to perform various tasks in Cognite Data Fusion (CDF) using Python, see the GitHub repository `here <https://github.com/  cognitedata/cognite-python-docs>`__.
