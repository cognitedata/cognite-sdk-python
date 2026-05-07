Simulators
==========

.. currentmodule:: cognite.client

Simulation Run Load Balancing
------------------------------

When enabled on your CDF project, simulation run load balancing distributes simulation runs
across all available connectors automatically. This means:

- ``simulatorIntegrationExternalId`` is optional when creating routines and runs.
  If omitted, any available connector for that simulator can pick up the run.
- Runs start with status ``queued`` instead of ``ready``.
  A connector claims the run and transitions it to ``ready``.

Without load balancing (default), ``simulatorIntegrationExternalId`` is required
and each run is tied to a specific connector.

SimulatorsAPI
-------------
.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst

   AsyncCogniteClient.simulators

Simulator Integrations
-----------------------
.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst

   AsyncCogniteClient.simulators.integrations

Simulator Models
----------------
.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst

   AsyncCogniteClient.simulators.models

Simulator Model Revisions
--------------------------
.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst

   AsyncCogniteClient.simulators.models.revisions

Simulator Routines
------------------
.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst

   AsyncCogniteClient.simulators.routines

Simulator Routine Revisions
----------------------------
.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst

   AsyncCogniteClient.simulators.routines.revisions

Simulation Runs
---------------
.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst

   AsyncCogniteClient.simulators.runs

Simulator Logs
--------------
.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst

   AsyncCogniteClient.simulators.logs

Data classes
------------
.. automodule:: cognite.client.data_classes.simulators
    :members:
    :show-inheritance: 