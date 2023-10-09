Contextualization
=================

Entity Matching
---------------
These APIs will return as soon as possible, deferring a blocking wait until the last moment. Nevertheless, they can block for a long time awaiting results.

Fit Entity Matching Model
^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.entity_matching.EntityMatchingAPI.fit

Re-fit Entity Matching Model
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.entity_matching.EntityMatchingAPI.refit

Retrieve Entity Matching Models
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.entity_matching.EntityMatchingAPI.retrieve
.. automethod:: cognite.client._api.entity_matching.EntityMatchingAPI.retrieve_multiple
.. automethod:: cognite.client._api.entity_matching.EntityMatchingAPI.list

Delete Entity Matching Models
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.entity_matching.EntityMatchingAPI.delete

Update Entity Matching Models
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.entity_matching.EntityMatchingAPI.update

Predict Using an Entity Matching Model
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.entity_matching.EntityMatchingAPI.predict

Engineering Diagrams
--------------------

Detect entities in Engineering Diagrams
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.diagrams.DiagramsAPI.detect

Convert to an interactive SVG where the provided annotations are highlighted
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.diagrams.DiagramsAPI.convert


Vision
------

The Vision API enable extraction of information from imagery data based on
their visual content. For example, you can can extract features such as text, asset tags or industrial objects from images using this service.

**Quickstart**

Start an asynchronous job to extract information from image files stored in CDF:

.. code:: python

    from cognite.client import CogniteClient
    from cognite.client.data_classes.contextualization import VisionFeature

    c = CogniteClient()
    extract_job = c.vision.extract(
        features=[VisionFeature.ASSET_TAG_DETECTION, VisionFeature.PEOPLE_DETECTION],
        file_ids=[1, 2],
    )


The returned job object, :code:`extract_job`, can be used to retrieve the status of the job and the prediction results once the job is completed.
Wait for job completion and get the parsed results:

.. code:: python

    extract_job.wait_for_completion()
    for item in extract_job.items:
        predictions = item.predictions
        # do something with the predictions

Save the prediction results in CDF as `Annotations <https://docs.cognite.com/api/v1/#tag/Annotations>`_:

.. code:: python

    extract_job.save_predictions()

.. note::
    Prediction results are stored in CDF as `Annotations <https://docs.cognite.com/api/v1/#tag/Annotations>`_ using the :code:`images.*` annotation types. In particular, text detections are stored as :code:`images.TextRegion`, asset tag detections are stored as :code:`images.AssetLink`, while other detections are stored as :code:`images.ObjectDetection`.

Tweaking the parameters of a feature extractor:

.. code:: python

    from cognite.client.data_classes.contextualization import FeatureParameters, TextDetectionParameters

    extract_job = c.vision.extract(
        features=VisionFeature.TEXT_DETECTION,
        file_ids=[1, 2],
        parameters=FeatureParameters(text_detection_parameters=TextDetectionParameters(threshold=0.9))
        # or
        # parameters = {"textDetectionParameters": {"threshold": 0.9}}
    )

Extract
^^^^^^^

.. automethod:: cognite.client._api.vision.VisionAPI.extract

Get vision extract job
^^^^^^^^^^^^^^^^^^^^^^

.. automethod:: cognite.client._api.vision.VisionAPI.get_extract_job


Contextualization Data Classes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.contextualization
    :members:
    :undoc-members:
    :show-inheritance:
    :inherited-members:


.. automodule:: cognite.client.data_classes.annotation_types.images
    :members:
    :undoc-members:
    :show-inheritance:
    :inherited-members:


.. automodule:: cognite.client.data_classes.annotation_types.primitives
    :members:
    :undoc-members:
    :show-inheritance:
    :inherited-members:
