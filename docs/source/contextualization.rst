Contextualization
=================
.. currentmodule:: cognite.client

Entity Matching
---------------
These APIs will return as soon as possible, deferring a blocking wait until the last moment. Nevertheless, they can block for a long time awaiting results.

.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst

   CogniteClient.entity_matching


Engineering Diagrams
--------------------
.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst

   CogniteClient.diagrams

Vision
------

The Vision API enable extraction of information from imagery data based on
their visual content. For example, you can can extract features such as text, asset tags or industrial objects from images using this service.

**Quickstart**

Start an asynchronous job to extract information from image files stored in CDF:

.. code:: python

    from cognite.client import CogniteClient
    from cognite.client.data_classes.contextualization import VisionFeature

    client = CogniteClient()
    extract_job = client.vision.extract(
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

    extract_job = client.vision.extract(
        features=VisionFeature.TEXT_DETECTION,
        file_ids=[1, 2],
        parameters=FeatureParameters(text_detection_parameters=TextDetectionParameters(threshold=0.9))
        # or
        # parameters = {"textDetectionParameters": {"threshold": 0.9}}
    )

.. autosummary::
   :methods:
   :toctree: generated/
   :template: custom-automethods-template.rst
   
   CogniteClient.vision


Contextualization Data Classes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. autosummary::
   :classes:
   :toctree: generated/
   :template: custom-automodule-template.rst

   data_classes.contextualization
   data_classes.annotation_types.images
   data_classes.annotation_types.primitives
