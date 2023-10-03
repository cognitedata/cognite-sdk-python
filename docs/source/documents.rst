Documents
===============
Documents API
---------------
Aggregate Document Count
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.documents.DocumentsAPI.aggregate_count

Aggregate Document Value Cardinality
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.documents.DocumentsAPI.aggregate_cardinality_values

Aggregate Document Property Cardinality
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.documents.DocumentsAPI.aggregate_cardinality_properties

Aggregate Document Unique Values
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.documents.DocumentsAPI.aggregate_unique_values

Aggregate Document Unique Properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.documents.DocumentsAPI.aggregate_unique_properties

List Documents
^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.documents.DocumentsAPI.list

Retrieve Document Content
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.documents.DocumentsAPI.retrieve_content

Retrieve Document Content Buffer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.documents.DocumentsAPI.retrieve_content_buffer

Search Documents
^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.documents.DocumentsAPI.search


Documents classes
^^^^^^^^^^^^^^^^^^
.. automodule:: cognite.client.data_classes.documents
    :members:
    :show-inheritance:

Preview
---------
Download Image Preview Bytes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.documents.DocumentPreviewAPI.download_page_as_png_bytes

Download Image Preview to Path
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.documents.DocumentPreviewAPI.download_page_as_png

Download PDF Preview Bytes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.documents.DocumentPreviewAPI.download_document_as_pdf_bytes

Download PDF Preview to Path
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.documents.DocumentPreviewAPI.download_document_as_pdf

Retrieve PDF Preview Temporary Link
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.documents.DocumentPreviewAPI.retrieve_pdf_link

