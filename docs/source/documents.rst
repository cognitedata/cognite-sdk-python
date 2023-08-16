Documents
===============
Documents API
---------------
Aggregate Document Count
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.documents.DocumentsAPI.aggregate_count

Aggregate Document Property Cardinality
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.documents.DocumentsAPI.aggregate_cardinality

Aggregate Document Unique Property Values
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.documents.DocumentsAPI.aggregate_unique

List Documents
^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.documents.DocumentsAPI.list

Retrieve Documents
^^^^^^^^^^^^^^^^^^^
.. automethod:: cognite.client._api.documents.DocumentsAPI.retrieve_content

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

