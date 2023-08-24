Appendix
---------

.. _appendix-upsert:

Upsert
^^^^^^^^^^^^^^^^^^^^

Upsert methods `cognite_client.api.upsert()` for example `cognite_client.assets.upsert()` the following caveats and
notes apply:

.. warning::
    Upsert is not an atomic operation. It performs multiple calls to the update and create endpoints, depending
    on whether the items exist from before or not. This means that if one of the calls fail, it is possible
    that some of the items have been updated/created while others have not been created/updated.

.. note::
    The mode parameter controls how the update is performed. If you set 'patch', the call will only update
    the fields in the Item object that are not None. This means that if the items exists from before, the
    fields that are not specified will not be changed. If you set 'replace', all the fields that are not
    specified, i.e., set to None and support being set to null, will be nulled out. See the API
    documentation for the update endpoint for more information.
