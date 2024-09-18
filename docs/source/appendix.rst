
.. _appendix-upsert:

Upsert
^^^^^^^^^^^^^^^^^^^^

Upsert methods `cognite_client.api.upsert()` for example `cognite_client.assets.upsert()` the following caveats and
notes apply:

.. warning::
    Upsert is not an atomic operation. It performs multiple calls to the update and create endpoints, depending
    on whether the items exist from before or not. This means that if one of the calls fail, it is possible
    that some of the items have been updated/created while others have not been created/updated.

.. _appendix-update:

Update and Upsert Mode Parameter
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The mode parameter controls how the update is performed. If you set 'patch', the call will only update
the fields in the Item object that are not None. This means that if the items exists from before, the
fields that are not specified will not be changed. The 'replace_ignore_null' works similarly, to
'patch', but instead of updating it will replace the fields with new values. There is not difference
between 'patch' and 'replace_ignore_null' for fields that only supports set. For example, `name` and
`description` on TimeSeries. However, for fields that supports `set` and `add/remove`, like `metadata`,
'patch` will add to the metadata, while 'replace_ignore_null' will replace the metadata with the new
metadata. If you set 'replace', all the fields that are not specified, i.e., set to None and
support being set to null, will be nulled out.

Example **patch**:

.. code:: python

    >>> from cognite.client import CogniteClient
    >>> client = CogniteClient()
    >>>
    >>> new_ts = client.time_series.create(
    ...     TimeSeriesWrite(
    ...         external_id="new_ts",
    ...         name="New TS",
    ...         metadata={"key": "value"}
    ...     )
    ... )
    >>>
    >>> updated = client.time_series.update(
    ...     TimeSeriesWrite(
    ...         external_id="new_ts",
    ...         description="Updated description",
    ...         metadata={"new": "entry"}
    ...     ),
    ...     mode="patch"
    ... )
    >>> print(updated.dump())
    {'externalId': 'new_ts', 'name': 'New TS', 'description': 'Updated description', 'metadata': {'key': 'value', 'new': 'entry'}}

Example **replace**:

.. code:: python

    >>> from cognite.client import CogniteClient
    >>> client = CogniteClient()
    >>>
    >>> new_ts = client.time_series.create(
    ...     TimeSeriesWrite(
    ...         external_id="new_ts",
    ...         name="New TS",
    ...         metadata={"key": "value"}
    ...     )
    ... )
    >>>
    >>> updated = client.time_series.update(
    ...     TimeSeriesWrite(
    ...         external_id="new_ts",
    ...         description="Updated description",
    ...         metadata={"new": "entry"}
    ...     ),
    ...     mode="patch"
    ... )
    >>> print(updated.dump())
    {'externalId': 'new_ts', 'description': 'Updated description', 'metadata': {'new': 'entry'}}

Example **replace_ignore_null**:

.. code:: python

    >>> from cognite.client import CogniteClient
    >>> client = CogniteClient()
    >>>
    >>> new_ts = client.time_series.create(
    ...     TimeSeriesWrite(
    ...         external_id="new_ts",
    ...         name="New TS",
    ...         metadata={"key": "value"}
    ...     )
    ... )
    >>>
    >>> updated = client.time_series.update(
    ...     TimeSeriesWrite(
    ...         external_id="new_ts",
    ...         description="Updated description",
    ...         metadata={"new": "entry"}
    ...     ),
    ...     mode="patch"
    ... )
    >>> print(updated.dump())
    {'externalId': 'new_ts', 'name': 'New TS', 'description': 'Updated description', 'metadata': {'new': 'entry'}}

**Note** that the `name` parameter was not specified in the update, and was therefore not changed,
same as in `patch`

Example **replace_ignore_null** without `metadata`:

.. code:: python

    >>> from cognite.client import CogniteClient
    >>> client = CogniteClient()
    >>>
    >>> new_ts = client.time_series.create(
    ...     TimeSeriesWrite(
    ...         external_id="new_ts",
    ...         name="New TS",
    ...         metadata={"key": "value"}
    ...     )
    ... )
    >>>
    >>> updated = client.time_series.update(
    ...     TimeSeriesWrite(
    ...         external_id="new_ts",
    ...         description="Updated description",
    ...     ),
    ...     mode="patch"
    ... )
    >>> print(updated.dump())
    {'externalId': 'new_ts', 'name': 'New TS', 'description': 'Updated description', 'metadata': {'key': 'value'}}

**Note** Since `metadata` was not specified in the update, it was not changed.

.. _appendix-alpha-beta-features:

Alpha and Beta Features
^^^^^^^^^^^^^^^^^^^^^^^^
New Cognite Data Fusion API features may get support in the Python SDK before they are released for
general availability (GA). These features are marked as alpha or beta in the documentation, and will also
invoke a `FeaturePreviewWarning` when used.

Furthermore, we distinguish between maturity of the API specification and the SDK implementation. Typically,
the API specification may be in beta, while the SDK implementation is in alpha.

* `alpha` - The feature is not yet released for general availability. There may be breaking changes to the API
  specification and/or the SDK implementation without further notice.
* `beta` - The feature is not yet released for general availability. The feature is considered stable and 'settled'.
  Learnings during the Beta period may result in a requirement to make breaking changes to API spec/SDK implementation.
  In these situations, release processes must be coordinated to minimise Beta customer disruption (for example use of
  `DeprecationWarning`).
