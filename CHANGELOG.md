# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The changelog for SDK version 0.x.x can be found [here](https://github.com/cognitedata/cognite-sdk-python/blob/0.13/CHANGELOG.md).

Changes are grouped as follows
- `Added` for new features.
- `Changed` for changes in existing functionality.
- `Deprecated` for soon-to-be removed features.
- `Removed` for now removed features.
- `Fixed` for any bug fixes.
- `Security` in case of vulnerabilities.

## [2.38.5] - 2022-01-12
### Fixed
- Bug where creating/updating/deleting more than 5 transformation schedules in a single call would fail.

## [2.38.4] - 2021-12-17
### Fixed
- Bug where list generator helper will return more than chunk_size items.

## [2.38.3] - 2021-12-13
### Fixed
- Bug where client consumes all streaming content when logging request.

## [2.38.2] - 2021-12-09
### Added
- add the possibility to pass extra body fields to APIClient._create_multiple.

## [2.38.1] - 2021-12-07
### Fixed
- Bug where loading `transformations.jobs` from JSON fails for raw destinations.

## [2.38.0] - 2021-11-30
### Added
- `transformations` api client, which allows the creation, deletion, update, run and retrieval of transformations.
- `transformations.schedules` api client, which allows the schedule, unschedule and retrieval of recurring runs of a transformation.
- `transformations.notifications` api client, which allows the creation, deletion and retrieval of transformation email notifications.
- `transformations.schema` api client, which allows the retrieval of the expected schema of sql transformations based on the destination data type.
- `transformations.jobs` api client, which retrieves the  status of transformation runs.

## [2.37.1] - 2021-12-01
### Fixed
- Bug where `sequences` full update attempts to "set" column spec. "set" is not supported for sequence column spec.

## [2.37.0] - 2021-11-30
### Added
- Added support for retrieving file download urls 

## [2.36.0] - 2021-11-30
### Fixed
- Changes default JSON `.dumps()` behaviour to be in strict compliance with the standard: if any NaNs or +/- Infs are encountered, an exception will now be raised.

## [2.35.0] - 2021-11-29
### Added
- Added support for `columns` update on sequences

## [2.34.0] - 2021-11-5
### Added
- Added support for `data_set_id` on template views

## [2.33.0] - 2021-10-27
### Security
- Disallow downloading files to path outside download directory in `files.download()`.

## [2.32.0] - 2021-10-04
### Added
 - Support for extraction pipelines

## [2.31.1] - 2021-09-27
### Fixed
- Fixed a bug related to handling of binary response payloads.

## [2.31.0] - 2021-08-26
### Added
- View resolver for template fields.

## [2.30.0] - 2021-08-18
### Added
- Support for Template Views

## [2.29.0] - 2021-08-16
### Added
- Raw rows are retrieved using parallel cursors when no limit is set.

## [2.28.2] - 2021-08-10
### Added
- Relationships now supports `partitions` parameter for [parallel retrieval](https://docs.cognite.com/api/v1/#section/Parallel-retrieval)

## [2.28.1] - 2021-08-10
### Changed
- debug mode now logs response payload and headers.

## [2.27.0] - 2021-07-20

### Fixed
- When using CogniteClient with the client-secret auth flow, the object would not be pickle-able (e.g. when using multiprocessing) because of an anonymous function.

## [2.26.1] - 2021-07-20

### Changed
- Optimization. Do not get windows if remaining data points is 0. Reduces number of requests when asking for 100k data points/10k aggregates from 2 to 1.

## [2.26.0] - 2021-07-08

### Added
- Support for set labels on AssetUpdate

## [2.25.0] - 2021-07-06

### Added
- filter_nodes function to ThreeDRevisionsAPI

## [2.24.0] - 2021-06-28

### Added
- ignore_unknown_ids flag to Relationships delete method

## [2.23.0] - 2021-06-25

### Added
- insert_dataframe and retrieve_dataframe methods to the Raw client

## [2.22.0] - 2021-06-22

### Added
- More contextualization job statuses
### Changed
- Refactor contextualization constant representation

## [2.21.0] - 2021-06-18

### Added
- Datasets support for labels

## [2.20.0] - 2021-06-04

### Added
- rows() in RawRowsAPI support filtering with `columns` and `min/maxLastUpdatedTime`

## [2.19.0] - 2021-05-06

### Added
- Support for /token/inspect endpoint

## [2.18.2] - 2021-04-23

### Fixed
- Bug in templates instances filter that would cause `template_names` to be ignored.

## [2.18.1] - 2021-04-22

### Added
- Configure file download/upload timeouts with `COGNITE_FILE_TRANSFER_TIMEOUT` environment variable or
`file_transfer_timeout` parameter on `CogniteClient`.

### Changed
- Increased default file transfer timeout from 180 to 600 seconds
- Retry more failure modes (read timeouts, 502, 503, 504) for files upload/download requests.

## [2.18.0] - 2021-04-20

### Changed
- `COGNITE_DISABLE_SSL` now also covers ssl verification on IDP endpoints used for generating tokens.


## [2.17.1] - 2021-04-13

### Added
- `created_time`, and `last_updated_time` to template data classes.
- `data_set_id` to template instance data class.


## [2.17.0] - 2021-03-26

### Changed
- Ignore exceptions from pypi version check and reduce its timeout to 5 seconds.

### Fixed
- Only 200/201/202 is treated as succesfull response. 301 led to json decoding errors -
now handled gracefully.
- datasets create limit was set to 1000 in the sdk, leading to cases of 400 from the api where the limit is 10.

### Added
- Support for specifying proxies in the CogniteClient constructor

### Removed
- py.typed file. Will not declare library as typed until we run a typechecker on the codebase.


## [2.16.0] - 2021-03-24

### Added
- support for templates.
- date-based `cdf-version` header.

## [2.15.0] - 2021-03-12

### Added
- `createdTime` field on raw dbs and tables.

## [2.14.0] - 2021-03-18

### Added
- dropna argument to insert_dataframe method in DatapointsAPI

## [2.13.0] - 2021-03-12

### Added
- `sortByNodeId` and `partitions` query parameters to `list_nodes` method.

## [2.12.2] - 2021-03-11

### Fixed
- CogniteAPIError raised (instead of internal KeyError) when inserting a RAW row without a key.

## [2.12.1] - 2021-03-09

### Fixed
- CogniteMissingClientError raised when creating relationship with malformed body.

## [2.12.0] - 2021-03-04

### Changed
- Move Entity matching API from beta to v1.

## [2.11.1] - 2021-02-18

### Changed
- Resources are now more lenient on which types they accept in for labels

## [2.11.0] - 2021-02-18

### Changed
- Entity matching fit will flatten dictionaries and resources to "metadata.subfield" similar to pipelines.

### Added
- Relationships now support update

## [2.10.7] - 2021-02-02

### Fixed
- Relationships API list calls via the generator now support `chunk_size` as parameter.

## [2.10.6] - 2021-02-02

### Fixed
- Retry urllib3.NewConnectionError when it isn't in the context of a ConnectionRefusedError

## [2.10.5] - 2021-01-25

### Fixed
- Fixed asset subtree not returning an object with id->item cache for use in .get

## [2.10.4] - 2020-12-14

### Changed
- Relationships filter will now chain filters on large amounts of sources or targets in batches of 1000 rather than 100.


## [2.10.3] - 2020-12-09

### Fixed
- Retries now have backup time tracking per request, rather than occasionally shared between threads.
- Sequences delete ranges now no longer gives an error if no data is present

## [2.10.2] - 2020-12-08

### Fixed
- Set geoLocation.type in files to "Feature" if missing

## [2.10.1] - 2020-12-03

### Added
- Chaining of requests to the relationships list method,
allowing the method to take arbitrarily long lists for `source_external_ids` and `target_external_ids`

## [2.10.0] - 2020-12-01

### Added
- Authentication token generation and lifecycle management

## [2.9.0] - 2020-11-25

### Added
- Entity matching API is now available in the beta client.

## [2.8.0] - 2020-11-23

### Changed
- Move relationships to release python SDK

## [2.7.0] - 2020-11-10

### Added
- `fetch_resources` parameter to the relationships `list` and `retrieve_multiple` methods, which attempts to fetch the resource referenced in the relationship.

## [2.6.4] - 2020-11-10

### Fixed
- Fixed a bug where 429 was not retried on all endpoints

## [2.6.3] - 2020-11-06

### Fixed
- Resource metadata should be able to set empty using `.metadata.set(None)` or `.metadata.set({})`.

## [2.6.2] - 2020-11-05

### Fixed
- Asset retrieve subtree should return empty AssetList if asset does not exist.

## [2.6.1] - 2020-10-30

### Added
- `geospatial` to list of valid relationship resource types.

## [2.6.0] - 2020-10-26

### Changed
- Relationships list should take dataset internal and external id as different parameters.

## [2.5.4] - 2020-10-22

### Fixed
- `_is_retryable` didn't handle clusters with a dash in the name.

## [2.5.3] - 2020-10-14

### Fixed
- `delete_ranges` didn't cast string timestamp into number properly.

## [2.5.2] - 2020-10-06

### Fixed
- `labels` in FileMetadata is not cast correctly to a list of `Label` objects.

## [2.5.1] - 2020-10-01
- Include `py.typed` file in sdk distribution

## [2.5.0] - 2020-09-25

### Added
- Relationships beta support.

### Removed
- Experimental Model Hosting client.

## [2.4.3] - 2020-09-18
- Increase raw rows list limit to 10,000

## [2.4.2] - 2020-09-10
- Fixed a bug where urls with query parameters were excluded from the retryable endpoints.

## [2.4.1] - 2020-09-09

### Changed
- Generator-based listing now supports partitions. Example:
  ``` python
  for asset in client.assets(partitions=10):
    # do something
  ```

## [2.4.0] - 2020-08-31

### Added
- New 'directory' in Files

## [2.3.0] - 2020-08-25

### Changed
- Add support for mypy and other type checking tools by adding packaging type information

## [2.2.2] - 2020-08-18

### Fixed
- HTTP transport logic to better handle retrying of connection errors
- read timeouts will now raise a CogniteReadTimeout
- connection errors will now raise a CogniteConnectionError, while connection refused errors will raise the more
 specific CogniteConnectionRefused exception.

### Added
- Jitter to exponential backoff on retries

### Changed
- Make HTTP requests no longer follow redirects by default
- All exceptions now inherit from CogniteException

## [2.2.1] - 2020-08-17

### Added
- Fixed a bug where `/timeseries/list` was missing from the retryable endpoints.

## [2.2.0] - 2020-08-14

### Added
- Files labelling support

## [2.1.1] - 2020-08-13

### Fixed
- Fixed a bug where only v1 endpoints (not playground) could be added as retryable

## [2.1.1] - 2020-08-04

### Fixed
- Calls to datapoints `retrieve_dataframe` with `complete="fill"` would break using Pandas version 1.1.0 because it raises TypeError when calling `.interpolate(...)` on a dataframe with no columns.

## [2.1.0] - 2020-07-22

### Added
- Support for passing a single string to `AssetUpdate().labels.add` and `AssetUpdate().labels.remove`. Both a single string and a list of strings is supported. Example:
  ```python
  # using a single string
  my_update = AssetUpdate(id=1).labels.add("PUMP").labels.remove("VALVE")
  res = c.assets.update(my_update)

  # using a list of strings
  my_update = AssetUpdate(id=1).labels.add(["PUMP", "ROTATING_EQUIPMENT"]).labels.remove(["VALVE"])
  res = c.assets.update(my_update)
  ```

## [2.0.0] - 2020-07-17

### Changed
- The interface to interact with labels has changed. A new, improved interface is now in place to make it easier to work with CDF labels. The new interface behaves this way:
  ```python
  # crate label definition(s)
  client.labels.create(LabelDefinition(external_id="PUMP", name="Pump", description="Pump equipment"))
  # ... or multiple
  client.labels.create([LabelDefinition(external_id="PUMP"), LabelDefinition(external_id="VALVE")])

  # list label definitions
  label_definitions = client.labels.list(name="Pump")

  # delete label definitions
  client.labels.delete("PUMP")
  # ... or multiple
  client.labels.delete(["PUMP", "VALVE"])

  # create an asset with label
  asset = Asset(name="my_pump", labels=[Label(external_id="PUMP")])
  client.assets.create(assets)

  # filter assets by labels
  my_label_filter = LabelFilter(contains_all=["PUMP", "VERIFIED"])
  asset_list = client.assets.list(labels=my_label_filter)

  # attach/detach labels to/from assets
  my_update = AssetUpdate(id=1).labels.add(["PUMP"]).labels.remove(["VALVE"])
  res = c.assets.update(my_update)
  ```

### Fixed
- Fixed bug where `_call_` in SequencesAPI (`client.sequences`) was incorrectly returning a `GET` method instead of `POST`.

## [1.8.1] - 2020-07-07
### Changed
- For 3d mappings delete, only use node_id and asset_id pairs in delete request to avoid potential bad request.
- Support attaching/detaching multiple labels on assets in a single method

## [1.8.0] - 2020-06-30
### Added
- Synthetic timeseries endpoint for DatapointsApi
- Labels endpoint support
- Assets labelling support
- Support for unique value aggregation for events.

### Changed
- When `debug=true`, redirects are shown more clearly.

## [1.7.0] - 2020-06-03
### Fixed
- datasetId is kept as an integer in dataframes.

### Changed
- Internal list of retryable endpoints was changed to a class variable so it can be modified.

## [1.6.0] - 2020-04-28
### Added
- Support events filtering by ongoing events (events without `end_time` defined)
- Support events filtering by active timerange of event
- Support files metadata filtering by `asset_external_ids`
- Aggregation endpoint for Assets, DataSets, Events, Files, Sequences and TimeSeries API

## [1.5.2] - 2020-04-02
### Added
- Support for security categories on file methods

## [1.5.1] - 2020-04-01
### Added
- Support for security categories on files
- active_at_time on relationships

### Fixed
- No longer retry calls to /files/initupload
- Retry retryable POST endpoints in datasets API

## [1.5.0] - 2020-03-12
### Added
- DataSets API and support for this in assets, events, time series, files and sequences.
- .asset helper function on time series.
- asset external id filter on time series.

## [1.4.13] - 2020-03-03
### Added
- Relationship list supports multiple sources, targets, relationship types and datasets.

## [1.4.12] - 2020-03-02

### Fixed
- Fixed a bug in file uploads where fields other than name were not being passed to uploaded directories.

## [1.4.11] - 2020-02-21

### Changed
- Datapoint insertion changed to be less memory intensive.

### Fixed
- Fixed a bug where add service account to group expected items in response.
- Jupyter notebook output and non-camel cased to_pandas uses nullable int fields instead of float for relevant fields.

## [1.4.10] - 2020-01-24
### Added
- Support for the error field for synthetic time series query in the experimental client.
- Support for retrieving data from multiple sequences at once.

## [1.4.9] - 2019-12-19

### Fixed
- Fixed a bug where datapoints `retrieve` could return less than limit even if there were more datapoints.
- Fixed an issue where `insert_dataframe` would give an error with older pandas versions.

## [1.4.8] - 2019-12-19

### Added
- Support for `ignore_unknown_ids` on time series `retrieve_multiple`, `delete` and datapoints `retrieve` and `latest` and related endpoints.
- Support for asset subtree filters on files, sequences, and time series.
- Support for parent external id filters on assets.
- Synthetic datapoints retrieve has additional functions including variable replacement and sympy support.

### Changed
- Synthetic datapoints now return errors in the `.error` field, in the jupyter output, and optionally in pandas dataframes if `include_errors` is set.

## [1.4.7] - 2019-12-05

### Added
- Support for synthetic time series queries in the experimental client.
- parent external id filter added for assets.

### Fixed
- startTime in event dataframes is now a nullable int dtype, consistent with endTime.

## [1.4.6] - 2019-12-02

### Fixed
- Fixed notebook output for Asset, Datapoint and Raw.

## [1.4.5] - 2019-12-02

### Changed

- The ModelHostingAPI now calls Model Hosting endpoints in playground instead of 0.6.

## [1.4.4] - 2019-11-29

### Added
 - Option to turn off version checking from CogniteClient constructor

### Changed
- In sequences create, the column definitions object accepts both camelCased and snake_cased keys.
- Retry 429 on all endpoints

### Fixed
- Fixed notebook output for DatapointsList

## [1.4.3] - 2019-11-27
### Fixed
- In Jupyter notebooks, the output from built-in list types is no longer camel cased.

## [1.4.2] - 2019-11-27

### Changed
- In the 3D API, the call and list methods now include all models by default instead of only unpublished ones.
- In Jupyter notebooks, the output from built-in types is no longer camel cased.

### Added
- Support for filtering events by asset subtree ids.

## [1.4.1] - 2019-11-18

### Added
- Support for filtering events by asset external id.
- query parameter on asset search.
- `ignore_unknown_ids` parameter on asset and events method `delete` and `retrieve_multiple`.

## [1.4.0] - 2019-11-14

### Changed
- In the ModelHostingAPI, models, versions and schedules are now referenced by name instead of id. The ids are no longer available.
- In the ModelHostingAPI, functions related to model versions are moved from the ModelsAPI to the new ModelVersionsAPI.
- In the ModelHostingAPI, the model names must be unique. Also, the version names and schedule names must be unique per model.
- Default value for `limit` in search method is now 100 instead of None to clarify api default behaviour when no limit is passed.

## [1.3.4] - 2019-11-07

### Changed
- Error 500's are no longer retried by default, only HTTP 429, 502, 503, 504 are.
- Optimized HTTP calls by caching user agent.
- Relationship filtering is now integrated into `list` instead of `search`.
- Sequences `insert_dataframe` parameter `external_id_headers` documentation updated.
- Type hints for several objects formerly `Dict[str, Any]` improved along with introducing matching dict derived classes.

### Fixed
- `source_created_time` and `source_modified_time` on files now displayed as time fields.
- Fixed pagination for `include_outside_points` and other edge cases in datapoints.
- Fixed a bug where `insert_dataframe` with strings caused a numpy error.

### Added
- Relationships can now have sequences as source or target.

## [1.3.3] - 2019-10-21

### Changed
- Datapoints insert dataframe function will check for infinity values.
- Allow for multiple calls to .add / .remove in object updates such as metadata, without later calls overwriting former.
- List time series now ignores the include_metadata parameter.

### Added
- Advanced list endpoint is used for listing time series, adding several new filters and partitions.

## [1.3.2] - 2019-10-16

### Added
- Datapoints objects now store is_string, is_step and unit to allow for better interpretation of the data.
- Sorting when listing events
- Added a search function in the relationships API.

### Changed
- `list` and `__call__` methods for files now support list parameters for `root_ids`, `root_external_ids`.
- retrieve_dataframe with `complete` using Datapoints fields instead of retrieving time series metadata.

### Fixed
- Fixed chunking logic in list_generator to always return last partial chunk.
- Fixed an error on missing target/source in relationships.

## [1.3.1] - 2019-10-09
### Fixed
- Fixed support for totalVariation aggregate completion.
- Changed conversion of raw RowList to pandas DataFrame to handle missing values (in columns) across the rows. This also fixes the bug where one-off values would be distributed to all rows in the DataFrame (unknown bug).

## [1.3.0] - 2019-10-03
### Changed
- Sequences officially released and no longer considered experimental.
- Sequences data insert no longer takes a default value for columns.

## [1.2.1] - 2019-10-01
### Fixed
- Tokens are sent with the correct "Authorization" header instead of "Authentication".

## [1.2.0] - 2019-10-01
### Added
- Support for authenticating with bearer tokens. Can now supply a jwt or jwt-factory to CogniteClient. This token will override any api-key which has been set.

## [1.1.12] - 2019-10-01
### Fixed
- Fixed a bug in time series pagination where getting 100k datapoints could cause a missing id error when using include_outside_points.
- SequencesData `to_pandas` no longer returns NaN on integer zero columns.
- Fixed a bug where the JSON encoder would throw circular reference errors on unknown data types, including numpy floats.

## [1.1.11] - 2019-09-23
### Fixed
- Fix testing.CogniteClientMock so it is possible to get attributes on child which have not been explicitly in the CogniteClientMock constructor

## [1.1.10] - 2019-09-23
### Fixed
- Fix testing.CogniteClientMock so it is possible to get child mock not explicitly defined

### Added
- `list` and `__call__` methods for events now support list parameters for `root_asset_ids`, `root_asset_external_ids`.

## [1.1.9] - 2019-09-20
### Changed
- Renamed testing.mock_cognite_client to testing.monkeypatch_cognite_client

### Added
- testing.CogniteClientMock object

## [1.1.8] - 2019-09-19
### Added
- Support for aggregated properties of assets.
- `Asset` and `AssetList` classes now have a `sequences` function which retrieves related sequences.
- Support for partitioned listing of assets and events.

### Changed
- `list` and `__call__` methods for assets now support list parameters for `root_ids`, `root_external_ids`.
- Sequences API no longer supports column ids, all relevant functions have been changed to only use external ids.

### Fixed
- Fixed a bug in time series pagination where getting 100k dense datapoints would cause a missing id error.
- Sequences retrieve functions fixed to match API change, to single item per retrieve.
- Sequences retrieve/insert functions fixed to match API change to take lists of external ids.

## [1.1.7] - 2019-09-13
### Fixed
- `testing.mock_cognite_client()` so that it still accepts arguments after exiting from mock context.

## [1.1.6] - 2019-09-12
### Fixed
- `testing.mock_cognite_client()` so that the mocked CogniteClient may accept arguments.

## [1.1.5] - 2019-09-12
### Added
- Method `files.download_to_path` for streaming a file to a specific path

## [1.1.4] - 2019-09-12
### Added
- `root_asset_ids` parameter for time series list.

### Changed
- Formatted output in jupyter notebooks for `SequenceData`.
- `retrieve_latest` function in theDatapoints API extended to support more than 100 items.
- Log requests at DEBUG level instead of INFO.

## [1.1.3] - 2019-09-05
### Changed
- Disabled automatic handling of cookies on the requests session objects

### Fixed
- `to_pandas` method on CogniteResource in the case of objects without metadata

## [1.1.2] - 2019-08-28
### Added
- `limit` parameter on sequence data retrieval.
- Support for relationships exposed through experimental client.
- `end` parameter of sequence.data retrieval and range delete accepts -1 to indicate last index of sequence.

### Changed
- Output in jupyter notebooks is now pandas-like by default, instead of outputting long json strings.

### Fixed
- id parameters and timestamps now accept any integer type including numpy.int64, so values from dataframes can be passed directly.
- Compatibility fix for renaming of sequences cursor and start/end parameters in the API.

## [1.1.1] - 2019-08-23
### Added
- `complete` parameter on `datapoints.retrieve_dataframe`, used for forward-filling/interpolating intervals with missing data.
- `include_aggregate_names` option on `datapoints.retrieve_dataframe` and `DatapointsList.to_pandas`, used for removing the `|<aggregate-name>` postfix on dataframe column headers.
- datapoints.retrieve_dataframe_dict function, which returns {aggregate:dataframe} without adding aggregate names to columns
- source_created_time and source_modified_time support for files

## [1.1.0] - 2019-08-21
### Added
- New method create_hierarchy() added to assets API.
- SequencesAPI.list now accepts an asset_ids parameter for searching by asset
- SequencesDataAPI.insert now accepts a SequenceData object for easier copying
- DatapointsAPI.insert now accepts a Datapoints object for easier copying
- helper method `cognite.client.testing.mock_cognite_client()` for mocking CogniteClient
- parent_id and parent_external_id to AssetUpdate class.

### Changed
- assets.create() no longer validates asset hierarchy and sorts assets before posting. This functionality has been moved to assets.create_hierarchy().
- AssetList.files() and AssetList.events() now deduplicate results while fetching related resources, significantly reducing memory load.

## [1.0.5] - 2019-08-15
### Added
- files.create() method to enable creating a file without uploading content.
- `recursive` parameter to raw.databases.delete() for recursively deleting tables.

### Changed
- Renamed .iteritems() on SequenceData to .items()
- raw.insert() now chunks raw rows into batches of 10,000 instead of 1,000

### Fixed
- Sequences queries are now retried if safe
- .update() in all APIs now accept a subclass of CogniteResourceList as input
- Sequences datapoint retrieval updated to use the new cursor feature in the API
- Json serializiation in `__str__()` of base data classes. Now handles Decimal and Number objects.
- Now possible to create asset hierarchy using parent external id when the parent is not part of the batch being inserted.
- `name` parameter of files.upload_bytes is now required, so as not to raise an exception in the underlying API.

## [1.0.4] - 2019-08-05
### Added
- Variety of useful helper functions for Sequence and SequenceData objects, including .column_ids and .column_external_ids properties, iterators and slice operators.
- Sequences insert_dataframe function.
- Sequences delete_range function.
- Support for external id column headers in datapoints.insert_dataframe()

### Changed
- Sequences data retrieval now returns a SequenceData object.
- Sequences insert takes its parameters row data first, and no longer requires columns to be passed.
- Sequences insert now accepts tuples and raw-style data input.
- Sequences create now clears invalid fields such as 'id' in columns specification, so sequences can more easily re-use existing specifications.
- Sequence data function now require column_ids or column_external_ids to be explicitly set, rather than both being passed through a single columns field

## [1.0.3] - 2019-07-26
### Fixed
- Renamed Model.schedule_data_spec to Model.data_spec so the field from the API will be included on the object.
- Handling edge case in Sequences pagination when last datapoint retrieved is at requested end
- Fixing data points retrieval when count aggregates are missing
- Displays unexpected fields on error response from API when raising CogniteAPIError

## [1.0.2] - 2019-07-22
### Added
- Support for model hosting exposed through experimental client

### Fixed
- Handling dynamic limits in Sequences API

## [1.0.1] - 2019-07-19
### Added
- Experimental client
- Support for sequences exposed through experimental client

## [1.0.0] - 2019-07-11
### Added
- Support for all endpoints in Cognite API
- Generator with hidden cursor for all resource types
- Concurrent writes for all resources
- Distribution of "core" sdk which does not depend on pandas and numpy
- Typehints for all methods
- Support for posting an entire asset hierarchy, resolving ref_id/parent_ref_id automatically
- config attribute on CogniteClient to view current configuration.

### Changed
- Renamed methods so they reflect what the method does instead of what http method is used
- Updated documentation with automatically tested examples
- Renamed `stable` namespace to `api`
- Rewrote logic for concurrent reads of datapoints
- Renamed CogniteClient parameter `num_of_workers` to `max_workers`

### Removed
- `experimental` client in order to ensure sdk stability.
