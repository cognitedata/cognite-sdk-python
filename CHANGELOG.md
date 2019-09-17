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

## [Planned]
- Concurrent reads for all resource types using `/cursors` endpoints
- Upserts for all resource types
- Separate read/write fields on data classes

## [Unreleased]

### Added
- Support for aggregated properties of assets.
- `Asset` and `AssetList` classes now have a `sequences` function which retrieves related sequences.

### Changed
- `list` and `__call__` methods for assets now support list parameters for `root_ids`, `root_external_ids`.
- Sequences API no longer supports column ids, all relevant functions have been changed to only use external ids.

### Fixed
- Fixed a bug in time series pagination where getting 100k dense datapoints would cause a missing id error.
- Sequences retrieve functions fixed to match API change, to single item per retrieve.

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
