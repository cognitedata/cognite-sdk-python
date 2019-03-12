# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Changes are grouped as follows
- `Added` for new features.
- `Changed` for changes in existing functionality.
- `Deprecated` for soon-to-be removed features.
- `Removed` for now removed features.
- `Fixed` for any bug fixes.
- `Security` in case of vulnerabilities.

## [Unreleased]
### Added
- Support for all endpoints in Cognite API

### Removed
- `experimental` client in order to ensure sdk stability.

### Changed
- Rename methods so they reflect what the method does instead of what http method is used


## [0.13.0] - 2019-03-12
### Added
- New client for modelhosting API: `experimental.model_hosting`
- Support for thread-local credentials
- CHANGELOG.md :)
- Enable toggling gzip with environment variable "COGNITE_DISABLE_GZIP"

### Removed
- `data_transfer_service` module. Replaced with separate PyPi package. 
- `experimental.analytics` client

### Changed
- Move all modules under `cognite.client` namespace so that we can have other 
packages under `cognite` namespace. This means `CogniteClient` no longer can be 
imported directly from `cognite` namespace.
- Move all autopaging functionality to common method on ApiClient class.
- `path` parameter in `client.assets.get_assets()` and `client.time_series.get_time_series()` is now `List[int]` instead of `str`
- Use shared requests session instead of one per client

### Fixed
- get_datapoints and get_datapoints_frame no longer cap time series data in certain edge cases.

## [0.12.4] - 2019-01-09
### Added
- Support for testing against py35, py36, and py37 with tox

## [0.12.0] - 2019-01-03
### Added
- Support for making raw http calls through `CogniteClient`
- Pretty prints for all response classes

### Changed
- Client interface. Methods are now accessed through `CogniteClient` class. * Ensures 
thread-safety. All sub clients (specific to APIs, e.g. files, events, timeseries, datapoints) 
inherit from APIClient class which give them access to sending requests through _self_.
- Documentation improved and includes examples for most methods.
- `DataTransferService`updated to use `CogniteClient`
- Api version no longer exposed to user.  0.5 is default and some 0.6 features are exposed 
through an **experimental** sub client.

### Removed
- to_ndarray() method on response objects.
