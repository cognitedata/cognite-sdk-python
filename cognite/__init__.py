"""Cognite API SDK

This package has been created to ensure excellent excellent user experience for data scientists using the Cognite
Data Platform (CDP).
"""

# RELEASE CONVENTIONS
#
# Format:
# MAJOR.MINOR[.PATCH]
#
# Increment the:
# * MAJOR version when you make changes which are NOT backwards-compatible
# * MINOR version when you add functionality in a backwards-compatible manner
# * PATCH version when you make backwards-compatible bug fixes.
#
# For more information on versioning see https://semver.org/
#

from cognite.data_transfer_service import DataTransferService

__all__ = ["v04", "v05", "preprocessing", "config", "data_transfer_service"]
__version__ = "0.9.91"
