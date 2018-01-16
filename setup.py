from setuptools import setup

# RELEASE NAMING CONVENTIONS
#
# Format: MAJOR.MINOR[.MICRO][PRE-RELEASE IDENTIFIER]
# Example: 0.4.1a1
#
# Major: Major revision number for the software like 2 or 3 for Python
# Minor: Groups moderate changes to the software like bug fixes or minor improvements
# Micro: Releases dedicated to bug fixes
#
# Valid pre-release identifiers are: a (alpha), b (beta), rc (release candidate)
#
# alpha releases: early pre-releases. A lot of changes can occur between alphas and the final release,
# like feature additions or refactorings. But they are minor changes and the software should stay pretty unchanged
# by the time the first beta is reached.
#
# beta releases: at this stage, no new features are added and developers are tracking remaning bugs.
#
# release candidate releases: a release candidate is an ultimate release before the final release. Unless something
# bad happens, nothing is changed.


version = '0.4.2'

setup(name='cognite-sdk',
      version=version,
      description='Cognite API SDK for Python',
      url='http://cognite-sdk-python.readthedocs.io/',
      download_url='https://github.com/cognitedata/cognite-sdk-python/cognite/archive/{}.tar.gz'.format(version),
      author='Erlend Vollset',
      author_email='erlend.vollset@cognite.com',
      packages=['cognite'],
      install_requires=[
          'requests',
          'pandas'
      ],
      zip_safe=False,
      include_package_data=True)