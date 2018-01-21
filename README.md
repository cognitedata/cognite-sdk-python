Python SDK for Cognite API
==========================
Python Package to ensure excellent CDP user experience for data scientists.
To view the documentation for this package click [here](http://cognite-sdk-python.readthedocs.io/ "SDK Documentation").
## Development instructions
### Setup
```bash
$ git clone https://github.com/cognitedata/cognite-sdk-python.git
$ cd cognite-sdk-python
$ pipenv install
$ pipenv shell
```

Any changes, bug fixes, additions, or improvements you wish to make should be done on a development branch. A pull request should be created to have your code reviewed.
### Deployment to Pypi
1. Update version/release number in cognite/__init__.py following the release conventions shown below.
2. Create a pull request, have it reviewed and merged. (This will trigger a Jenkins build and automatic release to PyPi).
3. Create new tag on github to match the updated version number.

### Linting
We follow the PEP8 standard and use pylint to enforce it.
#### Command line
`$ pylint <module>`
#### Editor and IDE Integration
Alternativley integrate pylint in your editor or IDE by following the instructions [here](https://docs.pylint.org/en/1.6.0/ide-integration.html)

### Unit testing
Set up unit tests for all new functionality
Run unit tests by running the following command from the root directory:

`$ python3 unit_tests/run_tests.py`
### Documentation
Build html files of documentation locally by running
```bash
$ cd docs 
$ make html
```
Documentation will be automatically generated from the google-style docstrings in the source code. It is then built and released when changes are merged into master.

### Release version conventions
Format: 
``` 
MAJOR.MINOR[.MICRO]
```

The major and minor version numbers should mirror the Cognite API. Micro releases are dedicated to bug fixes, improvements, and additions.
