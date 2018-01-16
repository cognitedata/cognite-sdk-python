Python SDK for Cognite API
==========================
Python Package to ensure excellent CDP user experience for data scientists.

## Development instructions
### Setup
```bash
$ git clone https://github.com/cognitedata/cognite-sdk-python.git
$ cd cognite-sdk-python
$ pipenv install
$ pipenv shell
```

### Linting
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
These will be automatically built and released when docs are pushed to master.

### Deployment to Pypi
1. Lint code
2. Create new release on github following the release numbering conventions shown below
2. Update version number in setup.py
3. Merge pull request into master

### Release numbering conventions
Format: MAJOR.MINOR[.MICRO][PRE-RELEASE IDENTIFIER]

Example: 0.4.1a1

#### Major
Major revision number for the software like 2 or 3 for Python
#### Minor
Groups moderate changes to the software like bug fixes or minor improvements
#### Micro
Releases dedicated to bug fixes

#### Pre-Releases
Valid pre-release identifiers are: a (alpha), b (beta), rc (release candidate)

##### alpha
Early pre-releases. A lot of changes can occur between alphas and the final release, like feature additions or refactorings. But they are minor changes and the software should stay pretty unchanged by the time the first beta is reached.

##### beta
At this stage, no new features are added and developers are tracking remaning bugs.

##### release candidate
A release candidate is an ultimate release before the final release. Unless something bad happens, nothing is changed.
