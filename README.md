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

Any changes, bug fixes, additions, or improvements you wish to make should be done on a development branch. A pull request should be created to have your code reviewed.
### Deployment to Pypi
1. Lint code by following the instructions below.
2. Create new release on github following the release conventions shown below.
2. Update version/release number in setup.py.
3. Merge pull request into master.
4. Jenkins takes care of the rest :)

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

### Release conventions
Format: 
``` 
MAJOR.MINOR[.MICRO][PRE-RELEASE IDENTIFIER]
```

Valid pre-release identifiers are: a (alpha), b (beta), rc (release candidate)

Example: 0.4.1a1

#### Release numbering
| **Release**   | **Description** |
| ------------- |-----------------|
| **Major**     | Major revision number for the software like 2 or 3 for Python |
| **Minor**     | Groups moderate changes to the software like bug fixes or minor improvements |
| **Micro**     | Releases dedicated to bug fixes |

#### Pre-Releases
| **Release**   | **Description** |
| ------------- |-----------------|
| **alpha (a)** | Early pre-releases. A lot of changes can occur between alphas and the final release, like feature additions or refactorings. But they are minor changes and the software should stay pretty unchanged by the time the first beta is reached. |
| **beta (b)**  | At this stage, no new features are added and developers are tracking remaning bugs. |
| **release candidate (rc)** | A release candidate is an ultimate release before the final release. Unless something bad happens, nothing is changed. |

