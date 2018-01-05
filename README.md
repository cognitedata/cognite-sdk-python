Python SDK for Cognite API
==========================
Python Package to ensure excellent CDP user experience for data scientists.

## Development instructions
### Setup
```bash
$ git clone https://github.com/cognitedata/cognite-sdk-python.git
$ pip install
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

### Deployment to Pypi
1. Update version number in setup.py
2. Create new tag on github
```bash
$ git tag <version> -m <message>
$ git push --tags origin master
```
3. Check unit tests and lint code
4. build
```bash
$ python3 setup.py sdist
$ python3 setup.py bdist_wheel
```
5. Upload using twine

`$ twine upload dist/*`

