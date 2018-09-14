<a href="https://cognite.com/">
    <img src="https://github.com/cognitedata/cognite-sdk-python/blob/master/cognite_logo.png" alt="Cognite logo" title="Cognite" align="right" height="80" />
</a>

Cognite Python SDK
==========================
[![build](https://webhooks.dev.cognite.ai/build/buildStatus/icon?job=github-builds/cognite-sdk-python/master)](https://jenkins.cognite.ai/job/github-builds/job/cognite-sdk-python/job/master/)
[![codecov](https://codecov.io/gh/cognitedata/cognite-sdk-python/branch/master/graph/badge.svg)](https://codecov.io/gh/cognitedata/cognite-sdk-python)
[![Documentation Status](https://readthedocs.com/projects/cognite-sdk-python/badge/?version=latest)](https://cognite-sdk-python.readthedocs-hosted.com/en/latest/?badge=latest)

Python SDK to ensure excellent user experience for developers and data scientists working with the Cognite Data Platform.

## Prerequisites
In order to start using the Python SDK, you need
- Python3 (>= 3.3) and pip
- An API key. Never include the API key directly in the code or upload the key to github. Instead, set the API key as an environment variable. See the usage example for how to authenticate with the API key.

This is how you set the API key as an environment variable on Mac OS and Linux:
```bash
$ export COGNITE_API_KEY=<your API key>
```

On Windows, you can follows [these instructions](https://www.computerhope.com/issues/ch000549.htm) to set the API key as an environment variable.

## Installation
```bash
$ pip install cognite-sdk
```

## Learn more

See the [examples](examples) folder for samples using the SDK.

Check out the documentation below, including the public API guide.

If you work at Cognite, you can find examples of code use in the repository ML-examples.


## Contributing
Want to contribute? Check out [CONTRIBUTING](https://github.com/cognitedata/cognite-sdk-python/blob/master/CONTRIBUTING.md).

## Documentation
* [SDK Documentation](https://cognite-sdk-python.readthedocs-hosted.com/en/latest/)
* [API Documentation](https://doc.cognitedata.com/)
* [API Guide](https://doc.cognitedata.com/guides/api-guide.html)


## License
Apache 2.0
