import re

from setuptools import find_packages, setup

version = re.search('^__version__\s*=\s*"(.*)"', open("cognite/client/_version.py").read(), re.M).group(1)

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="cognite-sdk",
    version=version,
    description="Client library for Cognite Data Fusion (CDF)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://cognite-sdk-python.readthedocs-hosted.com",
    download_url="https://github.com/cognitedata/cognite-sdk-python/archive/{}.tar.gz".format(version),
    author="Erlend Vollset",
    author_email="erlend.vollset@cognite.com",
    install_requires=["requests>=2.21.0,<3.0.0", "pandas", "requests_oauthlib==1.3.0"],
    python_requires=">=3.5",
    packages=["cognite." + p for p in find_packages(where="cognite")],
    include_package_data=True,
)
