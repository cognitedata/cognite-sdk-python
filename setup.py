import re

from setuptools import find_packages, setup

packages = find_packages(exclude=["tests*"])

version = re.search('^__version__\s*=\s*"(.*)"', open("cognite/client/__init__.py").read(), re.M).group(1)

setup(
    name="cognite-sdk",
    version=version,
    description="Cognite API SDK for Python",
    url="http://cognite-sdk-python.readthedocs.io/",
    download_url="https://github.com/cognitedata/cognite-sdk-python/archive/{}.tar.gz".format(version),
    author="Erlend Vollset",
    author_email="erlend.vollset@cognite.com",
    packages=packages,
    install_requires=["requests", "pandas", "protobuf", "cognite-logger>=0.3"],
    python_requires=">=3.5",
    zip_safe=False,
    include_package_data=True,
)
