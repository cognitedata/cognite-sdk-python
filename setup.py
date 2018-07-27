import cognite
from setuptools import find_packages, setup

packages = find_packages(exclude=["tests*"])

setup(
    name="cognite-sdk",
    version=cognite.__version__,
    description="Cognite API SDK for Python",
    url="http://cognite-sdk-python.readthedocs.io/",
    download_url="https://github.com/cognitedata/cognite-sdk-python/archive/{}.tar.gz".format(cognite.__version__),
    author="Erlend Vollset",
    author_email="erlend.vollset@cognite.com",
    packages=packages,
    install_requires=["requests", "pandas", "protobuf", "cognite-logger"],
    zip_safe=False,
    include_package_data=True,
)
