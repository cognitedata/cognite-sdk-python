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
    install_requires=[
        "requests>=2.21.0,<3.0.0",
        "pandas",
        "requests_oauthlib==1.*",
        "gdal==3.4.2;platform_system=='Windows'",
        "fiona==1.8.21;platform_system=='Windows'",
        "geopandas==0.10.*",
        "shapely==1.*",
    ],
    dependency_links=[
        "https://download.lfd.uci.edu/pythonlibs/x6hvwk7i/GDAL-3.4.2-cp38-cp38-win_amd64.whl",
        "https://download.lfd.uci.edu/pythonlibs/x6hvwk7i/Fiona-1.8.21-cp38-cp38-win_amd64.whl"
    ],
    python_requires=">=3.5",
    packages=["cognite." + p for p in find_packages(where="cognite")],
    include_package_data=True,
)
