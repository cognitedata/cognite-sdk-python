from setuptools import find_packages, setup

REQUIRED_PACKAGES = []

setup(
    name="my_model",
    version="0.1",
    packages=find_packages(),
    include_package_data=True,
    install_requires=REQUIRED_PACKAGES,
)
