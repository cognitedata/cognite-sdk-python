from setuptools import find_packages, setup

REQUIRED_PACKAGES = ["pandas>=0.23", "numpy>=1.15"]

setup(
    name="linreg",
    version="0.1",
    install_requires=REQUIRED_PACKAGES,
    packages=find_packages(),
    description="A simple linear regression model for a tutorial",
    url="https://relevant.webpage",
    maintainer="Tutorial",
    maintainer_email="Tutorial"
)