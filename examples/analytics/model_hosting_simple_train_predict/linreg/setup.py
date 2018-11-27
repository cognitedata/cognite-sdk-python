from setuptools import find_packages, setup

# It's important to specify all dependencies on external packages,
# so that these can be installed.
# There's no need to specify cognite-sdk (it's installed by default)
REQUIRED_PACKAGES = ["pandas>=0.23", "numpy>=1.15"]

setup(
    name="linreg",
    version="0.1",
    install_requires=REQUIRED_PACKAGES,
    packages=find_packages(),
    description="A simple linear regression model for a tutorial",
    url="https://relevant.webpage",
    maintainer="Tutorial",
    maintainer_email="Tutorial",
)
