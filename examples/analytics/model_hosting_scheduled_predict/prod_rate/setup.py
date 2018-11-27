from setuptools import find_packages, setup

# It's important to specify all dependencies on external packages,
# so that these can be installed.
# There's no need to specify cognite-sdk (it's installed by default)
REQUIRED_PACKAGES = ["scikit-learn==0.19.1", "numpy>=1.8.2", "scipy>=0.13.3"]

setup(
    name="prod-rate",
    version="0.1",
    install_requires=REQUIRED_PACKAGES,
    packages=find_packages(),
    description="A random forrest regressor used to find production rate for abc equipment",
    url="https://relevant.webpage",
    maintainer="Tutorial",
    maintainer_email="Tutorial",
)
