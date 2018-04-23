from setuptools import setup, find_packages

import cognite

packages = find_packages()
packages.remove('tests')

setup(name='cognite-sdk',
      version=cognite.__version__,
      description='Cognite API SDK for Python',
      url='http://cognite-sdk-python.readthedocs.io/',
      download_url='https://github.com/cognitedata/cognite-sdk-python/archive/{}.tar.gz'.format(cognite.__version__),
      author='Erlend Vollset',
      author_email='erlend.vollset@cognite.com',
      packages=packages,
      install_requires=[
          'requests',
          'pandas',
          'protobuf'
      ],
      zip_safe=False,
      include_package_data=True)
