from setuptools import setup

version = '0.3.9'

setup(name='cognite-sdk',
      version=version,
      description='Cognite API SDK for Python',
      url='http://cognite-sdk-python.readthedocs.io/',
      download_url='https://github.com/cognitedata/cognite-sdk-python/cognite/archive/{}.tar.gz'.format(version),
      author='Erlend Vollset',
      author_email='erlend.vollset@cognite.com',
      packages=['cognite'],
      install_requires=[
          'requests',
          'pandas'
      ],
      zip_safe=False,
      include_package_data=True)