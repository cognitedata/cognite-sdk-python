from setuptools import setup
import cognite

setup(name='cognite-sdk',
      version=cognite.__version__,
      description='Cognite API SDK for Python',
      url='http://cognite-sdk-python.readthedocs.io/',
      download_url='https://github.com/cognitedata/cognite-sdk-python/archive/{}.tar.gz'.format(cognite.__version__),
      author='Erlend Vollset',
      author_email='erlend.vollset@cognite.com',
      packages=['cognite'],
      install_requires=[
          'requests',
          'pandas'
      ],
      python_requires='>=3',
      zip_safe=False,
      include_package_data=True)