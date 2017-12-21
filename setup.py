from setuptools import setup

setup(name='cognite-sdk',
      version='0.3.4',
      description='Cognite API SDK for Python',
      url='https://github.com/cognitedata/cognite-sdk-python',
      download_url='https://github.com/cognitedata/cognite-sdk-python/cognite/archive/0.3.4.tar.gz',
      author='Erlend Vollset',
      author_email='erlend.vollset@cognite.com',
      packages=['cognite'],
      install_requires=[
          'requests',
          'pandas'
      ],
      zip_safe=False,
      include_package_data=True)