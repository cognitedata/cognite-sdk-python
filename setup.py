from setuptools import setup

setup(name='cognite',
      version='0.2',
      description='Cognite Python SDK',
      url='https://github.com/cognitedata/cognite-sdk-python',
      download_url='https://github.com/cognitedata/cognite-sdk-python/cognite/archive/0.1.tar.gz',
      author='Erlend Vollset',
      author_email='erlend.vollset@cognite.com',
      packages=['cognite'],
      install_requires=[
          'requests',
          'pandas'
      ],
      zip_safe=False,
      include_package_data=True)