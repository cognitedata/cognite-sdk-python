from setuptools import setup

setup(name='cognite-sdk-sdk',
      version='0.3',
      description='Cognite Python SDK',
      url='https://github.com/cognitedata/cognite-sdk-python',
      download_url='https://github.com/cognitedata/cognite-sdk-python/cognite-sdk/archive/0.3.tar.gz',
      author='Erlend Vollset',
      author_email='erlend.vollset@cognite.com',
      packages=['cognite-sdk'],
      install_requires=[
          'requests',
          'pandas'
      ],
      zip_safe=False,
      include_package_data=True)