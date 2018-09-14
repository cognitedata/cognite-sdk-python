from setuptools import setup, find_packages

setup(name='model',
      version='0.1',
      packages=find_packages(),
      description='SVM sklearn GMLE CMHE',
      package_data={'model': ['model.*', 'processing_requirements.txt']})
