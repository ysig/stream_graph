"""`setup.py` file of stream_graph"""
from setuptools import setup, find_packages

# Package requierements
with open('requirements.txt') as f:
    INSTALL_REQUIRES = [l.strip() for l in f.readlines() if l]


setup(name='stream_graph',
      version='0.0',
      description='A library for Stream Graphs',
      author='Ioannis Siglidis [LIP6]',
      author_email='Yiannis.Siglidis@lip6.fr',
      packages=find_packages(),
      install_requires=INSTALL_REQUIRES)
