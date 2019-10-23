"""`setup.py` file of stream_graph"""
from __future__ import print_function
import sys
import importlib
import warnings
from platform import system
from setuptools import setup, find_packages, Extension

# Import/install setup dependencies
def install_and_import(package):
    try:
        importlib.import_module(package)
    except ImportError:
        from pip._internal import main as pip_main
        warnings.warn('package ' + package +
                      ' is required through installation: trying to install it with pip')
        try:
            pip_main(['install', package])
        except Exception:
            raise

    globals()[package] = importlib.import_module(package)


install_and_import('numpy')
from numpy import get_include
install_and_import('Cython')
from Cython.Build import build_ext

# Compile extensions

# Set optimization arguments for compilation
OS = system()
if OS == 'Windows':
    extra_compile_args = ["/std:c++14", "/O2", "/W3"]
elif OS in ['Linux', 'Darwin']:
   extra_compile_args = ["-std=c++11", "-O3", "-w"]

# Remove the "-Wstrict-prototypes" compiler option, which isn't valid for C++.
import distutils.sysconfig
cfg_vars = distutils.sysconfig.get_config_vars()
for key, value in cfg_vars.items():
    if type(value) == str:
        cfg_vars[key] = value.replace("-Wstrict-prototypes", "")

# Package requirements
with open('requirements.txt') as f:
    INSTALL_REQUIRES = [l.strip() for l in f.readlines() if l]

# Add the _c_functions extension on kernels
ext_address = "./stream_graph/_c_functions/"
ext = Extension(name="stream_graph._c_functions",
                sources=[ext_address + "functions.pyx",
                         ext_address + "src/closeness.cpp"],
                include_dirs=[ext_address + "include", get_include()],
                depends=[ext_address + "include/functions.hpp"],
                language="c++",
                extra_compile_args=extra_compile_args)

# Add readme pypi
with open("README.md", "r") as fh:
    long_description = fh.read()
    long_description = '\n'.join(s for i, s in enumerate(
            [s for s in long_description.split('\n')
             if not (len(s) >= 2 and s[:2] == "[!")]) if i != 2)

setup(name='stream_graph',
      version='0.2.0',
      description='A library for Stream Graphs',
      author='Yiannis Siglidis [LIP6]',
      author_email='Yiannis.Siglidis@lip6.fr',
      packages=find_packages(),
      install_requires=INSTALL_REQUIRES,
      long_description=long_description,
      long_description_content_type='text/markdown',
      project_urls={
        'Documentation': 'https://ysig.github.io/stream_graph/doc',
        'Lab Website': 'http://www.complexnetworks.fr/',
        'Source': 'https://github.com/ysig/stream_graph',
        'Tracker': 'https://github.com/ysig/stream_graph/issues',
        },
      url='https://ysig.github.io/GraKeL/dev/',
      license="GPLv3+",
      classifiers=['Intended Audience :: Science/Research',
                   'Intended Audience :: Developers',
                   'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
                   'Programming Language :: C',
                   'Programming Language :: Python',
                   'Topic :: Software Development',
                   'Topic :: Sociology',
                   'Topic :: Scientific/Engineering',
                   'Operating System :: POSIX :: Linux',
                   'Operating System :: MacOS',
                   'Operating System :: Microsoft :: Windows',
                   'Programming Language :: Python :: 2.7',
                   'Programming Language :: Python :: 3.5',
                   'Programming Language :: Python :: 3.6',
                   'Programming Language :: Python :: 3.7',
                   ],
      python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*, <4',
      ext_modules=[ext],
      extras_require={
        'visualize': ["bokeh"]
      },
      cmdclass={'build_ext': build_ext},
      )
