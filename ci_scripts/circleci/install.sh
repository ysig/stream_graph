#!/bin/bash
# This script is meant to be called in the "deploy" step defined in 
# circle.yml. See https://circleci.com/docs/ for more details.
# The behavior of the script is controlled by environment variable defined
# in the circle.yml in the top level folder of the project.

# System dependencies
sudo -E apt-get -yq remove texlive-binaries --purge > /dev/null
sudo apt-get update > /dev/null
sudo apt-get install libatlas-dev > /dev/null
sudo apt-get install build-essential > /dev/null

# Setup a python venv and install basics
source ./venv/bin/activate
pip install --upgrade pip
pip install --upgrade pandas matplotlib setuptools sphinx sphinx-gallery sphinx_rtd_theme sphinxcontrib-bibtex nb2plots numpydoc pillow cairosvg > /dev/null
pip install ccfi
pip install -r requirements.txt > /dev/null

# More dependencies
sudo -E apt-get -yq update > /dev/null
sudo -E apt-get -yq --no-install-suggests --no-install-recommends --force-yes install dvipng texlive-latex-base texlive-latex-extra > /dev/null

# Install project
python setup.py clean
python setup.py develop


# Build Docs
set -o pipefail && cd doc && make html 2>&1 | tee ~/log.txt && cd ..
cat ~/log.txt && if grep -q "Traceback (most recent call last):" ~/log.txt; then false; else true; fi

