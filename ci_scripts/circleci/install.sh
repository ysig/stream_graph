#!/bin/bash
# This script is meant to be called in the "deploy" step defined in 
# circle.yml. See https://circleci.com/docs/ for more details.
# The behavior of the script is controlled by environment variable defined
# in the circle.yml in the top level folder of the project.

# System dependencies
sudo -E apt-get -yq remove texlive-binaries --purge > /dev/null
sudo apt-get update > /dev/null
sudo apt-get install build-essential libffi-dev > /dev/null

# Setup a python venv and install basics
source ./venv/bin/activate
pip install --upgrade pip
pip install --upgrade bokeh pandas matplotlib setuptools sphinx sphinx-gallery sphinx_rtd_theme sphinxcontrib-bibtex nb2plots numpydoc pillow > /dev/null
pip install cffi
pip install -r requirements.txt > /dev/null

# More dependencies
sudo -E apt-get -yq update > /dev/null
sudo -E apt-get -yq --no-install-suggests --no-install-recommends --force-yes install dvipng texlive-latex-base texlive-latex-extra > /dev/null


# Install phantom js
sudo apt-get install chrpath libssl-dev libxft-dev -y
sudo apt-get install libfreetype6 libfreetype6-dev -y
sudo apt-get install libfontconfig1 libfontconfig1-dev -y
cd ~
export PHANTOM_JS="phantomjs-2.1.1-linux-x86_64"
wget https://github.com/Medium/phantomjs/releases/download/v2.1.1/$PHANTOM_JS.tar.bz2
sudo tar xvjf $PHANTOM_JS.tar.bz2
sudo mv $PHANTOM_JS /usr/local/share
sudo ln -sf /usr/local/share/$PHANTOM_JS/bin/phantomjs /usr/local/bin
phantomjs --version
pwd

# Install project
python setup.py clean
python setup.py develop


# Build Docs
set -o pipefail && cd doc && make html 2>&1 | tee ~/log.txt && cd ..
cat ~/log.txt && if grep -q "Traceback (most recent call last):" ~/log.txt; then false; else true; fi

