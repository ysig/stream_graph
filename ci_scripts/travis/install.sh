# Install file used by cibuildwheel
pip install --upgrade pip
pip install --upgrade setuptools
pip install -r requirements.txt
pip install networkx

# Install coverage if needed
if [[ "$COVERAGE" == "true" ]]; then
    pip install coverage
fi

# Print versions
python --version
python -c "import numpy; print('numpy %s' % numpy.__version__)"
python -c "import pandas; print('pandas %s' % pandas.__version__)"

