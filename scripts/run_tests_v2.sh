#!/bin/bash
set -e
python3 -m venv /venv-testing
source /venv-testing/bin/activate
# This allows us to install moto for AWS testing
pip install --upgrade pip
pip install setuptools-rust
#python setup.py install
pip install -e .[carto,postgres,oracle,dev]
# Run our tests
pytest /tests
