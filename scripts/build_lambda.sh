#!/usr/bin/env bash

set -e

virtualenv venv
source venv/bin/activate

# Install databridge-etl-tools
python setup.py install

deactivate

# Zip up databridge-etl-tools and dependencies
cd venv/lib/python3.7/site-packages/
zip -r9 ../../../../function.zip .

# Zip together previous zip file and lambda function
cd ../../../../
zip -g function.zip lambda/function.py

# Publish lambda function
aws lambda update-function-code --function-name databridge-etl-tools --zip-file fileb://function.zip