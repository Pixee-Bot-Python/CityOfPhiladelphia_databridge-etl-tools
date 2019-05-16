#!/usr/bin/env bash

set -e

echo "Zip up site-packages"
zip -r function.zip /home/travis/virtualenv/python3.5.6/lib/python3.5/site-packages/

echo "Zip together previous zip file and lambda function"
zip -g function.zip lambda/function.py

echo "Publish lambda function"
aws lambda update-function-code --function-name databridge-etl-tools --zip-file fileb://function.zip