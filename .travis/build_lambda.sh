#!/usr/bin/env bash

set -e

mkdir dist

echo "Zip up site-packages"
zip -r dist/databridge-etl-tools.zip /home/travis/virtualenv/python3.5.6/lib/python3.5/site-packages/

echo "Zip together previous zip file and lambda function"
zip -g dist/databridge-etl-tools.zip lambda/function.py