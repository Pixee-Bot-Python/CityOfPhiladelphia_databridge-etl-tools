#!/usr/bin/env bash

set -e

mkdir dist

echo "Zip up site-packages"
cd /home/travis/virtualenv/python3.5.6/lib/python3.5/site-packages/
zip -r $TRAVIS_BUILD_DIR/dist/databridge-etl-tools.zip .

echo "Zip together previous zip file and lambda function"
cd $TRAVIS_BUILD_DIR/lambda
zip -g $TRAVIS_BUILD_DIR/dist/databridge-etl-tools.zip function.py