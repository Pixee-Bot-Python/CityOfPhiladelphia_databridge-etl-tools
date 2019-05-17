#!/usr/bin/env bash

set -e

cd $TRAVIS_BUILD_DIR

mkdir dist

echo "Zip up site-packages, excluding non-compiled psycopg2 files to avoid confusing python imports"
cd /home/travis/virtualenv/python3.5.6/lib/python3.5/site-packages/
zip -r $TRAVIS_BUILD_DIR/dist/databridge-etl-tools.zip . -x psycopg2/**\* psycopg2_binary-2.8.2.dist-info/**\

echo "Zip together previous zip file and psycopg2 compiled files"
cd $TRAVIS_BUILD_DIR/
zip -ur $TRAVIS_BUILD_DIR/dist/databridge-etl-tools.zip psycopg2-3.6

echo "Zip together previous zip file and lambda function"
cd $TRAVIS_BUILD_DIR/lambda
zip -g $TRAVIS_BUILD_DIR/dist/databridge-etl-tools.zip index.py