#!/usr/bin/env bash

set -e

pip install awscli

# cd $TRAVIS_BUILD_DIR

# mkdir lib

# echo "Download oracle instant client from S3 and unzip it"
# aws s3api get-object \
#     --bucket citygeo-oracle-instant-client \
#     --key instantclient-basic-linux.x64-12.2.0.1.0.zip \
#         /tmp/instantclient-basic-linux.x64-12.2.0.1.0.zip 
# unzip /tmp/instantclient-basic-linux.x64-12.2.0.1.0.zip -d ./lib/
# rm /tmp/instantclient-basic-linux.x64-12.2.0.1.0.zip
 
# echo "Download oracle instant sdk from S3 and unzip it"
# aws s3api get-object \
#     --bucket citygeo-oracle-instant-client \
#     --key instantclient-sdk-linux.x64-12.2.0.1.0.zip \
#         /tmp/instantclient-sdk-linux.x64-12.2.0.1.0.zip
# unzip /tmp/instantclient-sdk-linux.x64-12.2.0.1.0.zip -d ./lib/
# rm /tmp/instantclient-sdk-linux.x64-12.2.0.1.0.zip

# echo "Copy libaio.so into lib"
# cp /lib/x86_64-linux-gnu/libaio.so.1 ./lib/