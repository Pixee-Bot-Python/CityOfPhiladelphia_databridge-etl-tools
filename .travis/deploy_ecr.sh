#!/usr/bin/env bash

set -e

# instant basic-lite instant oracle client
aws s3api get-object \
    --bucket citygeo-oracle-instant-client \
    --key oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm \
        oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm

# instant oracle-sdk
aws s3api get-object \
    --bucket citygeo-oracle-instant-client \
    --key oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm \
        oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm

REPO_NAME=airflow-databridge-etl-tools-worker-$ENVIRONMENT

eval $(aws ecr get-login --no-include-email --region us-east-1)
docker build -t $REPO_NAME .
docker tag $REPO_NAME:latest $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/$REPO_NAME:latest
docker push $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/$REPO_NAME:latest