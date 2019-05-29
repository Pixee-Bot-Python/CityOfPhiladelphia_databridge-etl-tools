#!/usr/bin/env bash

aws lambda update-function-code  \
    --function-name databridge-etl-tools-$ENVIRONMENT \
    --s3-bucket citygeo-airflow-databridge2 \
    --s3-key lambda/databridge-etl-tools.zip