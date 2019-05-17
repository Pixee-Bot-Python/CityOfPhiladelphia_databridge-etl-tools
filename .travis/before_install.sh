#!/usr/bin/env bash

set -e

pip install awscli

echo "Download compiled psycopg2 files"
svn export https://github.com/jkehler/awslambda-psycopg2.git/trunk/psycopg2-3.6