#FROM ubuntu:16.04
#FROM python:3.6.15-slim-bullseye
FROM python:3.7.12-slim-buster

# Add our worker users custom binaries to the path, some python packages are installed here.
ENV PATH="/home/worker/.local/bin:${PATH}"
#ENV PYTHONPATH="/home/worker/.local/lib/python3.7/site-packages:${PYTHONPATH}"

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Solve annoying locale problems in docker
# C.UTF-8 should have better availablility then the default
# we like to use, "en_US.UTF-8"
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8

# Oracle
ENV ORACLE_HOME=/usr/lib/oracle/12.1/client64
ENV LD_LIBRARY_PATH=$ORACLE_HOME/lib
ENV PATH=$ORACLE_HOME/bin:$PATH
ENV HOSTALIASES=/tmp/HOSTALIASES

RUN set -ex \
    && buildDeps=' \
        python3-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        build-essential \
        libblas-dev \
        liblapack-dev \
    ' \
    && apt-get update -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        libpq-dev \
        python3 \
        python3-pip \
        netbase \
        apt-utils \
        unzip \
        curl \
        netcat \
        locales \
        git \
        alien \
        libgdal-dev \
        libgeos-dev \
        binutils \
        libproj-dev \
        gdal-bin \
        libspatialindex-dev \
        libaio1 \
        freetds-dev

# Locale stuff
RUN sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash worker

# Cleanup
RUN apt-get remove --purge -yqq $buildDeps \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

# instant basic-lite instant oracle client
COPY oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm ./
RUN alien -i oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm \
    && rm oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm

# instant oracle-sdk
RUN alien -i oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm \
    && rm oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm


USER worker
WORKDIR /home/worker/

# pip stuff
RUN pip3 install --upgrade pip \
    && pip3 install setuptools-rust \
    && pip3 install -U setuptools \
    && pip3 install Cython==0.29.28 \
                   awscli==1.22.70 \
                   boto3==1.21.15 \
                   click==8.0.4 \
                   cryptography==36.0.1 \
                   petl==1.7.8 \
                   pyasn1==0.4.8 \
                   pyodbc==4.0.32 \
                   pytz==2021.3 \
                   wheel

# Per WORKDIR above, these should be placed in /home/worker/
COPY --chown=worker:root scripts/entrypoint.sh ./entrypoint.sh
COPY --chown=worker:root tests/ ./tests/
COPY --chown=worker:root setup.py ./setup.py
COPY --chown=worker:root databridge_etl_tools ./databridge_etl_tools

RUN chmod +x ./entrypoint.sh

# Python syntax check
RUN python -m compileall ./databridge_etl_tools

# Install databridge-etl-tools using setup.py
RUN pip3 install -e .[ago,carto,oracle,postgres,dev]

# For some reason our latest commit wasn't being installed in setup.py, so install it here instead for now.
#RUN pip3 install -e git+https://github.com/CityOfPhiladelphia/geopetl.git@f4d3cd5571908fe6c51096f67c002b26a7f732c3#egg=geopetl
# roland-3-31-22 branch to fix geom_column issue
#RUN pip3 install -e git+https://github.com/CityOfPhiladelphia/geopetl.git@389f7d78c734197df0f3130e87e6b9091c34d805#egg=geopetl

# Quick hack to fix CSV dump issue from Oracle
RUN   sed -i "s|MAX_NUM_POINTS_IN_GEOM_FOR_CHAR_CONVERSION_IN_DB = 150|MAX_NUM_POINTS_IN_GEOM_FOR_CHAR_CONVERSION_IN_DB = 100|g" /home/worker/.local/lib/python3.7/site-packages/geopetl/oracle_sde.py

# Set aws access keys as an env var for use with boto3
# do this under the worker user.
# These are passed in via --build-arg at build time.
ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY

ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
ENV AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY


ENTRYPOINT ["/home/worker/entrypoint.sh"]
