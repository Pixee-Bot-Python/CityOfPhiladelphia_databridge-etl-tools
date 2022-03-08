FROM python:3.6.15-slim-bullseye

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8
ENV LC_ALL  en_US.UTF-8

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

# pip stuff
RUN pip3 install --upgrade pip \
    && pip3 install setuptools-rust \
    && pip3 install -U setuptools \
    && pip3 install -e git+https://github.com/CityOfPhiladelphia/geopetl.git@b80a38cf1dae2cec9ce2c619281cc513795bf608#egg=geopetl \
                   Cython==0.29.27 \
                   awscli==1.22.46 \
                   boto3==1.20.46 \
                   carto==1.11.3 \
                   click==8.0.3 \
                   cryptography \
                   cx-Oracle \
                   petl==1.7.4 \
                   psycopg2==2.9.3 \
                   pyasn1==0.4.8 \
                   pyodbc==4.0.32 \
                   pytz==2021.3



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

COPY scripts/entrypoint.sh /entrypoint.sh
COPY tests/ tests/
RUN chmod +x /entrypoint.sh

# Cache bust
ENV updated-adds-on 5-1-2019_5
COPY databridge_etl_tools /databridge_etl_tools
# Python syntax check
RUN python -m compileall /databridge_etl_tools
COPY setup.py /setup.py
RUN pip3 install -e .[ago,postgres,oracle,carto,dev]


USER worker
ENTRYPOINT ["/entrypoint.sh"]
#CMD ["/bin/bash"] 

