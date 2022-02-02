#!/usr/bin/env python

from distutils.core import setup

setup(
    name='databridge_etl_tools',
    version='0.2.0',
    packages=['databridge_etl_tools',],
    install_requires=[
        'boto3==1.20.46',
        'botocore==1.23.46',
        'certifi==2021.10.8',
        'chardet==4.0.0',
        'click==8.0.3',
        'docutils==0.15.2',
        'future==0.18.2',
        'idna==3.3',
        'jmespath==0.10.0',
        'petl==1.7.4',
        'pyrestcli==0.6.11',
        'python-dateutil==2.8.2',
        'requests==2.27.1',
        'six==1.16.0',
        'urllib3==1.26.8'
    ],
    extras_require={
        'carto': ['carto==1.11.3'],
        'oracle': ['cx_Oracle==8.3.0'],
        'postgres': ['psycopg2-binary==2.9.3'],
        'dev': [
            'moto==3.0.1',
            'pytest',
            'requests-mock',
            'mock'
        ]
    },
    # TEMPORARY! using commmit id from victor's shapes_and_worklfows_2
    dependency_links=[
        'https://github.com/CityOfPhiladelphia/geopetl/tarball/b80a38cf1dae2cec9ce2c619281cc513795bf608'
    ],
    entry_points={
        'console_scripts': [
            'databridge_etl_tools=databridge_etl_tools:main',
        ],
    },
)
