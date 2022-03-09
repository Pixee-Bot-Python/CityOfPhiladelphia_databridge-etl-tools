#!/usr/bin/env python

from distutils.core import setup

setup(
    name='databridge_etl_tools',
    version='0.1.0',
    packages=['databridge_etl_tools',],
    install_requires=[
        'boto3',
        'botocore',
        'certifi',
        'chardet',
        'click',
        'docutils',
        'future',
        'idna',
        'jmespath',
        'petl',
        'pyrestcli',
        'python-dateutil',
        'requests',
        's3transfer',
        'six',
        'urllib3'
    ],
    extras_require={
        'ago': [
                'arcgis==2.0.0',
                'Shapely==1.8.1.post1',
                'pyproj==3.2.1'
                ],
        'carto': ['carto==1.11.3'],
        'oracle': ['cx_Oracle==8.3.0'],
        'postgres': ['psycopg2-binary==2.9.3',
                    'psycopg2==2.9.3'],
        'dev': [
            'moto==3.0.7',
            'pytest==7.0.1',
            'requests-mock==1.9.3'
        ]
    },
    dependency_links=[
        'https://github.com/CityOfPhiladelphia/geopetl/tarball/b80a38cf1dae2cec9ce2c619281cc513795bf608'
    ],
    entry_points={
        'console_scripts': [
            'databridge_etl_tools=databridge_etl_tools:main',
        ],
    },
)
