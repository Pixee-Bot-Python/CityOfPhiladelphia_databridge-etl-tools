#!/usr/bin/env python

from distutils.core import setup

setup(
    name='databridge_etl_tools',
    version='0.2.0',
    packages=['databridge_etl_tools',],
    install_requires=[
        'boto3==1.21.15',
        'botocore==1.24.15',
        'certifi==2021.10.8',
        'chardet==4.0.0',
        'click==8.0.4',
        'docutils==0.15.2',
        'future==0.18.2',
        'idna==3.3',
        'jmespath==0.10.0',
        'mock==4.0.3',
        'petl==1.7.8',
        'pyrestcli==0.6.11',
        'python-dateutil==2.8.2',
        'requests==2.27.1',
        's3transfer==0.5.2',
        'six==1.16.0',
        'Shapely==1.8.1.post1',
        # TEMPORARY! using commmit id from victor's shapes_and_worklfows_2
        #'geopetl @ https://github.com/CityOfPhiladelphia/geopetl/tarball/07fc22b4c92d1fe4e3d47a954c1dd36cccaaeb6d'
        # Updating to a new one to handle gdb_geomattr_data fields
        'geopetl @ https://github.com/CityOfPhiladelphia/geopetl/tarball/65ead28d4480d7cedcbc92272c3b4789f1e22810'
    ],
    extras_require={
        'ago': [
                'arcgis==2.0.0',
                'Shapely==1.8.1.post1',
                'pyproj<=3.2.1',
                'numpy==1.21.5'
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
    entry_points={
        'console_scripts': [
            'databridge_etl_tools=databridge_etl_tools:main',
        ],
    },
)
